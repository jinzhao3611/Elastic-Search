"""
This module implements a (partial, sample) query interface for elasticsearch movie search. 
You will need to rewrite and expand sections to support the types of queries over the fields in your UI.

Documentation for elasticsearch query DSL:
https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl.html

For python version of DSL:
https://elasticsearch-dsl.readthedocs.io/en/latest/

Search DSL:
https://elasticsearch-dsl.readthedocs.io/en/latest/search_dsl.html
"""

from flask import *
from index import Movie
from elasticsearch_dsl import Q
from elasticsearch_dsl.utils import AttrList
from elasticsearch_dsl import Search

app = Flask(__name__)

# Initialize global variables for rendering page
tmp_text = ""
tmp_title = ""
tmp_star = ""
tmp_direct = ""
tmp_lang = ""
tmp_country = ""
tmp_locat = ""
tmp_time = ""
tmp_cat = ""
tmp_min = ""
tmp_max = ""
gresults = {}


# display query page
@app.route("/")
def search():
    return render_template('page_query.html')


# display results page for first set of results and "next" sets.
@app.route("/results", defaults={'page': 1}, methods=['GET', 'POST'])
@app.route("/results/<page>", methods=['GET', 'POST'])
def results(page):
    global tmp_text
    global tmp_title
    global tmp_star
    global tmp_direct
    global tmp_lang
    global tmp_country
    global tmp_locat
    global tmp_time
    global tmp_cat
    global tmp_min
    global tmp_max
    global gresults

    # convert the <page> parameter in url to integer.
    if type(page) is not int:
        page = int(page.encode('utf-8'))
    # if the method of request is post (for initial query), store query in local global variables
    if request.method == 'POST':
        text_query = request.form['query']
        star_query = request.form['starring']
        mintime_query = request.form['mintime']
        direct_query = request.form['director']
        lang_query = request.form['language']
        country_query = request.form['country']
        locat_query = request.form['location']
        time_query = request.form['time']
        cat_query = request.form['categories']

        if len(mintime_query) is 0:
            mintime = 0
        else:
            mintime = int(mintime_query)
        maxtime_query = request.form['maxtime']
        if len(maxtime_query) is 0:
            maxtime = 99999
        else:
            maxtime = int(maxtime_query)

        # update global variable template data
        tmp_text = text_query
        tmp_star = star_query
        tmp_cat = cat_query
        tmp_country = country_query
        tmp_direct = direct_query
        tmp_lang = lang_query
        tmp_locat = locat_query
        tmp_time = time_query
        tmp_min = mintime
        tmp_max = maxtime
    else:
        # use the current values stored in global variables.
        text_query = tmp_text
        star_query = tmp_star
        cat_query = tmp_cat
        country_query = tmp_country
        direct_query = tmp_direct
        lang_query = tmp_lang
        time_query = tmp_time
        locat_query = tmp_locat
        mintime = tmp_min
        if tmp_min > 0:
            mintime_query = tmp_min
        else:
            mintime_query = ""
        maxtime = tmp_max
        if tmp_max < 99999:
            maxtime_query = tmp_max
        else:
            maxtime_query = ""

    # store query values to display in search boxes in UI
    shows = {}
    shows['text'] = text_query
    shows['star'] = star_query
    shows['cat'] = cat_query
    shows['country'] = country_query
    shows['direct'] = direct_query
    shows['lang'] = lang_query
    shows['time'] = time_query
    shows['maxtime'] = maxtime_query
    shows['mintime'] = mintime_query
    shows['locat'] = locat_query

    # Create a search object to query our index 
    search = Search(index='sample_film_index')

    # Build up your elasticsearch query in piecemeal fashion based on the user's parameters passed in.
    # The search API is "chainable".
    # Each call to search.query method adds criteria to our growing elasticsearch query.
    # You will change this section based on how you want to process the query data input into your interface.

    # search for runtime using a range query
    q = Q('range', runtime={'gte': mintime, 'lte': maxtime})
    s = search.query(q)

    # search for matching text query
    if len(text_query) > 0:
        q = Q('multi_match', query=text_query, type='cross_fields', fields=['title^2', 'text'], operator='and')
        s = s.query(q)

    # search for matching starring
    if len(star_query) > 0: 
        q = Q('fuzzy', starring={'value': star_query, 'transpositions': True})
        # people cannot remember clearly about the names of starrings
        s = s.query(q)

    # search for matching director
    if len(direct_query) > 0:
        q = Q('match', director={'query': direct_query, 'operator': 'and'})
        s = s.query(q)

    # search for matching language
    if len(lang_query) > 0:
        q = Q('match', language=lang_query)
        s = s.query(q)

    # search for matching country
    if len(country_query) > 0:
        q = Q('match', country=country_query)
        s = s.query(q)

    # search for matching location
    if len(locat_query) > 0:
        q = Q('match', location=locat_query)
        s = s.query(q)

    # search for matching categories
    if len(cat_query) > 0:
        q = Q('match', categories={"query": cat_query, "cutoff_frequency": 0.02})
        s = s.query(q)

    # search for matching time
    if len(time_query) > 0:
        # we want the query to be exactly the same with the term in the index
        q = Q('term', time=time_query)
        s = s.query(q)

    # highlight
    s = s.highlight_options(pre_tags='<mark>', post_tags='</mark>')
    s = s.highlight('text', fragment_size=999999999, number_of_fragments=1)
    s = s.highlight('title', fragment_size=999999999, number_of_fragments=1)
    s = s.highlight('starring', fragment_size=999999999, number_of_fragments=1)
    s = s.highlight('director', fragment_size=999999999, number_of_fragments=1)
    s = s.highlight('time', fragment_size=999999999, number_of_fragments=1)
    s = s.highlight('language', fragment_size=999999999, number_of_fragments=1)
    s = s.highlight('location', fragment_size=999999999, number_of_fragments=1)
    s = s.highlight('categories', fragment_size=999999999, number_of_fragments=1)
    s = s.highlight('country', fragment_size=999999999, number_of_fragments=1)

    # determine the subset of results to display (based on current <page> value)
    start = 0 + (page - 1) * 10
    end = 10 + (page - 1) * 10

    # execute search and return results in specified range.
    response = s[start:end].execute()

    if response.hits.total == 0:
        # if conjunction failed, make the query disjunctive for text field
        search = Search(index='sample_film_index')

        # search for runtime
        q = Q('range', runtime={'gte': mintime, 'lte': maxtime})
        s = search.query(q)

        # search for matching text query
        if len(text_query) > 0:
            q = Q('multi_match', query=text_query, type='cross_fields', fields=['title^2', 'text'], operator='or')
            s = s.query(q)

            # search for matching starring
        if len(star_query) > 0:
            q = Q('fuzzy', starring={'value': star_query, 'transpositions': True})
            # people cannot remember clearly about the names of starrings
            s = s.query(q)

        # search for matching director
        if len(direct_query) > 0:
            q = Q('match', director={'query': direct_query, 'operator': 'and'})
            s = s.query(q)

        # search for matching language
        if len(lang_query) > 0:
            q = Q('match', language=lang_query)
            s = s.query(q)

        # search for matching country
        if len(country_query) > 0:
            q = Q('match', country=country_query)
            s = s.query(q)

        # search for matching location
        if len(locat_query) > 0:
            q = Q('match', location=locat_query)
            s = s.query(q)

        # search for matching categories
        if len(cat_query) > 0:
            q = Q('match', categories={"query": cat_query, "cutoff_frequency": 0.02})
            s = s.query(q)

        # search for matching time
        if len(time_query) > 0:
            q = Q('term', time=time_query)
            s = s.query(q)

        # highlight
        s = s.highlight_options(pre_tags='<mark>', post_tags='</mark>')
        s = s.highlight('text', fragment_size=999999999, number_of_fragments=1)
        s = s.highlight('title', fragment_size=999999999, number_of_fragments=1)
        s = s.highlight('starring', fragment_size=999999999, number_of_fragments=1)
        s = s.highlight('director', fragment_size=999999999, number_of_fragments=1)
        s = s.highlight('time', fragment_size=999999999, number_of_fragments=1)
        s = s.highlight('language', fragment_size=999999999, number_of_fragments=1)
        s = s.highlight('location', fragment_size=999999999, number_of_fragments=1)
        s = s.highlight('categories', fragment_size=999999999, number_of_fragments=1)
        s = s.highlight('country', fragment_size=999999999, number_of_fragments=1)
        # warning = 'cannot find all the terms!'
        response = s[start:end].execute()

    # insert data into response
    resultList = {}
    for hit in response.hits:
        result = dict()
        result['score'] = hit.meta.score

        if 'highlight' in hit.meta:
            # highlight specific fields
            if 'title' in hit.meta.highlight:
                result['title'] = hit.meta.highlight.title[0]
            else:
                result['title'] = hit.title

            if 'text' in hit.meta.highlight:
                result['text'] = hit.meta.highlight.text[0]
            else:
                result['text'] = hit.text
            if 'starring' in hit.meta.highlight:
                result['starring'] = hit.meta.highlight.starring[0]
            else:
                result['starring'] = hit.starring
            if 'director' in hit.meta.highlight:
                result['director'] = hit.meta.highlight.director[0]
            else:
                result['director'] = hit.director
            if 'time' in hit.meta.highlight:
                result['time'] = hit.meta.highlight.time[0]
            else:
                result['time'] = hit.time
            if 'country' in hit.meta.highlight:
                result['country'] = hit.meta.highlight.country[0]
            else:
                result['country'] = hit.country
            if 'language' in hit.meta.highlight:
                result['language'] = hit.meta.highlight.language[0]
            else:
                result['language'] = hit.language
            if 'location' in hit.meta.highlight:
                result['location'] = hit.meta.highlight.location[0]
            else:
                result['location'] = hit.location
            if 'categories' in hit.meta.highlight:
                result['categories'] = hit.meta.highlight.categories[0]
            else:
                result['categories'] = hit.categories
        else:
            result['title'] = hit.title
            result['text'] = hit.text
            result['starring'] = hit.starring
            result['director'] = hit.director
            result['time'] = hit.time
            result['country'] = hit.country
            result['language'] = hit.language
            result['location'] = hit.location
            result['categories'] = hit.categories

        resultList[hit.meta.id] = result

    # make the result list available globally
    gresults = resultList

    # total number of matching results
    result_num = response.hits.total

    # if results are found, extract title and text information from doc_data, else do nothing
    if result_num > 0:
        return render_template('page_SERP.html', results=resultList, res_num=result_num, page_num=page, queries=shows)
    else:
        warning = None
        message = ['one of the field you typed in cannot be found.']
        return render_template('page_SERP.html', results=message, res_num=result_num, page_num=page, queries=shows,
                               warning=warning)


# display a particular document given a result number
@app.route("/documents/<res>", methods=['GET'])
def documents(res):
    global gresults
    film = gresults[res]
    filmtitle = film['title']
    for term in film:
        if type(film[term]) is AttrList:
            s = "\n"
            for item in film[term]:
                s += item + ",\n "
            film[term] = s
    # fetch the movie from the elasticsearch index using its id
    movie = Movie.get(id=res, index='sample_film_index')
    filmdic = movie.to_dict()
    film['runtime'] = str(filmdic['runtime']) + " min"
    return render_template('page_targetArticle.html', film=film, title=filmtitle)


if __name__ == "__main__":
    app.run()
