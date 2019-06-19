import json
import time

from elasticsearch import Elasticsearch
from elasticsearch import helpers
from elasticsearch_dsl import Index, Document, Text, Integer
from elasticsearch_dsl.connections import connections
from elasticsearch_dsl.analysis import analyzer
from search_helper import runtime_str2int, list2str

# Connect to local host server
connections.create_connection(hosts=['127.0.0.1'])

es = Elasticsearch()

my_analyzer = analyzer(name_or_instance='custom',
                           tokenizer='standard',
                           filter=['lowercase', 'stop', 'porter_stem'])

class Movie(Document):
    """
    here we define a class Movie that inherited from Document, basically this is where we set a datatype to
    each field in our json file.
    """
    title = Text(analyzer=my_analyzer)
    text = Text(analyzer=my_analyzer)
    starring = Text(analyzer='standard')
    director = Text(analyzer='standard')
    language = Text(analyzer='standard')
    country = Text(analyzer="standard")
    categories = Text(analyzer='standard')
    location = Text(analyzer="standard")
    time = Text(analyzer='keyword')
    runtime = Integer()

    # override the Document save method to include subclass field definitions
    def save(self, *args, **kwargs):
        return super(Movie, self).save(*args, **kwargs)


# Populate the index
def buildIndex():
    """
    # definition of Index: The Index adds or updates a typed JSON document in a specific index, making it searchable.
    buildIndex creates a new film index, deleting any existing index of the same name.
    It loads a json file containing the movie corpus and does bulk loading using a generator function.
    """
    film_index = Index('sample_film_index')
    film_index.document(Movie)
    # deleting any existing index of the same name
    if film_index.exists():
        film_index.delete()
    film_index.create()

    # get json object movies
    with open('2018_movies.json', 'r', encoding='utf-8') as data_file:
        movies = json.load(data_file)
        size = len(movies)

    # Implemented as a generator, to return one movie with each call.
    # Note that we include the index name here.
    # The Document type is always 'doc'.
    # Every item to be indexed must have a unique key.
    def actions():
        # mid is movie id (used as key into movies dictionary)
        for mid in range(1, size + 1):
            yield {
                "_index": "sample_film_index",
                "_type": 'doc',
                "_id": mid,
                "title": movies[str(mid)]['Title'],
                "text": movies[str(mid)]['Text'],
                "starring": movies[str(mid)]['Starring'],
                "runtime": runtime_str2int(movies[str(mid)]['Running Time']),
                "language": list2str(movies[str(mid)]['Language']),
                "country": list2str(movies[str(mid)]['Country']),
                "director": list2str(movies[str(mid)]['Director']),
                "location": movies[str(mid)]['Location'],
                "time": movies[str(mid)]['Time'],
                "categories": movies[str(mid)]['Categories'],
            }
    # Action series for bulk loading with helpers.bulk function.
    helpers.bulk(es, actions())


def main():
    start_time = time.time()
    buildIndex()
    print("=*=*= Built index in {} seconds =*=*=".format(time.time() - start_time))


if __name__ == '__main__':
    main()
