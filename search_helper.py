import re
from collections import defaultdict
import json


def runtime_str2int(runtime):
    """transform runtime string into an integer"""
    runtime_str = re.findall(r'[0-9]+', str(runtime))
    ints_list = [int(i) for i in runtime_str]
    if len(ints_list) == 0:
        return
    if ints_list[0] > 9:
        return ints_list[0]
    elif len(ints_list) == 1 and ints_list[0] < 10:
        return ints_list[0] * 60
    elif len(ints_list) <= 3 and ints_list[0] < 10:
        return ints_list[0] * 60 + ints_list[1]
    else:
        return max(ints_list)


def list2str(starring):
    """transform a list into strings"""
    return ', '.join(starring) if isinstance(starring, list) else starring


def test_corpus(filename='test_corpus.json'):
    """Create a test corpus as a json file contains 10 hand-made documents."""
    test_corp = defaultdict(dict)
    titles = ['romance one', 'romance two', 'romance three', 'horror four', 'horror five', 'horror six',
              'comedy one', 'comedy two', 'comedy three', 'comedy four']
    texts = ['dramatic romance romance romance film', 'romance romance he likes it too films',
             'romance three is better than one film',
             'this film is dramatic', 'horror the bad guy changed dramatically',
             'horror romance romance i don\'t like it',
             'one is directed by Tom', 'tom also plans to direct five', 'Tom is the director', 'the director is tom']
    starrings = ['aa', 'bb', 'cc', ['aa', 'bb'], ['aa', 'cc', 'dd'], ['ff'], 'bb', 'dd', ['dd', 'cc'], 'tom']
    locations = ['USA', 'us', 'America', 'CA', 'UK', 'uk', 'canada', 'china', 'MA', '']
    directors = [['ben', 'Jim'], 'ben staling', 'ben', ['jerry wang', 'peter su', 'pepsi wong'], 'jerry staling', 'jerry', 'tom', 'tom', 'tom', 'tom']
    language = ['english', 'English', 'French', 'Hindi', 'Japanese', 'Korean language', 'Mandarin', 'Chinese', 'English', 'English']
    countries = ['United States', 'United States', 'America', 'UK', 'US', 'China', 'France', 'India', 'United Kingdom', 'Spain']
    runtime = ['101 minutes', '90 minutes', '2 hr 2 min', '1 hr 32 min', '95 minutes', '125 minutes', '1 hr 55 min', '2 hr', '', '']
    categories = ['2010s romantic drama films', '2016 films', '2016 films', '2010s horror thriller films',
                  'Indian film stubs', '2016 horror films', '2016 3D films', '2016 3D films', 'romantic drama films', 'Indian films']
    times = ['1990', '2010', '', '', '1937', '2008', '2025', '2048', '1911', '1876']
    for i, title, director, starring, location, text, lang, country, run, cat, time in \
            zip(range(1, 11), titles, directors, starrings, locations, texts, language, countries, runtime, categories, times):
        test_corp[str(i)]['title'] = title
        test_corp[str(i)]['director'] = director
        test_corp[str(i)]['starring'] = starring
        test_corp[str(i)]['location'] = location
        test_corp[str(i)]['text'] = text
        test_corp[str(i)]['language'] = lang
        test_corp[str(i)]['country'] = country
        test_corp[str(i)]['runtime'] = run
        test_corp[str(i)]['categories'] = cat
        test_corp[str(i)]['time'] = time

    json_obj = json.dumps(test_corp)
    with open(filename, 'w') as f:
        f.write(json_obj)
    print ('test corpus has been created!')

