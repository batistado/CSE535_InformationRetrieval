# -*- coding: utf-8 -*-
import mtranslate
import json
import urllib.request
import os
import re
from langdetect import detect
import urllib

def parse_query_file(file_path):
    if not os.path.exists(file_path):
        print('Invalid Query file path')
        return

    queries = []
    with open(os.path.abspath(file_path), 'r', encoding='utf8') as qf:
        for line in qf:
            raw_query = re.sub(r':', '', line)
            raw_query = re.compile(r'^(\d+)\s(.*)$').search(raw_query)
            if raw_query:
                queries.append({
                    'rawQuery': raw_query.group(2),
                    'lang': detect(raw_query.group(2)),
                    'queryId': raw_query.group(1)
                })

    return queries

queries_data = parse_query_file('./queries.txt')

def generate_trec(queries_data, model='IRF18P3BM25'):
    result = []
    all_langs = {'en', 'ru', 'de'}
    base_url = 'http://localhost:8983/solr/{}/select?defType=edismax&fl=id,score&indent=on&wt=json&rows=20'.format(model)
    for query_data in queries_data:
        lang = query_data['lang']
        if lang not in all_langs:
            lang = 'en'
        all_langs.remove(lang)

        lang_queries = [urllib.parse.quote_plus(query_data['rawQuery'])]
        weights = ['text_' + lang + '^2.5+tweet_hashtags^1.5+_text_' + lang + '^2.7']
        while len(all_langs) > 0:
            curr_lang = all_langs.pop()
            lang_queries.append(urllib.parse.quote_plus(mtranslate.translate(query_data['rawQuery'], to_language=curr_lang)))
            weights.append('text_' + curr_lang + '^2.0+_text_' + curr_lang + '^2')
        inurl = base_url + '&q=' + '+OR+'.join(lang_queries) + '&qf=' + '+'.join(weights)
        all_langs = {'en', 'ru', 'de'}

        # print(inurl)
        response = urllib.request.urlopen(inurl)
        docs = json.load(response)['response']['docs']
        # # the ranking should start from 1 and increase
        rank = 1
        for doc in docs:
            result.append(query_data['queryId'] + ' ' + 'Q0' + ' ' + str(doc['id']) + ' ' + str(rank) + ' ' + str(doc['score']) + ' ' + model + '\n')
            rank += 1

    return result


def write_to_output(result):
    with open('output.txt', 'w') as out:
        out.writelines(result)


if __name__ == '__main__':
    write_to_output(generate_trec(queries_data))



