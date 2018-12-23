import mtranslate
import json, sys, os, re

def parse_json(file_path):
    try:
        with open(os.path.abspath(file_path), encoding='utf8') as f:
            return json.load(f)
    except FileNotFoundError:
        print('Invalid file path')


def translate_json(data: list):
    result = []
    total_tweets = len(data)
    count = 1
    for tweet in data:
        print('{} of {}...'.format(count, total_tweets))
        lang = tweet['lang']
        tweet['text_' + lang] = re.sub(r"(?:https?:\/\/)?(?:www\.)?[-a-zA-Z0-9@:%._\+~#=]{1,256}\.[a-z]{2,6}\b([-a-zA-Z0-9@:%_\+.~#?&//=]*)", "", tweet['text_' + lang])
        result.append(tweet)
        count += 1

    return result

def generate_translated_json(data):
    with open('translated_output.json', 'w') as out:
        json.dump(data, out)


if __name__ == '__main__':
    file_path = "./train.json"
    generate_translated_json(translate_json(parse_json(file_path)))
