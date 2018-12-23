import nltk
nltk.download('stopwords')
import preprocessor as p
import re
import datetime
from nltk.corpus import stopwords


p.set_options(p.OPT.URL, p.OPT.EMOJI, p.OPT.MENTION, p.OPT.HASHTAG, p.OPT.RESERVED, p.OPT.SMILEY)

lang_map = {
    'en': 'english',
    'es': 'spanish',
    'fr': 'french'
}

# with open('stopwords_hi.txt', 'r', encoding='utf-8') as f:
#     hindi_words = [x.strip() for x in f]

# with open('stopwords_hi.txt', 'r', encoding='utf-8') as f:
#     thai_words = [x.strip() for x in f]

# def remove_stop_words(stop_words, words):
#     return ' '.join(list(filter(lambda x: x not in stop_words, words)))

def preprocess(data):
    lang = data['lang']
    text_xx = 'text_' + lang

    data['tweet_date'] = datetime.datetime.strptime(data['created_at'], '%a %b %d %H:%M:%S %z %Y').strftime('%Y-%m-%dT%H:00:00.00Z')

    if 'extended_tweet' in data:
        data['tweet_text'] = data['extended_tweet']['full_text']
    else:
        data['tweet_text'] = data['text']
    
    if 'geo' in data and data['geo'] and 'coordinates' in data['geo'] and 'coordinates' in data['geo']['coordinates']:
        data['tweet_loc'] = ','.join(str(x) for x in data['geo']['coordinates']['coordinates'])
    elif 'place' in data and data['place'] and 'bounding_box' in data['place']:
        data['tweet_loc'] = ','.join(str(x) for x in data['place']['bounding_box']['coordinates'][0][0])

    parsed_text = p.parse(data['tweet_text'])
    data['tweet_emoticons'] = [t.match for e in [parsed_text.emojis, parsed_text.smileys] if e is not None for t in e ]
    data[text_xx] = p.clean(data['tweet_text']).lower()
    # words = re.findall(r'\w+', data[text_xx], flags = re.UNICODE)
    # if lang in lang_map:
    #     data[text_xx] = remove_stop_words(stopwords.words(lang_map[lang]), words)
    # elif lang == 'hi':
    #     data[text_xx] = remove_stop_words(hindi_words, words)
    # else:
    #     data[text_xx] = remove_stop_words(thai_words, words)
    return data