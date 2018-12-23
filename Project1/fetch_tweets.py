import twitter
import json
import time
import datetime
import os


from preprocess import preprocess

def serialize_date(date_obj, date_format):
    return date_obj.strftime(date_format)

langs = ['en', 'hi', 'th',  'es', 'fr']
filename = 'data_' + serialize_date(datetime.datetime.now(), "%Y%m%d-%H%M%S") + '.json'

api = twitter.Api(consumer_key='LFCYrBKm7xAgFm6QSU9PAfnLJ',
                  consumer_secret='djrqknN38E0qxwT0e1nVo08gjK3JFy6EoTWUOQ99GhJyhnc5A2',
                  access_token_key='87458605-rSPcfADQgQN2w5s4VBievnCCCznb3MoaWhqidZU9B',
                  access_token_secret='W9HGO8vF9Y3y9mg5bBCwcZKBvHdNWK0yUj6GzG1xIQLzt',
                  sleep_on_rate_limit=True)

print('Api authenticated')

terms = list()

print('Reading terms')
with open('terms.txt', 'r') as terms_file:
    for line in terms_file:
        line = line.strip()
        terms.append(line)

geocodes = list()

print('Reading Geocodes')
with open('geocodes.txt', 'r') as geocodes_file:
    for line in geocodes_file:
        line = line.strip()
        geocodes.append(line)

offset_days = 0

if not os.path.exists('Outputs/'):
    os.makedirs('Outputs/')

tweets = []

while (offset_days < 7):
    from_date = serialize_date(datetime.datetime.now() - datetime.timedelta(days=offset_days+1), "%Y-%m-%d")
    to_date = serialize_date(datetime.datetime.now() - datetime.timedelta(days=offset_days), "%Y-%m-%d")

    print('Fetching tweets from {} to {}'.format(from_date, to_date))
    for lang in langs:
        for term in terms:
            for geocode in geocodes:
                tweets = []
                terms_arr = term.split(',')
                geocode_arr = geocode.split(',')
                loc = geocode_arr[-1]
                filename =  terms_arr[0] + '_' + lang + '_' + loc + '_' + from_date + '_' + to_date + '.json'
                res_dict = api.GetSearch(term=terms_arr[0], lang=lang, geocode=','.join(geocode_arr[:-1]), count=100, since=from_date, until=to_date, return_json=True)
                    
                if len(res_dict['statuses']) == 0:
                    print('Encountered empty tweet. Skipping...')
                    continue

                with open(os.path.join('Outputs', filename), 'a') as out_file:
                    print('...Writing to file...')

                    for tweet in res_dict['statuses']:
                        if 'retweeted_status' in tweet:
                            print('Encountered retweet. Skipping...')
                            continue
                        tweet = preprocess(tweet)
                        tweet['topic'] = terms_arr[1]
                        tweet['city'] = loc
                        tweets.append(tweet)
                    print(json.dumps(tweets), file=out_file)
    offset_days += 1

