import sys
import tweepy
import os
import threading
from functools import wraps
import errno
import time
import signal
from slistener import SListener, terms

class TimeoutError(Exception):
    pass

def timeout(seconds=10, error_message=os.strerror(errno.ETIME)):
    def decorator(func):
        def _handle_timeout(signum, frame):
            raise TimeoutError(error_message)

        def wrapper(*args, **kwargs):
            signal.signal(signal.SIGALRM, _handle_timeout)
            signal.alarm(seconds)
            try:
                result = func(*args, **kwargs)
            finally:
                signal.alarm(0)
            return result

        return wraps(func)(wrapper)

    return decorator

if not os.path.exists('StreamOutputs/'):
    os.makedirs('StreamOutputs/')

## authentication
consumer_key='LFCYrBKm7xAgFm6QSU9PAfnLJ'
consumer_secret='djrqknN38E0qxwT0e1nVo08gjK3JFy6EoTWUOQ99GhJyhnc5A2'
access_token='87458605-rSPcfADQgQN2w5s4VBievnCCCznb3MoaWhqidZU9B'
access_secret='W9HGO8vF9Y3y9mg5bBCwcZKBvHdNWK0yUj6GzG1xIQLzt'

auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_secret)
api      = tweepy.API(auth)

geocodes = [
    [-99.3649, 19.0482, -98.9403, 19.5928, 'mexico city'],
    [77.048515, 28.481264, 77.241074, 28.645683, 'delhi'],
    [2.224122, 48.815575, 2.46976, 48.902157, 'paris'],
    [100.501765, 13.756331, 100.601765, 13.856331, 'bangkok'],
    [-74.005973, 40.712775, -73.005973, 40.912775, 'nyc']
]

langs = ['en', 'hi', 'th',  'es', 'fr']

@timeout(300)
def start_stream(term = None, lang = None, locs = None):
    stream = None
    try: 
        if term and lang:
            listen = SListener(api, term=term, lang=lang)
            stream = tweepy.Stream(auth, listen)
            stream.filter(languages = [lang], track = [term[0]])
        elif locs:
            listen = SListener(api, loc=locs)
            stream = tweepy.Stream(auth, listen)
            stream.filter(locations=locs[:-1])
    except:
        print("error!")
        stream.disconnect()

def main():
    queue = list()

    for loc in geocodes:
        queue.append({
            'locs': loc,
        })
    
    # for lang in langs:
    #     for term in terms:
    #         queue.append({
    #             'term': term,
    #             'lang': lang,
    #         })
    
    print('stream started')
    while True:
        obj = queue.pop(0)
        print('Now streaming for {}'.format(obj))
        if 'term' in obj:
            start_stream(term=obj['term'], lang=obj['lang'])
        else:
            start_stream(locs=obj['locs'])

        print('Switching topic..60 secs')
        time.sleep(60)
        print('stream started again!')
        queue.append(obj)


if __name__ == '__main__':
    main()
