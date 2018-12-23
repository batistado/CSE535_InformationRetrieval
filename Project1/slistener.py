from tweepy import StreamListener
import json, time, sys, datetime, os
from preprocess import preprocess

def serialize_date(date_obj, date_format):
    return date_obj.strftime(date_format)

## initializing
terms = []
print('Reading terms')
with open('terms.txt', 'r') as terms_file:
    for line in terms_file:
        line = line.strip().split(',')
        terms.append(line)

class SListener(StreamListener):
    def __init__(self, api = None, term = None, lang = None, loc = None):
        self.term = None
        self.loc = None
        self.lang = None
        if term and lang:
            self.fprefix = term[0] + '_' + lang + '_' + serialize_date(datetime.datetime.now(), "%Y%m%d-%H%M%S")
            self.term = term
            self.lang = lang
        elif loc:
            self.fprefix = loc[-1] + '_' + serialize_date(datetime.datetime.now(), "%Y%m%d-%H%M%S")
            self.loc = loc
        self.tail = '.json'
        self.api = api
        self.counter = 0

    def on_data(self, data):

        if  'in_reply_to_status' in data:
            self.on_status(data)
        elif 'delete' in data:
            delete = json.loads(data)['delete']['status']
            if self.on_delete(delete['id'], delete['user_id']) is False:
                return False
        elif 'limit' in data:
            if self.on_limit(json.loads(data)['limit']['track']) is False:
                return False
        elif 'warning' in data:
            warning = json.loads(data)['warnings']
            print(warning['message'])
            return False


    def on_status(self, status):
        status_dict = json.loads(status)

        if 'retweeted_status' in status:
            return
        
        status_dict = preprocess(status_dict)
        if self.term:
            status_dict['topic'] = self.term[1]
        else:
            for t in terms:
                if t[0] in status_dict['text']:
                    status_dict['topic'] = t[1]
                    break
            status_dict['tweet_loc'] = ','.join([str(x) for x in self.loc[0:2][::-1]])
            status_dict['city'] = self.loc[-1]

        file_name = self.fprefix + self.tail
        with open(os.path.join('./StreamOutputs', file_name), 'a') as out_file:
            print(json.dumps(status_dict), file=out_file)
        return

    def on_delete(self, status_id, user_id):
        return

    def on_limit(self, track):
        sys.stderr.write(track + "\n")
        return

    def on_error(self, status_code):
        sys.stderr.write('Error: ' + str(status_code) + "\n")
        time.sleep(60)
        return False

    def on_timeout(self):
        sys.stderr.write("Timeout, sleeping for 60 seconds...\n")
        time.sleep(60)
        return 