import requests as req
import json
import os
import threading
import queue
from collections import namedtuple
from sys import stderr

debug = (lambda *X: print(*X,file=stderr, flush=True)) if (os.getenv('NEWS_LOG_DEBUG') or '').lower()== 'true' else (lambda *X: None)
log = lambda *X: print(*X,file=stderr, flush=True) 

def notify_log(*X):
    log(*X)
    publish(
        url.publish_telegram % config.telegram_token, 
        params={
            'chat_id':config.maintainer_chatId,
            'text':' '.join(map(str,X)) 
        }
    )
    

url = (lambda X: namedtuple('Url', list(X.keys()))(**X))({
    'tweet_stream'  : 'http://127.0.0.1:5000/image/mmh.json', 
    'tweet_detail' : 'https://api.twitter.com/2/tweets/%d',
    'nft_mints': 'https://nft.kodadot.xyz',
    'publish_telegram' : 'https://api.telegram.org/bot%s/sendMessage'
})


config = (lambda X: namedtuple('Config', list(X.keys()))(**X))({
    'twitter_auth': {'Authorization' : 'Bearer %s' % os.getenv('NEWS_TWITTER_TOKEN')},
    'telegram_token' : os.getenv('NEWS_TELEGRAM_TOKEN'),
    'telegram_chatId': os.getenv('NEWS_TELEGRAM_CHATID'),
    'maintainer_chatId': os.getenv('NEWS_MAINTAINER_CHATID')
})

message_template = """I have just found this new mint: %s"""

def expand_urls(tweet_id):
    #debug('aAAA' ,tweet_id,'AAAAA ')
    response  = req.get(url.tweet_detail%int(tweet_id), params={'tweet.fields':'entities'}, headers=config.twitter_auth) #TODO: check status_code
    #debug("expand_urls:",response.request.url)
    #debug(response.json())
    return set(filter(
        lambda X: 0==X.find(url.nft_mints),  
        map(lambda X: X['expanded_url'], 
            response.json()['data']['entities']['urls'])
    ))

def publish(url, headers=None, params=None, data=None, command='get'):
    response = getattr(req,command)(url ,headers=headers, params=params, data=data)

    if response.status_code != 200:
        print('http failed with %d to url=%s with body=%s' %(response.status_code,response.request.url , response.request.body), file=stderr)
                        

def broadcast_tweets(out_queue):
    while True:
        tweet = out_queue.get()
        if tweet == None:
            break
        #debug(tweet)
        urls = expand_urls(tweet['data']['id']) 
        debug('Broadcast: expanded urls are=%s'%(urls,))
        if len(url) < 1:
            print("Found no url in matched tweet ID:%s by rule(s) %s" %(tweet['data']['id'], json.dumps(list(map(lambda X:X['tag'], tweet['matching_rules'])))), file=stderr)
            continue
        for u in urls: 
            msg = message_template % u
            publish(url.publish_telegram % config.telegram_token, params={'chat_id':config.telegram_chatId ,'text':msg})


def consume_tweets(out_queue):
    debug("Consumer init")
    while True:
        try: 
            stream = req.get(url.tweet_stream, headers=config.twitter_auth, stream=True)
            debug("Consumer: stream.status_code = %d, stream.request.url = %s" \
                  %(stream.status_code, stream.request.url))
            for tweet in stream.iter_lines(): #TODO connection retention #TODO remember broadcasted art
                debug('Consumer: recv of line=%s'% tweet)
                if tweet:
                    out_queue.put(json.loads(tweet))
        except req.exceptions.ChunkedEncodingError:
            debug('Consumer: reinit broken stream')
            continue
        except BaseException as e:
            debug("type=%s, args=%s" %(str(type(e)), ' '.join(map(str, e.args))))  
            break
        log("Consumer: Gratious end of the stream")
        break
    out_queue.put(None)

def main():
    q = queue.Queue()
    consume = threading.Thread(target=consume_tweets, args=(q,), name='Consumer')
    broadcast = threading.Thread(target=broadcast_tweets, args=(q,), name='Broadcaster')
    consume.start()
    broadcast.start()
    consume.join()
    broadcast.join()


if __name__ == '__main__':
    main()
