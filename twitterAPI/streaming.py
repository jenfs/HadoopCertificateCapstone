from slistener import SListener
import time, tweepy, sys, traceback

## authentication
consumer_key=""
consumer_secret=""
access_token=""
access_token_secret=""

auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.secure = True
auth.set_access_token(access_token, access_token_secret)

api = tweepy.API(auth)

def main():
    GEOBOX_US = [-124.47,24.0,-66.56,49.3843]
    
    listen = SListener(api, 'us-twitter')
    stream = tweepy.Stream(auth, listen)

    print "Streaming started..."

    try: 
        stream.filter(locations=GEOBOX_US)
    except:
        print "error: ", sys.exc_info()[0], traceback.format_exc()
        
        stream.disconnect()

if __name__ == '__main__':
    main()