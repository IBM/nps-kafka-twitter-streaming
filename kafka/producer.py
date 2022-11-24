import tweepy
import os,json
from kafka import KafkaProducer
from dotenv import load_dotenv
load_dotenv()


class KafkaStreamingClient(tweepy.StreamingClient):

    def start_producer(self):
        self.producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
              value_serializer=lambda x: json.dumps(x).encode('utf-8')
            )
    
    def on_tweet(self, tweet):
        if (tweet.data['text']): print(tweet.data['text'])
        self.producer.send('twitter',tweet.data)

if __name__=='__main__':
    streaming_client = KafkaStreamingClient(os.getenv('BEARER_TOKEN'))
    streaming_client.start_producer()
    streaming_client.sample(expansions=['author_id'])
