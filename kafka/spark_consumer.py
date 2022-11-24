import findspark
findspark.init()

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import sys
sys.path.append("/root/project/src/")
from webapp.connection  import *
import os,json,re
from langdetect import detect
import gensim
from gensim import *
from gensim.utils import simple_preprocess
from gensim.parsing.preprocessing import STOPWORDS
from nltk.stem import WordNetLemmatizer, PorterStemmer
from nltk.stem.porter import *
from nltk.corpus import words
import time
from kafka import KafkaProducer




producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
			  value_serializer=lambda x: json.dumps(list(x)).encode('utf-8'))



def get_topics(text):
    
    ps = PorterStemmer()
        

    preprocessed_text =[]
    for token in gensim.utils.simple_preprocess(text):
        if token  not in gensim.parsing.preprocessing.STOPWORDS and len(token) > 5:
            lemmatized_text = ps.stem(WordNetLemmatizer().lemmatize(token, pos='v'))
            preprocessed_text.append(lemmatized_text)
                

    dictionary = gensim.corpora.Dictionary([preprocessed_text])

    corpus = [dictionary.doc2bow([text]) for text in preprocessed_text]
    ldamodel = gensim.models.ldamodel.LdaModel(corpus, num_topics=20, id2word = dictionary, passes=20)

    res = []
    for i in ldamodel.show_topics(formatted=False):
        res.append(i) 

    return res





def filter_by_language(text):
    try:
        lang = detect(text)
        return [lang,text]
    except:
        return [None]



def send_to_netezza(s):

        for i in s.collect()[0]:
        
            str1 = re.sub("'",'"',str(i))
            idadb.ida_query(f'''
                INSERT INTO topics(timestamp,topics) VALUES ({time.time()},'{str1}');''')




def spark_setup():
    sc= SparkContext(appName='twitter-kafka')

    ssc = StreamingContext(sc,60)


    message = KafkaUtils.createDirectStream(ssc,topics=['twitter'],kafkaParams={"metadata.broker.list":"localhost:9092"})
    
    #Convert json to dict
    dict_json = message.map(lambda x:json.loads(x[1]))
    #Extract text from the dict object
    text = dict_json.map(lambda x:x['text'])

    

    #Filter by english language and keep only the texts
    filtered_text = text.map(filter_by_language).filter(lambda a:a[0]=='en').map(lambda a:a[1])

    #Group by key and change the text to one big text block
    words = filtered_text.map(lambda x:[1,x]).reduceByKey(lambda a,b :a+b).map(lambda a:a[1])

    #Get the keywords in each topic
    topics = words.map(get_topics)
    #Send the extracted keywords to netezza
    topics.foreachRDD(send_to_netezza)
    

    ssc.start()
    ssc.awaitTermination()



if __name__=='__main__':
    spark_setup()
    

