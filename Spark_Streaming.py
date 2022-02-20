import findspark
findspark.init()

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

def main():
    sc = SparkContext(appName= 'Spark_Streaming')
    ssc = StreamingContext(sc, 30)

    record = KafkaUtils.createDirectStream(ssc, topics= ['Crypto-Tweets'], kafkaParams= {'metadata.broker.list': 'localhost:9092'})
    record.pprint()

    ssc.start()
    ssc.awaitTermination()

if __name__== "__main__":
    main()