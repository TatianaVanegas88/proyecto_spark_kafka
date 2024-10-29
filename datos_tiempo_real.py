
from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

conf = SparkConf().setAppName("RealTimeProcessing")
sc = SparkContext(conf=conf)
ssc = StreamingContext(sc, 10)

kafkaStream = KafkaUtils.createDirectStream(ssc, ["real_time_topic"], {"metadata.broker.list": "localhost:9092"})

def process_rdd(time, rdd):
    if not rdd.isEmpty():
        count = rdd.count()
        print(f"Contando eventos: {count}")

kafkaStream.foreachRDD(process_rdd)
ssc.start()
ssc.awaitTermination()
