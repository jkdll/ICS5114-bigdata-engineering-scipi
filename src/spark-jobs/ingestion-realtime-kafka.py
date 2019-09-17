import sys
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import pyspark.sql.functions as f
from pyspark.sql.types import *
import json
import datetime
import sched, time
import sys

def get_cloud_info(location):
    """
    This method parses terraform output in json into a dictionary.
    """
    params = dict()
    # Read in the file
    with open(location, 'r') as myfile: data=myfile.read()
    obj = json.loads(data)
    for o in obj:
        params[o] = obj[o]['value']
    return params

details = get_cloud_info('infra_values.json')
argv_kafka_ip = details['kafka_instance']
argv_es_ip = details['elasticsearch_instance'][0]

def save_to_stage(rdd):
    """
    This method handles the kafka messages - we simply want to save them to the staging index for processing.
    """    
    # If we get an empty message, do nothing (should not happen!)
    if rdd.isEmpty():
        return

    esconf={}
    esconf["es.mapping.id"] = 'message_id'
    esconf["es.index.auto.create"] = "true"
    esconf["es.nodes"] = ip
    esconf["es.port"] = port
    esconf["es.nodes.wan.only"] = "true"
    esconf["es.write.operation"] = "index"
    sqlContext = SQLContext.getOrCreate(SparkContext.getOrCreate())
    df = sqlContext.createDataFrame(rdd,samplingRatio=1).toDF("topic","key", "value")
    # Add Identifier, Recieved Timestamp, and Boolean Flag to indicate not processed
    df = df.drop(df.key)
    df = df.withColumn('message_id', f.md5(df.value))
    df = df.withColumn("message_recieved_ts", f.lit(f.current_timestamp()))
    df = df.withColumn("message_processed", f.lit('false'))
    df.write.format("org.elasticsearch.spark.sql").options(**esconf).mode("append").save(resource)
    

def addtopic(m):
    """
    Message Handler to get metadata.
    """
    return m and (m.topic, m.key,m.message)
    
# Kafka Variables
#kafka_brokers = "52.14.240.200:9092"
kafka_brokers = str(argv_kafka_ip)+":9092"
kafka_topics = ["data-mag","data-other"]
# Elasticsearch Details
# connect_string = "18.223.190.1:9202:staging/doc"
connect_string = str(argv_es_ip) + ":9202:staging/doc"
ip = connect_string.split(':')[0]
port = connect_string.split(':')[1]
resource = connect_string.split(':')[2]
index = resource.split('/')[0]
doc_type = resource.split('/')[1]

# Create Spark Session & Streaming Context
sc = SparkSession.builder.appName('Ingestion-Realtime-Kafka')\
                    .config('spark.jars.packages','org.apache.spark:spark-network-common_2.11:2.3.3,org.elasticsearch:elasticsearch-hadoop:7.0.1,org.apache.spark:spark-streaming-kafka-assembly_2.11:1.6.3')\
                    .config('spark.executor.memory',"7g")\
                    .config('spark.logConf',"true")\
                    .getOrCreate()
ssc = StreamingContext(sc.sparkContext, 1)
kvs = KafkaUtils.createDirectStream(ssc, kafka_topics,{"metadata.broker.list": kafka_brokers},messageHandler=addtopic)
# Save Data Recieved
kvs.foreachRDD(save_to_stage)
ssc.start()
ssc.awaitTermination()