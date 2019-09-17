from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import IntegerType, StringType, BooleanType, ArrayType, DoubleType, StructType, StructField
import pyspark.sql.functions as f
import sys
import json


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
argv_es_ip = details['elasticsearch_instance'][0]

# Elasticsearch Connection Details
connect_string = str(argv_es_ip) + ":9202:relations/doc"
ip = connect_string.split(':')[0]
port = connect_string.split(':')[1]
resource = connect_string.split(':')[2]
index = resource.split('/')[0]
doc_type = resource.split('/')[1]

# Spark Session and ES Configuration
sc = SparkSession.builder.appName('BuildGraph')\
                    .config('spark.jars.packages','org.elasticsearch:elasticsearch-hadoop:6.7.2,com.typesafe.scala-logging:scala-logging-slf4j_2.11:2.1.2,com.typesafe.scala-logging:scala-logging_2.11:3.5.0')\
                    .config("es.nodes.wan.only","true")\
                    .config("es.port",port)\
                    .config("es.nodes",ip)\
                    .getOrCreate()

esconf={}
esconf["es.nodes"] = ip
esconf["es.port"] = port
esconf["es.nodes.wan.only"] = "true"
esconf["es.resource"] = (resource)
#esconf["es.query"] = '?q=message_processed:"false" AND topic:"data-mag"'

relation_data = sc.read.format("org.elasticsearch.spark.sql").options(**esconf).load()


author_relations = relation_data.select('entity_a_md5','entity_b_md5','entity_type_a')\
                    .where(relation_data.entity_type_a == 'AUTHOR')\
                    .groupBy(f.col("entity_a_md5").alias('upsert_id'))\
                    .agg(f.collect_list(f.col("entity_b_md5")).alias("related_entities_md5"))

paper_relations = relation_data.select('entity_a_md5','entity_b_md5','entity_type_a')\
                    .where(relation_data.entity_type_a == 'PAPER')\
                    .groupBy(f.col("entity_a_md5").alias('upsert_id'))\
                    .agg(f.collect_list(f.col("entity_b_md5")).alias("related_entities_md5"))

def save_data(graph_dataframe, connect_string,res,upsert_column):
    """
    connect_string format:
        <ip>:<port>:index/doc_type
    """
    ip = connect_string.split(':')[0]
    port = connect_string.split(':')[1]
    resource = res
    doc_type = connect_string.split('/')[1]
    esconf={}
    esconf["es.mapping.id"] = upsert_column
    esconf["es.nodes"] = ip
    esconf["es.port"] = port
    esconf["es.nodes.wan.only"] = "true"
    esconf["es.write.operation"] = "upsert"    
    graph_dataframe.write.format("org.elasticsearch.spark.sql").options(**esconf).mode("append").save(resource+'/'+doc_type)

save_data(author_relations.distinct(),connect_string,'author','upsert_id')
save_data(paper_relations.distinct(),connect_string,'paper','upsert_id')

sc.catalog.clearCache()
sc.stop()