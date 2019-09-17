from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import IntegerType, StringType, BooleanType, ArrayType, DoubleType, StructType, StructField
import pyspark.sql.functions as f
import sys
from graphframes import *
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
sc = SparkSession.builder.appName('Processing-Enrichment-LPA')\
                    .config('spark.jars.packages','org.elasticsearch:elasticsearch-hadoop:6.7.2,graphframes:graphframes:0.7.0-spark2.3-s_2.11,com.typesafe.scala-logging:scala-logging_2.11:3.5.0,com.typesafe.scala-logging:scala-logging-slf4j_2.11:2.1.2')\
                    .config("es.nodes.wan.only","true")\
                    .config("es.port",port)\
                    .config("es.nodes",ip)\
                    .config("es.index.read.missing.as.empty","true")\
                    .getOrCreate()

esconf={}
esconf["es.nodes"] = ip
esconf["es.port"] = port
esconf["es.nodes.wan.only"] = "true"
esconf["es.resource"] = (resource)
#esconf["es.query"] = '?q=message_processed:"false" AND topic:"data-mag"'

# Read in RDD
"""esRDD = sc.sparkContext.newAPIHadoopRDD(
    inputFormatClass="org.elasticsearch.hadoop.mr.EsInputFormat",
    keyClass="org.apache.hadoop.io.NullWritable", 
    valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable", 
    conf=esconf)

if esRDD.isEmpty():
    sys.exit()

data = esRDD.map(lambda item: [item[0],item[1]["entity_a"],item[1]["entity_a_md5"],item[1]["entity_type_a"]
                               ,item[1]["entity_b"],item[1]["entity_b_md5"],item[1]["entity_type_b"]
                               ,item[1]["score"],item[1]["upsert_id"],item[1]["relation"]] 
                )
flat_data = sc.createDataFrame(data,['id','entity_a','entity_a_md5','entity_type_a'
                                     ,'entity_b','entity_b_md5','entity_type_b'
                                    ,'score','upsert_id','relation'])"""

flat_data = sc.read.format("org.elasticsearch.spark.sql").options(**esconf).load()

# Build dataframe of <id,type> paris for updating data later.
type_lookup = flat_data.select(f.col('entity_a_md5').alias('id'),f.col('entity_type_a').alias('type')).distinct()\
        .union(flat_data.select(f.col('entity_b_md5').alias('id'),f.col('entity_type_b').alias('type')).distinct())\
        .distinct()
# Construct Edges - Simply ids for entity A and B in the graph, renamed to src and dest.
edges = flat_data.select(f.col('entity_a_md5').alias('src'),f.col('entity_b_md5').alias('dst')).distinct()
# Construct vertices - union of all items.
vertices = flat_data.select(f.col('entity_a_md5').alias('id'),f.col('entity_a').alias('name')).distinct()\
        .union(flat_data.select(f.col('entity_b_md5').alias('id'),f.col('entity_b').alias('name')).distinct())\
        .distinct()
# Perform label propogation
graph = GraphFrame(vertices, edges)
communities = graph.labelPropagation(maxIter=5)
# Build Dataframe to perform update
target_data = communities.alias('comm')\
            .join(type_lookup.alias('type'),type_lookup.id==communities.id,how='left')\
            .select('comm.id','comm.label','type.type')

# Generate Dataframes to perform updates 
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

# Author
author_updates = target_data.where("type like '%AUTHOR%'").distinct()\
                .groupBy(f.col("id"))\
                .agg(f.collect_list(f.col("label")).alias("lpa_communities"))
save_data(author_updates.distinct(), connect_string,'author','id')
# Phrase - Bigram/Trigram
phrase_updates = target_data.where("type like '%PHRASE%'").distinct()\
                .groupBy(f.col("id"))\
                .agg(f.collect_list(f.col("label")).alias("lpa_communities"))
save_data(phrase_updates.distinct(), connect_string,'phrase','id')
# Publisher
publisher_updates = target_data.where("type like '%PUBLISHER%'").distinct()\
                .groupBy(f.col("id"))\
                .agg(f.collect_list(f.col("label")).alias("lpa_communities"))
save_data(publisher_updates.distinct(), connect_string,'publisher','id')
# Field
field_updates = target_data.where("type like '%FIELD%'").distinct()\
                .groupBy(f.col("id"))\
                .agg(f.collect_list(f.col("label")).alias("lpa_communities"))
save_data(field_updates.distinct(), connect_string,'field','id')
# Paper
paper_updates = target_data.where("type like '%PAPER%'").distinct()\
                .groupBy(f.col("id"))\
                .agg(f.collect_list(f.col("label")).alias("lpa_communities"))
save_data(paper_updates.distinct(), connect_string,'paper','id')
# Organisation
organisation_updates = target_data.where("type like '%ORGANISATION%'").distinct()\
                .groupBy(f.col("id"))\
                .agg(f.collect_list(f.col("label")).alias("lpa_communities"))
save_data(organisation_updates.distinct(), connect_string,'organisation','id')
# Year
year_updates = target_data.where("type like '%YEAR%'").distinct()\
                .groupBy(f.col("id"))\
                .agg(f.collect_list(f.col("label")).alias("lpa_communities"))
save_data(year_updates.distinct(), connect_string,'year','id')
# Document Type
document_type_updates = target_data.where("type like '%DOC_TYPE%'").distinct()\
                .groupBy(f.col("id"))\
                .agg(f.collect_list(f.col("label")).alias("lpa_communities"))
save_data(document_type_updates.distinct(), connect_string,'document','id')
# Keyword
keyword_updates = target_data.where("type like '%KEYWORD%'").distinct()\
                .groupBy(f.col("id"))\
                .agg(f.collect_list(f.col("label")).alias("lpa_communities"))
save_data(keyword_updates.distinct(), connect_string,'keyword','id')
# Venue
venue_updates = target_data.where("type like '%VENUE%'").distinct()\
                .groupBy(f.col("id"))\
                .agg(f.collect_list(f.col("label")).alias("lpa_communities"))
save_data(venue_updates.distinct(), connect_string,'venue','id')

sc.catalog.clearCache()
sc.stop()