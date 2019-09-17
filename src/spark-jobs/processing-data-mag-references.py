"""
This process extracts the references from MAG papers
"""
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
connect_string = str(argv_es_ip) + ":9202:staging/doc"
ip = connect_string.split(':')[0]
port = connect_string.split(':')[1]
resource = connect_string.split(':')[2]
index = resource.split('/')[0]
doc_type = resource.split('/')[1]
# Spark Session and ES Configuration
sc = SparkSession.builder.appName('Processing-Data-MAG-References')\
                    .config('spark.jars.packages','org.elasticsearch:elasticsearch-hadoop:6.7.2,graphframes:graphframes:0.7.0-spark2.3-s_2.11')\
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
# Get Staging Items which have been processed, but the references were not calculated. Only for MAG
esconf["es.query"] = '?q=NOT _exists_:references_calculated AND topic:data-mag AND message_processed:true'

# Read in RDD
esRDD = sc.sparkContext.newAPIHadoopRDD(
    inputFormatClass="org.elasticsearch.hadoop.mr.EsInputFormat",
    keyClass="org.apache.hadoop.io.NullWritable", 
    valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable", 
    conf=esconf)

if esRDD.isEmpty():
    sys.exit()

# Parse ES Data to a regular Dataframe
data = esRDD.map(lambda item: [item[0],item[1]["topic"]
                               ,item[1]["message_recieved_ts"],item[1]["message_id"]
                                   ,item[1]["message_processed"],item[1]["value"]] )
# Flat Data will contain only metadata
flat_data = sc.createDataFrame(data,['id','topic','message_recieved_ts','message_id','message_processed','value'])
# pi is the input of papers which we will process
pi = sc.read.json(flat_data.rdd.map(lambda r: r.value))

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
    #esconf["es.update.script.inline"] = "ctx._source.id = params.id"
    #esconf["es.update.script.params"] = "id:"
    esconf["es.write.operation"] = "upsert"    
    graph_dataframe.write.format("org.elasticsearch.spark.sql").options(**esconf).mode("append").save(resource+'/'+doc_type)

# Get References
reference = pi.withColumnRenamed('id','source_id').withColumnRenamed('title','source_title')\
            .select('source_id','source_title',f.explode('references').alias('reference_id'))\
            .where(f.col('references').isNotNull())
# Do Left Join to find References
reference = reference.join(pi, pi.id == reference.reference_id,how='left')\
                    .select('source_id','source_title','reference_id','title')\
                    .withColumnRenamed('title','reference_title')\
                    .withColumn('source_title_md5',f.md5(f.col('source_title')))\
                    .withColumn('reference_title_md5',f.md5(f.col('reference_title')))

""" 
We do a set difference between: Papers where some references are found 
and Papers where some references are not found. 
We only want to mark papers where all references are found, as 'processed'. 
Only these will be marked as processed.
After finding this information, we insert into the index.
"""
papers_all_found = (reference.where(col("reference_title").isNotNull())\
                        .select('source_title_md5').distinct()\
                .subtract(\
                          (reference.where(col("reference_title").isNull())\
                            .select('source_title_md5').distinct()
                           ).select('source_title_md5')\
                          ).select('source_title_md5'))
papers_all_found = papers_all_found.withColumn('references_calculated',f.lit('true'))
papers_all_found = papers_all_found.select(f.col("source_title_md5").alias("id")\
                                           ,f.col("references_calculated"))
save_data(papers_all_found, connect_string,'staging','id')

# Next we construct the relations structure and insert it.
relations = reference.where(col("reference_title").isNotNull())\
                            .select(f.col('source_title').alias('entity_a'),\
                            f.col('reference_title').alias('entity_b'),\
                            f.col('source_title_md5').alias('entity_a_md5'),\
                            f.col('reference_title_md5').alias('entity_b_md5'),\
                           ).withColumn('entity_type_a',f.lit('PAPER'))\
                            .withColumn('entity_type_b',f.lit('PAPER'))\
                            .withColumn('relation',f.lit('CITES'))\
                            .withColumn('score',f.lit('1'))\
                            .withColumn('upsert_id',f.concat('entity_a_md5',f.lit('_'),'entity_b_md5'))
save_data(relations.distinct(), connect_string,'relations','upsert_id')

sc.catalog.clearCache()
sc.stop()