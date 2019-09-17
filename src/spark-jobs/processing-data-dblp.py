from pyspark.sql import SparkSession
from pyspark.ml.feature import Tokenizer, RegexTokenizer
from pyspark.sql.functions import col, udf
from pyspark.sql.types import IntegerType, StringType, BooleanType, ArrayType, DoubleType, StructType, StructField
import pyspark.sql.functions as f
from langdetect import detect
from pyspark.ml.feature import StopWordsRemover, NGram, HashingTF, IDF, Tokenizer
import string
import re
import json
import sys
import xml.etree.ElementTree as ET

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
sc = SparkSession.builder.appName('Processing-Data-MAG')\
                    .config('spark.jars.packages','org.elasticsearch:elasticsearch-hadoop:6.7.2')\
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
esconf["es.query"] = '?q=message_processed:"false" AND topic:"data-other"'
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

def parse(s):
    try:
        s = s.strip().replace("&nbsp", "")
        root = ET.fromstring(s)
        authors = []
        for author in root.findall('.//author'):
            authors.append(author.text)
        authors = ",".join(authors)
        title = root.find('.//title').text
        year = root.find('.//year').text
        journal = root.find('.//journal').text
        datadict = {'title' : title, 'year' : year, 'journal' : journal,'author' : authors}
        return str(datadict).replace('\'','"')
    except Exception:
        return ''
# Convert to Json and Read
pi = flat_data.select(f.udf(parse)('value').alias('json'))
pi = sc.read.json(pi.rdd.map(lambda r: r.json))
# Remove Corrupt
if '_corrupt_record' in pi.columns:
    pi = pi.where(f.col('_corrupt_record').isNull())
pi = pi.drop('_corrupt_record')
# Get Authors
pi = pi.withColumn(
    "author",
    f.split(col("author"), ",\\s*").cast(ArrayType(StringType())).alias("author")
)
pi = pi.withColumn('author', f.explode(pi.author))

author = pi.select('author').withColumn('author_md5',f.md5(f.col('author')))
author = author.withColumn('upsert_id',f.md5(f.col('author')))

journal = pi.select('journal').withColumn('journal_md5',f.md5(f.col('journal')))
journal = journal.withColumn('upsert_id',f.md5(f.col('journal')))

paper = pi.select('title').where(pi.title.isNotNull()).withColumn('title_md5',f.md5(f.col('title')))
paper = paper.withColumn('upsert_id',f.col('title_md5'))

year = pi.select('year').withColumn('year_md5',f.md5(f.col('year')))
year = year.withColumn('upsert_id',f.md5(f.col('year')))

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

author_relations = pi.withColumn('entity_a_md5',f.md5(f.col('author')))\
            .withColumn('entity_type_a',f.lit('AUTHOR'))\
            .withColumn('entity_b_md5',f.md5(f.col('title')))\
            .withColumn('entity_type_b',f.lit('PAPER'))\
            .withColumn('relation',f.lit('AUTHORED'))
            
venue_relations = pi.withColumn('entity_a_md5',f.md5(f.col('journal')))\
            .withColumn('entity_type_a',f.lit('VENUE'))\
            .withColumn('entity_b_md5',f.md5(f.col('title')))\
            .withColumn('entity_type_b',f.lit('PAPER'))\
            .withColumn('relation',f.lit('AT'))
            
year_relations = pi.withColumn('entity_a_md5',f.md5(f.col('year')))\
            .withColumn('entity_type_a',f.lit('YEAR'))\
            .withColumn('entity_b_md5',f.md5(f.col('title')))\
            .withColumn('entity_type_b',f.lit('PAPER'))\
            .withColumn('relation',f.lit('PUBLISHED_YEAR'))

relations = author_relations.union(venue_relations).union(year_relations)
relations = relations.withColumn('upsert_id', f.concat('entity_a_md5',f.lit('_'),'entity_b_md5'))


if author is not None:
    save_data(author.distinct(),connect_string,'author','author_md5')

if journal is not None:
    save_data(journal.distinct(),connect_string,'venue','journal_md5')

if paper is not None:
    save_data(paper.distinct(),connect_string,'paper','title_md5')

if year is not None:
    save_data(year.distinct(),connect_string,'year','year_md5')

if relations is not None:
    save_data(relations.distinct(),connect_string,'relations','upsert_id')

sc.catalog.clearCache()
sc.stop()
