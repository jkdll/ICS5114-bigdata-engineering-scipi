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
esconf["es.query"] = '?q=message_processed:"false" AND topic:"data-mag"'
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
# Remove Corrupted Values
if '_corrupt_record' in pi.columns:
    pi = pi.where(col("_corrupt_record").isNull())

def removePunctuation(text):
    if text is None:
        return ''
    text=text.lower().strip()
    text=re.sub('[^0-9a-zA-Z ]','', text)
    return text

def generate_nlp_columns(input_dataset,target):
    udf_remove_punc = udf(lambda s: removePunctuation(s) )
    # Remove Punctuation
    input_dataset = input_dataset.withColumn(target,udf_remove_punc(target))
    # Tokenize Title
    tokenizer = Tokenizer(inputCol=target, outputCol=target+"_words")
    input_dataset = tokenizer.transform(input_dataset)
    # Remove Stop Words
    remover = StopWordsRemover(inputCol=target+"_words", outputCol=target+"_cleanwords")
    input_dataset = remover.transform(input_dataset)
    # Generate N-Grams 
    ngram = NGram(n=2, inputCol=target+"_cleanwords", outputCol=target+"_bigrams")
    input_dataset = ngram.transform(input_dataset)
    trigram = NGram(n=3, inputCol=target+"_cleanwords", outputCol=target+"_trigrams")
    input_dataset = trigram.transform(input_dataset)
    # Drop Extra Columns - Leave ngrams only.
    input_dataset = input_dataset.drop(target+"_words")
    input_dataset = input_dataset.drop(target+"_cleanwords")
    # Perform TFIDF
    #hashingTF = HashingTF(inputCol=target+"_trigrams", outputCol=target+"_hashing", numFeatures=20)
    #input_dataset = hashingTF.transform(input_dataset)
    #idf = IDF(inputCol=target+"_hashing", outputCol=target+"_features")
    #idfModel = idf.fit(input_dataset)
    #input_dataset = idfModel.transform(input_dataset) 
    return input_dataset

def trim_array(arr):
    for a in arr:
        a.trim()

# Filter Out Non-English Papers
pi = pi.filter(pi.lang == 'en')
# Generate N-Gram For Abstract and Title
if 'abstract' in pi.columns:
    pi = generate_nlp_columns(pi,'abstract')

pi = generate_nlp_columns(pi,'title')
# Clean Venue, Publisher
udf_scrub_text = udf(lambda s: s.strip().lower() if s is not None else None )
udf_split = udf(lambda s: s.split(',')[0] if s is not None else None )
if 'venue' in pi.columns:
    pi = pi.withColumn('venue',udf_split(pi.venue))

if 'publisher' in pi.columns:
    pi = pi.withColumn('publisher',udf_scrub_text(udf_split(pi.publisher)))

try:
    author = pi.select(f.explode('authors.name'))\
                        .select(f.trim(f.col('col'))\
                        .alias('author_name')).distinct()\
                        .withColumn('author_md5'\
                                ,f.md5(f.col('author_name')))
except:
    author = None

try:
    organisation = pi.select(f.explode('authors.org'))\
                        .select(f.trim(f.col('col'))\
                        .alias('organisation_name')).distinct()\
                        .withColumn('organisation_md5'\
                                ,f.md5(f.col('organisation_name')))
except:
    organisation = None

try:
    document = pi.select(f.trim(f.col('doc_type'))\
                         .alias('document_type') \
                        ).distinct()\
                        .withColumn('document_type_md5'\
                                ,f.md5(f.col('document_type')))
except:
    document = None

try:
    field = pi.select(f.explode('fos'))\
                        .select(f.trim(f.col('col')).alias('field'))\
                        .distinct()\
                        .withColumn('field_md5'\
                                ,f.md5(f.col('field')))
except:
    field = None

try:
    venue = pi.select(f.trim(f.col('venue')).alias('venue'))\
                .distinct()\
                .withColumn('venue_md5',f.md5(f.col('venue')))
except:
    venue = None

try:
    volume_issue = pi.select(f.concat('venue',f.lit('_'),'volume',f.lit('.'),'issue').alias('volume_issue'))\
                        .filter(pi.doc_type == 'Journal')\
                        .distinct()\
                        .withColumn('volume_issue_md5'\
                                ,f.md5(f.col('volume_issue')))
except:
    volume_issue = None

try:
    keyword = pi.select(f.explode('keywords'))\
                        .select(f.trim(f.col('col'))\
                        .alias('keyword')).distinct()\
                        .withColumn('keyword_md5'\
                                ,f.md5(f.col('keyword')))
except:
    keyword = None

try:
    year = pi.select('year').distinct().withColumn('year_md5'\
                                ,f.md5(f.col('year').cast(StringType())))
except:
    year = None

try:
    publisher = pi.select(f.trim(f.col('publisher')).alias('publisher'))\
                .distinct()\
                .withColumn('publisher_md5'\
                        ,f.md5(f.col('publisher')))
except:
    year = None

try:
    """paper = pi.select('id','title','abstract',\
                      'doi','lang','page_start',\
                      'page_end','n_citation','references')\
                    .withColumn('title_md5', f.md5(f.col('title')))\
                    .withColumn('source_id',f.col('id'))\
                    .withColumn('source_name',f.lit('data-mag'))"""
    paper = pi.withColumn('title_md5', f.md5(f.col('title')))\
                    .withColumn('source_id',f.col('id'))\
                    .withColumn('source_name',f.lit('data-mag'))
except:
    paper = None

try:
    bigrams = pi.select(f.explode('abstract_bigrams').alias('bigrams'))\
                    .union(pi.select(f.explode('title_bigrams')))\
                    .distinct().withColumn('bigrams_md5'\
                        ,f.md5(f.col('bigrams')))
except:
    bigrams = None

try:
    trigrams = pi.select(f.explode('abstract_trigrams').alias('trigrams'))\
                    .union(pi.select(f.explode('title_trigrams')))\
                    .distinct().withColumn('trigrams_md5'\
                        ,f.md5(f.col('trigrams')))
except:
    trigrams = None

def map_relations(row):
    newRow = []
    # Get Title
    title = row.title
    # Relation Paper->Author
    # Rleation Author->ORGANISATION
    if row.authors is not None:
        for author in row.authors:
            if hasattr(author, 'name') and author.name is not None:
                newRow.append([title,'PAPER',author.name,'AUTHOR','AUTHORED'])
            if hasattr(author, 'org') and author.org is not None:
                newRow.append([author.name,'AUTHOR',author.org,'ORGANISATION','MEMBER_OF'])
     
    # Relation Paper->DOC_TYPE
    if hasattr(row, 'doc') and row.doc_type is not None:
        newRow.append([title,'PAPER',row.doc_type,'DOC_TYPE','IS_ARTIFACT'])
    
    # Relation Paper->Field
    if hasattr(row, 'field') and row.fos is not None:
        for field in row.fos:
            newRow.append([title,'PAPER',field,'FIELD','IN_FIELD'])
        
    if hasattr(row, 'keywords') and row.keywords is not None:
        for keyword in row.keywords:
            if keyword is not None:
                newRow.append([title,'PAPER',keyword,'KEYWORD','HAS_KEYWORD'])
    
    if hasattr(row, 'publisher') and row.publisher is not None:
        newRow.append([title,'PAPER',row.publisher,'PUBLISHER','PUBLISHED_BY'])
    
    
    if hasattr(row, 'venue') and row.venue is not None:
        newRow.append([title,'PAPER',row.venue,'VENUE','AT'])
    
    if hasattr(row, 'year') and row.year is not None:
        newRow.append([title,'PAPER',str(row.year),'YEAR','PUBLISHED_YEAR'])
    
    if hasattr(row, 'abstract_bigrams') and row.abstract_bigrams is not None:
        for bigram in row.abstract_bigrams:
            if bigram is not None:
                newRow.append([title,'PAPER',bigram,'ABSTRACT_PHRASE','MENTIONS'])
    
    if hasattr(row, 'abstract_trigrams') and row.abstract_trigrams is not None:
        for trigram in row.abstract_trigrams:
            if trigram is not None:
                newRow.append([title,'PAPER',trigram,'ABSTRACT_PHRASE','MENTIONS'])
    
    if hasattr(row, 'title_bigrams') and row.title_bigrams is not None:
        for bigram in row.title_bigrams:
            if bigram is not None:
                newRow.append([title,'PAPER',bigram,'TITLE_PHRASE','TITLE_MENTIONS'])
    
    if hasattr(row, 'title_trigrams') and row.title_trigrams is not None:
        for trigram in row.title_trigrams:
            if trigram is not None:
                newRow.append([title,'PAPER',trigram,'TITLE_PHRASE','TITLE_MENTIONS'])          
    return newRow


relation = pi.select('title','authors','doc_type'\
                    ,'fos','keywords','publisher'
                    ,'venue','year','abstract_bigrams'\
                    ,'abstract_trigrams','title_bigrams'\
                     , 'title_trigrams')\
            .rdd.flatMap(map_relations).collect()

relation_schema = StructType([StructField('entity_a', StringType(), False),\
                     StructField('entity_type_a', StringType(), False),\
                     StructField('entity_b', StringType(), False),\
                     StructField('entity_type_b', StringType(), False),\
                     StructField('relation', StringType(), False)])

relation = sc.createDataFrame(relation,relation_schema)
relation = relation.withColumn('entity_a_md5',f.md5(relation.entity_a))\
                    .withColumn('entity_b_md5',f.md5(relation.entity_b))

graph_data = relation.groupBy('entity_a','entity_type_a','entity_a_md5'\
                ,'entity_b','entity_type_b','entity_b_md5'\
                ,'relation').count()\
                .withColumnRenamed("count", "score")

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

graph_data = graph_data.withColumn("upsert_id", f.concat('entity_a_md5',f.lit('_'),'entity_b_md5')) 
save_data(graph_data.distinct(),connect_string,'relations','upsert_id')

if author is not None:
    author_data = author.withColumn("upsert_id",f.col('author_md5'))
    save_data(author_data.distinct(),connect_string,'author','upsert_id')

if organisation is not None:
    organisation_data = organisation.withColumn("upsert_id",f.col('organisation_md5')).where(col("organisation_md5").isNotNull())
    save_data(organisation_data.distinct(),connect_string,'organisation','upsert_id')

if document is not None:
    document_data = document.withColumn("upsert_id",f.col('document_type_md5')).where(col("document_type_md5").isNotNull())
    save_data(document_data.distinct(),connect_string,'document','upsert_id')

if field is not None:
    field_data = field.withColumn("upsert_id",f.col('field_md5')).where(col("field_md5").isNotNull())
    save_data(field_data.distinct(),connect_string,'field','upsert_id')

if venue is not None:
    venue_data = venue.withColumn("upsert_id",f.col('venue_md5')).where(col("venue_md5").isNotNull())
    save_data(venue_data.distinct(),connect_string,'venue','upsert_id')

if volume_issue is not None:
    volume_issue_data = volume_issue.withColumn("upsert_id",f.col('volume_issue_md5')).where(col("volume_issue_md5").isNotNull())
    save_data(volume_issue_data.distinct(),connect_string,'volume_issue','upsert_id')

if keyword is not None:
    keyword_data = keyword.withColumn("upsert_id",f.col('keyword_md5')).where(col("keyword_md5").isNotNull())
    save_data(keyword_data.distinct(),connect_string,'keyword','upsert_id')

if year is not None:
    year_data = year.withColumn("upsert_id",f.col('year_md5')).where(col("year_md5").isNotNull())
    save_data(year_data.distinct(),connect_string,'year','upsert_id')

if publisher is not None:
    publisher_data = publisher.withColumn("upsert_id",f.col('publisher_md5')).where(col("publisher_md5").isNotNull())
    save_data(publisher_data.distinct(),connect_string,'publisher','upsert_id')

if paper is not None:
    paper_data = paper.withColumn("upsert_id",f.col('title_md5')).where(col("title_md5").isNotNull())
    save_data(paper_data.distinct(),connect_string,'paper','upsert_id')

if bigrams is not None:
    bigram_data = bigrams.withColumn("upsert_id",f.col('bigrams_md5')).where(col("bigrams_md5").isNotNull())
    save_data(bigram_data.distinct(),connect_string,'phrase','upsert_id')

if trigrams is not None:
    trigram_data = trigrams.withColumn("upsert_id",f.col('trigrams_md5')).where(col("trigrams_md5").isNotNull())
    save_data(trigram_data.distinct(),connect_string,'phrase','upsert_id')

update_processed = flat_data.withColumn('message_processed',f.lit("true"))
save_data(update_processed.distinct(),connect_string,'staging','id')

sc.catalog.clearCache()
sc.stop()