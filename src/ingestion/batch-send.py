"""
	Send Data to kafka:
	Mode 0 = send data for both MAG and Other
	Mode 1 = send data for MAG only
	Mode 2 = send data for Other only
"""
import os, sys
from confluent_kafka import Producer
sys.path.insert(0, './provisioners/')
import initialize_helper as helper 
import time
import random
import json
import hashlib
from elasticsearch import Elasticsearch

# Get Address of Kafka
infra_data = helper.get_cloud_info('infra_values.json')
instance_es = infra_data['elasticsearch_instance'][0]

# List of Files to send to Kafka
mag_path = './ingestion/data-mag/'
other_path = './ingestion/data-other/'

mag_files = os.listdir(mag_path)
mag_files = [mag_path + f for f in mag_files]
other_files = os.listdir(other_path)
other_files = [other_path + f for f in other_files]
es = Elasticsearch(instance_es+":9202")
count = 0
# Send each file to Kafka.
for file in mag_files:
	# Publish to other by default, if it is MAG, publish to MAG.
	topic = 'data-other'
	if "data-mag" in file:
		print('mag')
		topic = 'data-mag'
	else:
		print('other!')
	# Open file and read line
	f = open(file, "r")
	for line in f.readlines():
		try:
			value = line
			obj = json.loads(value)
			title = obj['title']
			# Generate message_id
			m = hashlib.md5()
			m.update(title)
			message_id = m.hexdigest()
			message_recieved_ts = str(int(time.time()))
			message_processed = "false"
			es.update(index='staging',doc_type='doc'
				,id=message_id
				,body={'doc':{'value':value
				, 'message_recieved_ts':message_recieved_ts
				, 'message_processed':message_processed
				, 'message_id':message_id
				, 'topic':topic
				, 'id:':message_id}
				,'doc_as_upsert':True}
				)
			count = count + 1
		except Exception,e:
			continue
	print('Sent ' + ctr(count) + " Documents")
	f.close()
