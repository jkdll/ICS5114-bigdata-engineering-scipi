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
import sys

if len(sys.argv) < 2:
	print('No argument supplied, exiting...')
	exit()

mode = sys.argv[1]

def delivery_callback(err, msg):
	"""
	Function to tell us whether messages were delivered
	"""
        if err:
            sys.stderr.write('%% SciPi >> Message failed delivery: %s\n' % err)
        else:
            sys.stderr.write('%% SciPi >> Message delivered to %s [%d] @ %d\n' % (msg.topic(), msg.partition(), msg.offset()))

# Get Address of Kafka
infra_data = helper.get_cloud_info('infra_values.json')
instance_kafka = infra_data['kafka_instance']

# List of Files to send to Kafka
mag_path = './ingestion/data-mag/'
other_path = './ingestion/data-other/'

mag_files = os.listdir(mag_path)
mag_files = [mag_path + f for f in mag_files]
other_files = os.listdir(other_path)
other_files = [other_path + f for f in other_files]

if mode == '0':
	all_files = mag_files+other_files
elif mode == '1':
	all_files = mag_files
elif mode == '2':
	all_files = other_files

print('>>>>>' +instance_kafka)
# Initialize Producer
conf = {'bootstrap.servers': instance_kafka+":9092",'debug': 'broker,admin'}
p = Producer(**conf)

random.shuffle(all_files)
counter = 0

# Send each file to Kafka.
for file in all_files:
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
		message = line
		# For Debugging: message = '{ "event_type":"'+topic+'","event_msg":'+'"'+message+'"}'
		try:
			p.produce(topic, message.rstrip(), callback=delivery_callback)
		except BufferError:
			sys.stderr.write('%% SciPi >> Local producer queue is full (%d messages awaiting delivery): try again\n' % len(p)) 
		if counter%1000 == 0:
			time.sleep(30)
	f.close()
