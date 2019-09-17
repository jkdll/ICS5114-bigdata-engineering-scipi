from confluent_kafka import Consumer, KafkaException
import sys
import getopt
import json
import logging
from pprint import pformat
import os
sys.path.insert(0, './provisioners/')
import initialize_helper as helper 



def print_assignment(consumer, partitions):
	print('Assignment:', partitions)

# Get Address of Kafka
infra_data = helper.get_cloud_info('infra_values.json')
instance_kafka = infra_data['kafka_instance']

conf = {'bootstrap.servers': str(instance_kafka)+":9092", 'group.id': '1', 'session.timeout.ms': 6000,'auto.offset.reset': 'earliest'}
c = Consumer(conf)

c.subscribe(['data-mag'], on_assign=print_assignment)
try:
	while True:
		msg = c.poll(timeout=4)
		if msg is None:
			continue
		if msg.error():
			print('error happened...')
			raise KafkaException(msg.error())
		else:
			sys.stderr.write('%% %s [%d] at offset %d with key %s:\n' % (msg.topic(), msg.partition(), msg.offset(), str(msg.key())))
			print(msg.value())
except KeyboardInterrupt:
	sys.stderr.write('%% Aborted by user\n')
finally:
	# Close down consumer to commit final offsets.
	c.close()