import initialize_helper as helper 
import subprocess
import time
import warnings
import urllib
from confluent_kafka.admin import AdminClient, NewTopic, NewPartitions, ConfigResource, ConfigSource
from confluent_kafka import KafkaException
with warnings.catch_warnings():
	warnings.simplefilter("ignore")
	import paramiko

def generate_commands(code_name,target_ip):
	"""
	generate_commands
		This function generates the commands to deploy elasticsearch.
		We first set the environment variables passed through env_var.
		Next we install some required packages.
		Next we Unzip the code at the (remote) code_location.
		Finally we execute the service.
	"""
	commands = list()
	#commands.append("sudo su \n")
	commands.append("sudo apt update -y && sudo apt install -y docker.io && sudo service docker start && sudo apt-get install -y unzip && sudo apt-get install -y docker-compose \n ")
	commands.append("unzip "+code_name+" \n ")
	commands.append("cd cp-all-in-one/; sudo echo 'TARGET_IP=" + target_ip + "' > .env \n ")
	commands.append("cd cp-all-in-one/; sudo docker-compose up -d --build \n")
	return commands


# Get Values from terraform
infra_data = helper.get_cloud_info('infra_values.json')
instance_kafka = infra_data['kafka_instance']
instance_ssh = "scipi_ssh_key"
instance_username = 'ubuntu'
print('SciPi >> Starting Kafka Deployment on machine with ip: ' + str(instance_kafka))
commands = generate_commands('kafka-cp-docker.zip',instance_kafka)
helper.transfer_file(instance_ssh,'ubuntu',instance_kafka,'./provisioners/kafka-code/kafka-cp-docker.zip')
for c in commands:
	helper.run_command_plain(instance_ssh,'ubuntu',instance_kafka,c)
	if 'install' in c:
		time.sleep(120)
	else:
		time.sleep(5)
print('SciPi >> Finished Kafka Deployment on machine with ip: ' + str(instance_kafka))
# Create Kafka Topics
conf = {'bootstrap.servers': str(instance_kafka)+":9092"}
a = AdminClient(conf)
topic = 'automated-topic'
helper.create_topics(a,['data-mag'])
helper.create_topics(a,['data-other'])
