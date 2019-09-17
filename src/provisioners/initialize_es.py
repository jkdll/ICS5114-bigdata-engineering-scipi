"""
The  purpose of this script is to deploy elasticsearch to the EC2 cluster and configure the servers.
"""
import initialize_helper as helper 
import subprocess
import time
import warnings
import urllib
from scp import SCPClient
with warnings.catch_warnings():
	warnings.simplefilter("ignore")
	import paramiko


def generate_commands(env_var,code_location,code_name,code_run):
	"""
	generate_commands
		This function generates the commands to deploy elasticsearch.
		We first set the environment variables passed through env_var.
		Next we install some required packages.
		Next we Unzip the code at the (remote) code_location.
		Finally we execute the service.
	"""
	commands = list()
	commands.append("echo \"#Starting!\" \n ")
	#commands.append("sudo su \n")
	for key, value in env_var.items():
		#commands.append("sudo echo \'export "+key+"=" + value.replace("\"","\\\"") + "\' >> ~/.bashrc \n ")
		commands.append("sudo echo \'export "+key+"=" + value + "\' >> ~/.bash_profile \n ")
	commands.append("sudo apt update && sudo apt install -y openjdk-8-jdk-headless && sudo apt-get install -y unzip \n ")
	#commands.append("cd "+code_location+" \n ")
	commands.append("unzip "+code_name+" \n ")
	commands.append("sudo sysctl -w vm.max_map_count=262144 \n ")
	#commands.append("exec bash \n ")
	#commands.append(""+code_run+" \n ")
	#commands.append("ls \n ")
	#commands.append("echo \"#READY!\" \n ")
	#commands.append("cat /etc/environment \n ")
	return commands


# Get Values from terraform
infra_data = helper.get_cloud_info('infra_values.json')
es_public_ips = infra_data['elasticsearch_instance']
es_private_ips = infra_data['elasticsearch_instance_priavteip']
instance_ssh = "scipi_ssh_key"
instance_username = 'ubuntu'


# Generate Env. Variables for Each Machine
count = 1
for private_ip in es_private_ips:
	# Declare Environment Variables
	env_variables = dict()
	env_variables['SIR_ES_IP'] = str(private_ip)
	env_variables['SIR_ES_PORT'] = '9202'
	env_variables['NODE_NAME'] = "node-" + str(count)
	if count == 1:
		# Master Node
		env_variables['NODE_ISMASTER'] = 'true'
		env_variables['NODE_ISDATA'] = 'false'
	else:
		env_variables['NODE_ISMASTER'] = 'false'
		env_variables['NODE_ISDATA'] = 'true'
	env_variables['SIR_ES_HOSTS'] = ("\""+str((",".join(es_private_ips)))+"\"").replace(",",":9330,")
	# Generate commands and put them into shell script.
	commands = generate_commands(env_variables,'/home/ubuntu/','es-node-code.zip','./elasticsearch-node/bin/elasticsearch')
	helper.put_commands(commands, './provisioners/elasticsearch-code/es-script.sh')

	print('SciPi>> Starting ElasticSearch Init > ' + env_variables['NODE_NAME'] + " at " + es_public_ips[count-1] )
	helper.run_script(instance_ssh,'ubuntu',es_public_ips[count-1],'./provisioners/elasticsearch-code/es-script.sh','./provisioners/elasticsearch-code/es-node-code.zip')
	time.sleep(180)
	helper.run_command(instance_ssh,'ubuntu',es_public_ips[count-1],'./elasticsearch-node/bin/elasticsearch ')
	print('SciPi>> Ending ElasticSearch Init >' + env_variables['NODE_NAME'] + " at " + es_public_ips[count-1] )
	count = count + 1

main_node = es_public_ips[0]
#helper.check_if_up('Elasticsearch','http://'+main_node+':9202',30,15)

