"""
The  purpose of this script is to deploy siren investigate to the EC2 instance and configure the server.
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
	commands = list()
	#commands.append('sudo su')
	for key, value in env_var.items():
		commands.append("echo \'export "+key+"=" + value + "\' >> ~/.bash_profile \n ")
	commands.append("sudo apt update && sudo apt install -y openjdk-8-jdk-headless && sudo apt-get install -y unzip \n ")
	commands.append("unzip -o "+code_name+" \n ")
	#commands.append("exec bash \n ")
	#commands.append(""+code_run+" \n ")
	#commands.append("cat /etc/environment \n ")
	return commands


# Get Values from terraform
infra_data = helper.get_cloud_info('infra_values.json')
instance_kibi = infra_data['kibi_instance']
instance_es = infra_data['elasticsearch_instance_priavteip']
instance_ssh = "scipi_ssh_key"
instance_username = 'ubuntu'
env_variables = dict()

# Set Environment Variables
env_variables['SIR_ES_URL'] = "http://"+str(instance_es[0])+":9202"


# Generate the List of Commands to initialize the server.
commands = generate_commands(env_variables,'/home/ubuntu/','siren-investigate.zip','./siren-investigate/bin/investigate')
# Save Commands to Shell Script & Transfer/Run Script
print('SciPi>> Starting Kibi Init >  at ' + instance_kibi )
helper.put_commands(commands,'./provisioners/kibi-code/kibi-script.es')
helper.run_script(instance_ssh,'ubuntu',instance_kibi,'./provisioners/kibi-code/kibi-script.es','./provisioners/kibi-code/siren-investigate.zip')
time.sleep(120)

helper.run_command_plain(instance_ssh,'ubuntu',instance_kibi,"sed -i 's/\${SIR_ES_URL}/" + env_variables['SIR_ES_URL'].replace('/','\\/') + "/g' siren-investigate/config/investigate.yml")
helper.run_command_plain(instance_ssh,'ubuntu',instance_kibi,"sed -i 's/\${KIBI_URL}/0.0.0.0/g' siren-investigate/config/investigate.yml")

helper.run_command(instance_ssh,'ubuntu',instance_kibi,'./siren-investigate/bin/investigate')
print('SciPi>> Ending Kibi Init >  at ' + instance_kibi )
