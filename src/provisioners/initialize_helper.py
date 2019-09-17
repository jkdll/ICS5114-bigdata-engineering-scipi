import json
import os
import initialize_helper as helper 
import subprocess
import time
import warnings
import urllib
from confluent_kafka.admin import AdminClient, NewTopic, NewPartitions, ConfigResource, ConfigSource
from confluent_kafka import KafkaException
from scp import SCPClient
with warnings.catch_warnings():
	warnings.simplefilter("ignore")
	import paramiko

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

def put_commands(commands,script_name):
	"""
	Generate a Shell script with the commands, place in script_name.
	"""
	with open(script_name, 'w') as f:
		for c in commands:
			f.write(c)

def check_if_up(application_name, url_string, wait_time, count_limit):
	"""
	check_if_up
		In this function we check if an application is up by performing a get request to a URL.
		If it is not up, we wait for a few seconds (defined by wait_time).
		We repeat this process for a certain number of times (count_limit)
	:param application_name: The name of the application we're checking
	:param url_string: The URL to perform the request to.
	:param wait_time: The time in seconds to wait after a check failes.
	:param count_limit: The number of checks to perform.
	"""
	return_status = -1
	count = 0
	while(return_status != 200):
		print('SciPi>> Checking if '+application_name+' is up at: '+url_string)
		try:
			return_status = urllib.urlopen(url_string).getcode()
		except:
			pass
		if return_status == 200:
			print('SciPi>> '+application_name+' is up!')
			break
		else:
			print('SciPi>> '+application_name+' is not up yet!')
		if count >= count_limit:
			break
		time.sleep(wait_time)
		count = count + 1

def run_script(ssh_key,un,ip_address,script,code):
	"""
	run_script
		This function transfers two components to a target server via SSH.
		The first component is the 'script' - The initialization shell script, containing code to run on the target server.
		The second component is the 'code'  - The application code, containing code to run on the target server.
		After transfer, we execute the script and wait for its termination.
	:param ssh_key: The SSH Key used to log into the machine.
	:param un: The Username of the machine to initialize.
	:param ip_address: The IP Address of the machine to initialize.
	:param script: The local path to the initialization script to transfer and run.
	:param code: The local path to the code to run.
	"""
	# Open connections
	ssh = paramiko.SSHClient()
	ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
	ssh.connect(ip_address, username=un, key_filename=ssh_key)
	# Transfer Files to Server
	scp = SCPClient(ssh.get_transport())
	scp.put(code, recursive=True, remote_path='/home/ubuntu/')
	scp.put(script, recursive=True, remote_path='/home/ubuntu/')
	scp.close()
	# Run Script
	time.sleep(3)
	# Generate Run Script Command & Send
	init_script_command = './'+script.split('/')[-1]
	#stdin, stdout, stderr = ssh.exec_command('sudo chmod 777 ' + script.split('/')[-1] + ' && nohup ' + run_script_command + ' ')
	full_command = ('nohup bash -c "sudo chmod 777 ' + script.split('/')[-1] + ' && ' + init_script_command + '" > nohup.out 2> nohup.err < /dev/null &')
	stdin, stdout, stderr = ssh.exec_command(full_command)
	exit_status = stdout.channel.recv_exit_status()
	if exit_status == 0:
	    print('SciPi>> Node with ' +ip_address+ ' deployed')
	else:
	   print("ScuPi>>Error", exit_status)
	ssh.close()	

def run_command(ssh_key,un,ip_address,command):
	"""
	run_script
		This function transfers two components to a target server via SSH.
		The first component is the 'script' - The initialization shell script, containing code to run on the target server.
		The second component is the 'code'  - The application code, containing code to run on the target server.
		After transfer, we execute the script and wait for its termination.
	:param ssh_key: The SSH Key used to log into the machine.
	:param un: The Username of the machine to initialize.
	:param ip_address: The IP Address of the machine to initialize.
	:param script: The local path to the initialization script to transfer and run.
	:param code: The local path to the code to run.
	"""
	# ssh -i scipi_ssh_key ubuntu@18.188.198.187 -l ubuntu  "source ~/.bash_profile; nohup ./elasticsearch-node/bin/elasticsearch > nohup-runservice.out 2> nohup-runservice.err < /dev/null &"
	command = 'ssh -o StrictHostKeyChecking=no -q -i ' + ssh_key + ' ' + un + '@' + ip_address + ' -l ' + un + ' "source ~/.bash_profile; nohup '+command+' > nohup-runservice.out 2> nohup-runservice.err < /dev/null &"'
	# For Debugging purposes:
	# print(command)
	subprocess.call(command, shell=True)

def run_command_plain(ssh_key,un,ip_address,command):
	"""
	run_script
		This function transfers two components to a target server via SSH.
		The first component is the 'script' - The initialization shell script, containing code to run on the target server.
		The second component is the 'code'  - The application code, containing code to run on the target server.
		After transfer, we execute the script and wait for its termination.
	:param ssh_key: The SSH Key used to log into the machine.
	:param un: The Username of the machine to initialize.
	:param ip_address: The IP Address of the machine to initialize.
	:param script: The local path to the initialization script to transfer and run.
	:param code: The local path to the code to run.
	"""
	# ssh -i scipi_ssh_key ubuntu@18.188.198.187 -l ubuntu  "source ~/.bash_profile; nohup ./elasticsearch-node/bin/elasticsearch > nohup-runservice.out 2> nohup-runservice.err < /dev/null &"
	command = 'ssh -o StrictHostKeyChecking=no -i ' + ssh_key + ' ' + un + '@' + ip_address + ' -l ' + un + ' " '+command+' > /dev/null 2>&1" > /dev/null 2>&1 '
	print(command)
	# For Debugging purposes
	# print(command)
	subprocess.call(command, shell=True)

def transfer_file(ssh_key,un,ip_address,file_path):
	"""
	transfer_file
		Simple function which transfers a file via ssh.
	"""
	ssh = paramiko.SSHClient()
	ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
	ssh.connect(ip_address, username=un, key_filename=ssh_key)
	# Transfer Files to Server
	scp = SCPClient(ssh.get_transport())
	scp.put(file_path, recursive=True, remote_path='/home/ubuntu/')
	scp.close()
	ssh.close()

def download_file(ssh_key,un,ip_address,remotepath,localpath):
	"""
	download_file
		Simple function which downloads a file to disk.
		Onle for Spark
	"""
	ssh = paramiko.SSHClient()
	ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
	ssh.connect(ip_address, username=un, key_filename=ssh_key)
	scp = SCPClient(ssh.get_transport())
	scp.get(remote_path=remotepath,local_path=localpath,recursive=True)
	scp.close()
	ssh.close()

def transfer_file_remote(ssh_key,un,ip_address,remotepath,localpath):
	"""
	transfer_file
		Simple function which transfers a file via ssh - specifying remote path.
	"""
	ssh = paramiko.SSHClient()
	ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
	ssh.connect(ip_address, username=un, key_filename=ssh_key)
	# Transfer Files to Server
	scp = SCPClient(ssh.get_transport())
	scp.put(localpath, recursive=True, remote_path=remotepath)
	scp.close()
	ssh.close()

def create_topics(a, topics):
    """
    create_topics
    Function to create topics for Scipi 
    """

    new_topics = [NewTopic(topic, num_partitions=3, replication_factor=1) for topic in topics]
    # Call create_topics to asynchronously create topics, a dict
    # of <topic,future> is returned.
    fs = a.create_topics(new_topics)

    # Wait for operation to finish.
    # Timeouts are preferably controlled by passing request_timeout=15.0
    # to the create_topics() call.
    # All futures will finish at the same time.
    for topic, f in fs.items():
        try:
            f.result()  # The result itself is None
            print("SciPi>> Kafka >> Topic {} created".format(topic))
        except Exception as e:
            print("SciPi>> Kafka >> Failed to create topic {}: {}".format(topic, e))


