import boto3
import initialize_helper as helper 
import socket
import os

# Get Values from terraform
infra_data = helper.get_cloud_info('infra_values.json')
ACCESS_KEY = infra_data['output_aws_access_key']
SECRET_KEY = infra_data['output_aws_secret_key']
AWS_REGION = infra_data['output_aws_region']
S3_BUCKET_NAME = infra_data['output_spark_s3']
EMR_MASTER = infra_data['emr_master_public_dns']
EMR_INGEST_MASTER = infra_data['emr_ingest_master_public_dns']

EMR_IP = socket.gethostbyname(EMR_MASTER)
EMR_INGEST_IP = socket.gethostbyname(EMR_INGEST_MASTER)

EMR_CLUSTERS = [EMR_IP,EMR_INGEST_IP]

INSTANCE_SSH = "scipi_ssh_key"

# Prepare Jobs in a zipfile
os.system("cp infra_values.json spark-jobs/infra_values.json")  
os.system("zip spark-jobs/spark-jobs.zip spark-jobs/* > /dev/null 2>&1")

# Upload Spark Jobs in Zip file to bucket.
s3 = boto3.client(
    's3',
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
)
filename = 'spark-jobs/spark-jobs.zip'
bucket_name = S3_BUCKET_NAME
s3.upload_file(filename, bucket_name, filename)

for cluster in EMR_CLUSTERS:
	# Change permissions for logging
	command = "sudo chmod 777 /stdout"
	helper.run_command_plain(INSTANCE_SSH,'hadoop',cluster,command)

	command = "sudo chmod 777 /stderr"
	helper.run_command_plain(INSTANCE_SSH,'hadoop',cluster,command)

	# Get Spark Jobs from Bucket and Unzip them on the master node.
	command = "rm -r spark-jobs/spark-jobs.zip"
	helper.run_command_plain(INSTANCE_SSH,'hadoop',cluster,command)

	command = "aws s3 cp s3://scipi/spark-jobs/spark-jobs.zip ."
	helper.run_command_plain(INSTANCE_SSH,'hadoop',cluster,command)

	command = "unzip -o spark-jobs.zip"
	helper.run_command_plain(INSTANCE_SSH,'hadoop',cluster,command)

# Run Real-time job
helper.run_command(INSTANCE_SSH,'hadoop',EMR_INGEST_IP,"spark-submit --master yarn --deploy-mode cluster --executor-memory 2G --packages org.apache.spark:spark-network-common_2.11:2.3.2,org.elasticsearch:elasticsearch-hadoop:7.0.1,org.apache.spark:spark-streaming-kafka-assembly_2.11:1.6.3 --files spark-jobs/infra_values.json spark-jobs/ingestion-realtime-kafka.py")
# Add Other Jobs to Second Cluster
command = "crontab -l > mycron"
helper.run_command_plain(INSTANCE_SSH,'hadoop',EMR_IP,command)

run_command = "spark-submit --master yarn --deploy-mode cluster --executor-memory 4G --packages org.elasticsearch:elasticsearch-hadoop:6.7.2,org.apache.spark:spark-streaming-kafka-assembly_2.11:1.6.3 --files spark-jobs/infra_values.json /home/hadoop/spark-jobs/processing-data-mag.py"
command = "echo '*/5 * * * * "+run_command+"' >> mycron;"
helper.run_command_plain(INSTANCE_SSH,'hadoop',EMR_IP,command)

run_command = "spark-submit --master yarn --deploy-mode cluster --executor-memory 4G --packages org.elasticsearch:elasticsearch-hadoop:6.7.2,org.apache.spark:spark-streaming-kafka-assembly_2.11:1.6.3 --files spark-jobs/infra_values.json /home/hadoop/spark-jobs/processing-data-dblp.py"
command = "echo '*/5 * * * * "+run_command+"' >> mycron;"
helper.run_command_plain(INSTANCE_SSH,'hadoop',EMR_IP,command)

run_command = "spark-submit --master yarn --deploy-mode cluster --executor-memory 4G --packages org.elasticsearch:elasticsearch-hadoop:6.7.2 --files spark-jobs/infra_values.json /home/hadoop/spark-jobs/processing-data-mag-references.py"
command = "echo '*/5 * * * * "+run_command+"' >> mycron;"
helper.run_command_plain(INSTANCE_SSH,'hadoop',EMR_IP,command)

run_command = "spark-submit --master yarn --deploy-mode cluster --executor-memory 4G --packages org.elasticsearch:elasticsearch-hadoop:6.7.2,graphframes:graphframes:0.7.0-spark2.3-s_2.11,com.typesafe.scala-logging:scala-logging_2.11:3.5.0,com.typesafe.scala-logging:scala-logging-slf4j_2.11:2.1.2 --files spark-jobs/infra_values.json /home/hadoop/spark-jobs/processing-enrichment-lpa.py"
command = "echo '*/5 * * * * "+run_command+"' >> mycron;"
helper.run_command_plain(INSTANCE_SSH,'hadoop',EMR_IP,command)


run_command = "spark-submit --master yarn --deploy-mode cluster --executor-memory 4G --packages org.elasticsearch:elasticsearch-hadoop:6.7.2,com.typesafe.scala-logging:scala-logging_2.11:3.5.0,com.typesafe.scala-logging:scala-logging-slf4j_2.11:2.1.2 --files spark-jobs/infra_values.json /home/hadoop/spark-jobs/processing-relations.py"
command = "echo '*/5 * * * * "+run_command+"' >> mycron;"
helper.run_command_plain(INSTANCE_SSH,'hadoop',EMR_IP,command)


command = 'crontab mycron'
helper.run_command_plain(INSTANCE_SSH,'hadoop',EMR_IP,command)
command = 'rm mycron'
helper.run_command_plain(INSTANCE_SSH,'hadoop',EMR_IP,command)

s3.delete_object(Bucket='scipi',Key='spark-jobs/spark-jobs.zip')


#spark-submit --master yarn --deploy-mode cluster --files spark-jobs/infra_values.json spark-jobs/processing-enrichment-lpa.py