python provisioners/initialize_notify.py DEPLOYMENT_STARTING
terraform apply -auto-approve
echo "SciPi>> Finished Building Resources"
terraform output -json > infra_values.json
echo "SciPi>> Infrastructure Details Saved to: infra_values.json"
echo "SciPi>> Sleeping for 2 minutes to give Infrastructure Time to Start"
sleep 2m
echo "SciPi>> Starting Initialization Script: Kafka"
python provisioners/initialize_kafka.py
echo "SciPi>> Starting Initialization Script: ElasticSearch"
python provisioners/initialize_es.py
echo "SciPi>> Transferring Spark Jobs to Cluster"
python provisioners/initialize_spark.py
echo "SciPi>> Starting Initialization Script: Kibi"
python provisioners/initialize_kibi.py
python provisioners/initialize_notify.py DEPLOYMENT_FINISHED
