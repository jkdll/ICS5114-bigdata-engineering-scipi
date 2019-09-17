import initialize_helper as helper 
import sys

def print_art(message):
	print("""
 .----------------.  .----------------.  .----------------.  .----------------.  .----------------.                                                                                 
| .--------------. || .--------------. || .--------------. || .--------------. || .--------------. |                                                                                
| |    _______   | || |     ______   | || |     _____    | || |   ______     | || |     _____    | |                                                                                
| |   /  ___  |  | || |   .' ___  |  | || |    |_   _|   | || |  |_   __ \   | || |    |_   _|   | |                                                                                
| |  |  (__ \_|  | || |  / .'   \_|  | || |      | |     | || |    | |__) |  | || |      | |     | |                                                                                
| |   '.___`-.   | || |  | |         | || |      | |     | || |    |  ___/   | || |      | |     | |                                                                                
| |  |`\____) |  | || |  \ `.___.'\  | || |     _| |_    | || |   _| |_      | || |     _| |_    | |                                                                                
| |  |_______.'  | || |   `._____.'  | || |    |_____|   | || |  |_____|     | || |    |_____|   | |                                                                                
| |              | || |              | || |              | || |              | || |              | |                                                                                
| '--------------' || '--------------' || '--------------' || '--------------' || '--------------' |                                                                                
 '----------------'  '----------------'  '----------------'  '----------------'  '----------------'                                                                                 
>> """+message+"""
		""")

def print_deployment_success(infra_data):
	instance_kafka = "http://" + str(infra_data['kafka_instance']) +":9021"
	instance_elastic = "http://"+ str(infra_data['elasticsearch_instance'][0]) +":9202"
	instance_kibi = "http://"+ str(infra_data['kibi_instance']) +":5601"
	print_art('Deployment Successful!')
	print('SciPi>> Access Kafka: ' + instance_kafka)
	print('SciPi>> Access ElasticSearch: ' + instance_elastic)
	print('SciPi>> Access Kibi:' + instance_kibi)

def print_deployment_starting():
	print_art('Starting Deployment...')

if sys.argv[1] == 'DEPLOYMENT_FINISHED':
	infra_data = helper.get_cloud_info('infra_values.json')
	instance_kafka = "http://" + str(infra_data['kafka_instance']) +":9021"
	instance_elastic = "http://"+ str(infra_data['elasticsearch_instance'][0]) +":9202"
	instance_kibi = "http://"+ str(infra_data['kibi_instance']) +":5601"
	print('Scipi>> Checking that all applications are up...')
	# Check if Each Service is up
	helper.check_if_up('Kafka', instance_kafka, 20, 15)
	helper.check_if_up('Elasticsearch', instance_elastic, 20, 15)
	helper.check_if_up('Kibi', instance_kibi, 20, 15)
	print_deployment_success(infra_data)
elif sys.argv[1] == 'DEPLOYMENT_STARTING':
	print_deployment_starting()
elif sys.argv[1] == 'DEPLOYMENT_DESTROY_START':
	print_art('Starting destroy...')
elif sys.argv[1] == 'DEPLOYMENT_DESTROY_END':
	print_art('Finished Destroy...Goodbye!')