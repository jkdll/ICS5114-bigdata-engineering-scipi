echo "#Starting!" 
 sudo echo 'export SIR_ES_HOSTS="172.31.3.48:9330,172.31.14.57:9330,172.31.3.170"' >> ~/.bash_profile 
 sudo echo 'export SIR_ES_IP=172.31.3.170' >> ~/.bash_profile 
 sudo echo 'export SIR_ES_PORT=9202' >> ~/.bash_profile 
 sudo echo 'export NODE_ISMASTER=false' >> ~/.bash_profile 
 sudo echo 'export NODE_NAME=node-3' >> ~/.bash_profile 
 sudo echo 'export NODE_ISDATA=true' >> ~/.bash_profile 
 sudo apt update && sudo apt install -y openjdk-8-jdk-headless && sudo apt-get install -y unzip 
 unzip es-node-code.zip 
 sudo sysctl -w vm.max_map_count=262144 
 