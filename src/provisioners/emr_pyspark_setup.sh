#!/usr/bin/env bash

# bind conda to spark
echo -e "\nexport PYSPARK_PYTHON=/home/hadoop/conda/bin/python" >> /etc/spark/conf/spark-env.sh
echo "export PYSPARK_DRIVER_PYTHON=/home/hadoop/conda/bin/python" >> /etc/spark/conf/spark-env.sh
echo "export PYSPARK_DRIVER_PYTHON_OPTS='notebook --no-browser --port 8888 --ip=\"0.0.0.0\"'" >> /etc/spark/conf/spark-env.sh
# Download Dependencies
sudo wget http://central.maven.org/maven2/org/elasticsearch/elasticsearch-hadoop/6.7.2/elasticsearch-hadoop-6.7.2.jar -P /usr/lib/spark/jars/
sudo wget https://dl.bintray.com/spark-packages/maven/graphframes/graphframes/0.7.0-spark2.3-s_2.11/graphframes-0.7.0-spark2.3-s_2.11.jar -P /usr/lib/spark/jars/
sudo wget http://central.maven.org/maven2/org/apache/spark/spark-streaming-kafka-0-8-assembly_2.11/2.4.3/spark-streaming-kafka-0-8-assembly_2.11-2.4.3.jar -P /usr/lib/spark/jars/
sudo wget http://central.maven.org/maven2/org/apache/spark/spark-network-common_2.11/2.3.3/spark-network-common_2.11-2.3.3.jar -P /usr/lib/spark/jars/
sudo wget http://central.maven.org/maven2/com/typesafe/scala-logging/scala-logging_2.11/3.5.0/scala-logging_2.11-3.5.0.jar -P /usr/lib/spark/jars/
sudo wget http://central.maven.org/maven2/com/typesafe/scala-logging/scala-logging-slf4j_2.11/2.1.2/scala-logging-slf4j_2.11-2.1.2.jar -P /usr/lib/spark/jars/