FROM ubuntu:18.04

WORKDIR /scipi

COPY . /scipi

RUN apt-get update && apt-get install -y \
	openssh-server \
	wget \
	unzip \
	python3.6 \
	python3-pip \
	python-pip \
	python \
	&& rm -rf /var/lib/apt/lists/*

RUN wget --quiet https://releases.hashicorp.com/terraform/0.11.13/terraform_0.11.13_linux_amd64.zip \
	&& unzip terraform_0.11.13_linux_amd64.zip \
	&& mv terraform /usr/bin 

RUN pip install confluent_kafka elasticsearch paramiko scp boto3


