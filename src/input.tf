/*
Variables for provisioning
*/

// Project Name
variable "project" {
  default = "scipi"
}

// AWS Access Key
variable "aws_access_key" {
  default = ""
}

// AWS Secret Key
variable "aws_secret_key" {
  default = ""
}

// Target Region
variable "aws_region" {
  default = "us-east-2"
}

// VPC to deploy resources to
variable "target_vpc" {
  default = "vpc-b9ae85d0"
}

// Subnet to deploy resources to
variable "target_subnet" {
  default = "subnet-281f9e65"
}

// Kafka Input Variables
variable "kafka_instance_type" {
  default = "t2.large"
}

variable "kafka_cluster_size" {
  default = "2"
}

// Elastic Search Input Variables

variable "es_instance_type" {
  default = "t2.large"
}

variable "es_instance_count" {
  default = 3
}

variable "es_ebs_size" {
  default = 10
}

// Kibi Input Variables
variable "kibi_instance_type" {
  default = "t2.xlarge"
}

// Spark Amazon EMR Input Variables - General
variable "emr_release_label" {
  # default = "emr-5.16.0"
  default = "emr-5.19.0"
}

variable "emr_applications" {
  default = ["Hadoop", "Spark"]
}

// Spark Amazon EMR Input Variables - Master
variable "emr_master_instance_type" {
  default = "m4.xlarge"
}

variable "emr_master_ebs_size" {
  default = "20"
}

// Spark Amazon EMR Input Variables - Slave
variable "emr_slave_core_instance_type" {
  default = "m4.xlarge"
}

variable "emr_slave_core_instance_count" {
  default = 3
}

variable "emr_slave_core_ebs_size" {
  default = "50"
}

// emr_ingest_values
variable "emr_ingest_master_instance_type" {
  default = "m4.xlarge"
}

variable "emr_ingest_master_ebs_size" {
  default = "20"
}

// Spark Amazon EMR Input Variables - Slave
variable "emr_ingest_slave_core_instance_type" {
  default = "m4.xlarge"
}

variable "emr_ingest_slave_core_instance_count" {
  default = 1
}

variable "emr_ingest_slave_core_ebs_size" {
  default = "50"
}
