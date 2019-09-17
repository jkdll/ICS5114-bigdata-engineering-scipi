/*
Script Name: 
  main.tf
Description:
  This is the terraform script which provisions or the required AWS resources.
*/

/*
*******************************************************************************
* Section 1 - Cloud Intrastructure Setup
************ This section contains generic resources to set up our infra:
************ 1) AWS Provider to store credentials
************ 2) SSH Key to log onto Machines
************ 3) Get Ubuntu 18.04 Image from AWS AMI repo. We will use this on
************    our machines
*******************************************************************************
*/
// 1) Credentials
provider "aws" {
  // Use us-east-2 as region
  region     = "${var.aws_region}"
  access_key = "${var.aws_access_key}"
  secret_key = "${var.aws_secret_key}"
}

// 2) Access Key
resource "aws_key_pair" "deployer" {
  key_name   = "scipi-deployer-key"
  public_key = "${file("scipi_ssh_key.pub")}"
}

// 3) Ubuntu AMI Image
data "aws_ami" "ubuntu" {
  most_recent = true

  filter {
    name   = "name"
    values = ["ubuntu/images/hvm-ssd/ubuntu-bionic-18.04-amd64-server-*"]
  }

  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }

  owners = ["099720109477"] # Canonical
}

/*
*******************************************************************************
* Section 2 - Cloud Resources 
************ This section creates AWS resources for our setup.
************ 1) Controller AWS Instance to orchestrate work.
************ 2) Kafka EC2 Instance for Data Ingestion.
************ 3) Elasticsearch Cluster for Data Sotrage.
************ 4) Kibi EC2 Instance for dashboarding.
************ 5) Spark EMR Cluster
*******************************************************************************
*/
// 1) Controller Instance
resource "aws_instance" "controller_instance" {
  ami           = "${data.aws_ami.ubuntu.id}"
  instance_type = "t2.micro"
  key_name      = "scipi-deployer-key"

  tags = {
    Name = "Controller Instance"
  }

  depends_on = ["aws_key_pair.deployer"]
}

// 2) Kafka Instance
resource "aws_instance" "kafka_instance" {
  ami           = "${data.aws_ami.ubuntu.id}"
  instance_type = "${var.kafka_instance_type}"
  key_name      = "scipi-deployer-key"

  tags = {
    Name = "Kafka Instance"
  }

  depends_on = ["aws_key_pair.deployer"]
}

// 3) ES Cluster
resource "aws_instance" "elasticsearch_instance" {
  count         = "${var.es_instance_count}"
  ami           = "${data.aws_ami.ubuntu.id}"
  instance_type = "${var.es_instance_type}"
  key_name      = "scipi-deployer-key"

  tags = {
    Name = "ES Instance-${count.index + 1}"
  }

  depends_on = ["aws_key_pair.deployer"]
}

// 4) Kibi Machine
resource "aws_instance" "kibi_instance" {
  ami           = "${data.aws_ami.ubuntu.id}"
  instance_type = "${var.kibi_instance_type}"
  key_name      = "scipi-deployer-key"

  tags = {
    Name = "Kibi Instance"
  }

  depends_on = ["aws_key_pair.deployer"]
}

// 5) Apache Spark Amazon EMR Cluster for batch
// S3 to hold logs
module "s3" {
  source = "./modules/s3"
  name   = "${var.project}"
}

// IAM role to run services
module "iam" {
  source = "./modules/iam"
}

// Security Groups for Cluster
module "security" {
  source              = "./modules/security"
  name                = "${var.project}"
  vpc_id              = "${var.target_vpc}"
  ingress_cidr_blocks = "0.0.0.0/0"
}

// EMR Cluster
module "emr" {
  source                    = "./modules/emr"
  name                      = "${var.project}"
  s3_name                   = "${var.project}"
  release_label             = "${var.emr_release_label}"
  applications              = "${var.emr_applications}"
  subnet_id                 = "${var.target_subnet}"
  key_name                  = "scipi-deployer-key"
  bootstrap_action_name     = "emr_bootstrap_actions.sh"
  emr_pyspark_setup         = "emr_pyspark_setup.sh"
  master_instance_type      = "${var.emr_master_instance_type}"
  master_ebs_size           = "${var.emr_master_ebs_size}"
  core_instance_type        = "${var.emr_slave_core_instance_type}"
  core_instance_count       = "${var.emr_slave_core_instance_count}"
  core_ebs_size             = "${var.emr_slave_core_ebs_size}"
  emr_master_security_group = "${module.security.emr_master_security_group}"
  emr_slave_security_group  = "${module.security.emr_slave_security_group}"
  emr_ec2_instance_profile  = "${module.iam.emr_ec2_instance_profile}"
  emr_service_role          = "${module.iam.emr_service_role}"
  emr_autoscaling_role      = "${module.iam.emr_autoscaling_role}"
}

// EMR Cluster
module "emr_ingest" {
  source                    = "./modules/emr"
  name                      = "${var.project}-ingest"
  s3_name                   = "${var.project}"
  release_label             = "${var.emr_release_label}"
  applications              = "${var.emr_applications}"
  subnet_id                 = "${var.target_subnet}"
  key_name                  = "scipi-deployer-key"
  bootstrap_action_name     = "emr_bootstrap_actions_ingest.sh"
  emr_pyspark_setup         = "emr_pyspark_setup_ingest.sh"
  master_instance_type      = "${var.emr_ingest_master_instance_type}"
  master_ebs_size           = "${var.emr_ingest_master_ebs_size}"
  core_instance_type        = "${var.emr_ingest_slave_core_instance_type}"
  core_instance_count       = "${var.emr_ingest_slave_core_instance_count}"
  core_ebs_size             = "${var.emr_ingest_slave_core_ebs_size}"
  emr_master_security_group = "${module.security.emr_master_security_group}"
  emr_slave_security_group  = "${module.security.emr_slave_security_group}"
  emr_ec2_instance_profile  = "${module.iam.emr_ec2_instance_profile}"
  emr_service_role          = "${module.iam.emr_service_role}"
  emr_autoscaling_role      = "${module.iam.emr_autoscaling_role}"
}
