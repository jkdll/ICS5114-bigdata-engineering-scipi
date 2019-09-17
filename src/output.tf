/*
Output Values from Terraform 
*/

// AWS Access Key
output "output_aws_access_key" {
  value = "${var.aws_access_key}"
}

// AWS Secret Key
output "output_aws_secret_key" {
  value = "${var.aws_secret_key}"
}

// Target Region
output "output_aws_region" {
  value = "${var.aws_region}"
}

// S3 Bucket Containing Spark Data
output "output_spark_s3" {
  value = "${var.project}"
}

output "ssh_key_name" {
  value = "${aws_key_pair.deployer.key_name}"
}

output "ssh_key_public" {
  value = "${aws_key_pair.deployer.public_key}"
}

output "ssh_key_fingerprint" {
  value = "${aws_key_pair.deployer.fingerprint}"
}

output "controller_ip" {
  value = "${aws_instance.controller_instance.public_ip}"
}

output "kafka_instance" {
  value = "${aws_instance.kafka_instance.public_ip}"
}

output "elasticsearch_instance" {
  value = "${aws_instance.elasticsearch_instance.*.public_ip}"
}

output "elasticsearch_instance_priavteip" {
  value = "${aws_instance.elasticsearch_instance.*.private_ip}"
}

output "kibi_instance" {
  value = "${aws_instance.kibi_instance.public_ip}"
}

output "emr_id" {
  value = "${module.emr.id}"
}

output "emr_name" {
  value = "${module.emr.name}"
}

output "emr_master_public_dns" {
  value = "${module.emr.master_public_dns}"
}

// EMR Ingest
output "emr_ingest_id" {
  value = "${module.emr_ingest.id}"
}

output "emr_ingest_name" {
  value = "${module.emr_ingest.name}"
}

output "emr_ingest_master_public_dns" {
  value = "${module.emr_ingest.master_public_dns}"
}
