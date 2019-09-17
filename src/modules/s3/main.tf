resource "aws_s3_bucket" "create_bucket" {
  bucket = "${var.name}"
  acl    = "private"

  tags = {
    Name        = "Bucket for EMR Bootstrap actions/Steps"
    Environment = "Scripts"
  }
}

resource "aws_s3_bucket_object" "bootstrap_action_file_ingest" {
  bucket     = "${var.name}"
  key        = "provisioners/emr_bootstrap_actions_ingest.sh"
  source     = "provisioners/emr_bootstrap_actions.sh"
  depends_on = ["aws_s3_bucket.create_bucket"]
}

resource "aws_s3_bucket_object" "bootstrap_action_file" {
  bucket     = "${var.name}"
  key        = "provisioners/emr_bootstrap_actions.sh"
  source     = "provisioners/emr_bootstrap_actions.sh"
  depends_on = ["aws_s3_bucket.create_bucket"]
}

resource "aws_s3_bucket_object" "pyspark_quick_setup_file" {
  bucket     = "${var.name}"
  key        = "provisioners/emr_pyspark_setup.sh"
  source     = "provisioners/emr_pyspark_setup.sh"
  depends_on = ["aws_s3_bucket.create_bucket"]
}

resource "aws_s3_bucket_object" "pyspark_quick_setup_file_ingest" {
  bucket     = "${var.name}"
  key        = "provisioners/emr_pyspark_setup_ingest.sh"
  source     = "provisioners/emr_pyspark_setup.sh"
  depends_on = ["aws_s3_bucket.create_bucket"]
}

/*
resource "aws_s3_bucket_object" "scipi_spark_jobs" {
  bucket     = "${var.name}"
  key        = "spark-jobs/spark-jobs.zip"
  source     = "spark-jobs/spark-jobs.zip"
  depends_on = ["aws_s3_bucket.create_bucket"]
}
*/

