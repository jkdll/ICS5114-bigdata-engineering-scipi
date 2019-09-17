python provisioners/initialize_notify.py DEPLOYMENT_DESTROY_START
echo "Finished Saving Kibi"
terraform destroy -auto-approve
python provisioners/initialize_notify.py DEPLOYMENT_DESTROY_END
