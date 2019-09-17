sudo apt-get --assume-yes install python3
sudo apt-get --assume-yes install pip
sudo apt-get --assume-yes install awscli
sudo touch /opt/download-files.sh
sudo chmod 777 /opt/download-files.sh
sudo mkdir /opt/zips/
sudo echo "aws s3api create-bucket --bucket bigdata-oag-zips --region us-east-1" >> /opt/download-files.sh
sudo echo -e "\n" >> /opt/download-files.sh
sudo echo "cd /opt/zips" >> /opt/download-files.sh
sudo echo -e "\n" >> /opt/download-files.sh
sudo echo "wget https://academicgraphv2.blob.core.windows.net/oag/linkage/venue_linking_pairs.zip" >> /opt/download-files.sh
sudo echo -e  "\n" >> /opt/download-files.sh
sudo echo "aws s3 mv * s3://bigdata-oag-zips/" >> /opt/download-files.sh
sudo echo -e  "\n" >> /opt/download-files.sh
sudo echo "wget https://academicgraphv2.blob.core.windows.net/oag/linkage/paper_linking_pairs.zip" >> /opt/download-files.sh
sudo echo -e  "\n" >> /opt/download-files.sh
sudo echo "aws s3 mv * s3://bigdata-oag-zips/" >> /opt/download-files.sh
sudo echo -e  "\n" >> /opt/download-files.sh
sudo echo "wget https://academicgraphv2.blob.core.windows.net/oag/linkage/author_linking_pairs.zip" >> /opt/download-files.sh
sudo echo -e  "\n" >> /opt/download-files.sh
sudo echo "aws s3 mv * s3://bigdata-oag-zips/" >> /opt/download-files.sh
sudo echo -e  "\n" >> /opt/download-files.sh
sudo echo "wget https://academicgraphv2.blob.core.windows.net/oag/aminer/venue/aminer_venues.zip" >> /opt/download-files.sh
sudo echo -e  "\n" >> /opt/download-files.sh
sudo echo "aws s3 mv * s3://bigdata-oag-zips/" >> /opt/download-files.sh
sudo echo -e  "\n" >> /opt/download-files.sh
sudo echo "wget https://academicgraphv2.blob.core.windows.net/oag/mag/venue/mag_venues.zip" >> /opt/download-files.sh
sudo echo -e  "\n" >> /opt/download-files.sh
sudo echo "aws s3 mv * s3://bigdata-oag-zips/" >> /opt/download-files.sh
sudo echo -e  "\n" >> /opt/download-files.sh
sudo echo "wget https://academicgraphv2.blob.core.windows.net/oag/mag/paper/mag_papers_0.zip" >> /opt/download-files.sh
sudo echo -e  "\n" >> /opt/download-files.sh
sudo echo "aws s3 mv * s3://bigdata-oag-zips/" >> /opt/download-files.sh
sudo echo -e  "\n" >> /opt/download-files.sh
sudo echo "wget https://academicgraphv2.blob.core.windows.net/oag/mag/paper/mag_papers_1.zip" >> /opt/download-files.sh
sudo echo -e  "\n" >> /opt/download-files.sh
sudo echo "aws s3 mv * s3://bigdata-oag-zips/" >> /opt/download-files.sh
sudo echo -e  "\n" >> /opt/download-files.sh
sudo echo "wget https://academicgraphv2.blob.core.windows.net/oag/mag/paper/mag_papers_2.zip" >> /opt/download-files.sh
sudo echo -e  "\n" >> /opt/download-files.sh
sudo echo "aws s3 mv * s3://bigdata-oag-zips/" >> /opt/download-files.sh
sudo echo -e  "\n" >> /opt/download-files.sh
sudo echo "wget https://academicgraphv2.blob.core.windows.net/oag/mag/author/mag_authors_0.zip" >> /opt/download-files.sh
sudo echo -e  "\n" >> /opt/download-files.sh
sudo echo "aws s3 mv * s3://bigdata-oag-zips/" >> /opt/download-files.sh
sudo echo -e  "\n" >> /opt/download-files.sh
sudo echo "wget https://academicgraphv2.blob.core.windows.net/oag/mag/author/mag_authors_1.zip" >> /opt/download-files.sh
sudo echo -e  "\n" >> /opt/download-files.sh
sudo echo "aws s3 mv * s3://bigdata-oag-zips/" >> /opt/download-files.sh
sudo echo -e  "\n" >> /opt/download-files.sh
sudo echo "wget https://academicgraphv2.blob.core.windows.net/oag/mag/author/mag_authors_2.zip" >> /opt/download-files.sh
sudo echo -e  "\n" >> /opt/download-files.sh
sudo echo "aws s3 mv * s3://bigdata-oag-zips/" >> /opt/download-files.sh
sudo echo -e  "\n" >> /opt/download-files.sh
sudo echo "wget https://academicgraphv2.blob.core.windows.net/oag/aminer/paper/aminer_papers_0.zip" >> /opt/download-files.sh
sudo echo -e  "\n" >> /opt/download-files.sh
sudo echo "aws s3 mv * s3://bigdata-oag-zips/" >> /opt/download-files.sh
sudo echo -e  "\n" >> /opt/download-files.sh
sudo echo "wget https://academicgraphv2.blob.core.windows.net/oag/aminer/paper/aminer_papers_1.zip" >> /opt/download-files.sh
sudo echo -e  "\n" >> /opt/download-files.sh
sudo echo "aws s3 mv * s3://bigdata-oag-zips/" >> /opt/download-files.sh
sudo echo -e  "\n" >> /opt/download-files.sh
sudo echo "wget https://academicgraphv2.blob.core.windows.net/oag/aminer/paper/aminer_papers_2.zip" >> /opt/download-files.sh
sudo echo -e  "\n" >> /opt/download-files.sh
sudo echo "aws s3 mv * s3://bigdata-oag-zips/" >> /opt/download-files.sh
sudo echo -e  "\n" >> /opt/download-files.sh
sudo echo "wget https://academicgraphv2.blob.core.windows.net/oag/aminer/paper/aminer_papers_3.zip" >> /opt/download-files.sh
sudo echo -e  "\n" >> /opt/download-files.sh
sudo echo "aws s3 mv * s3://bigdata-oag-zips/" >> /opt/download-files.sh
sudo echo -e  "\n" >> /opt/download-files.sh
sudo echo "wget https://academicgraphv2.blob.core.windows.net/oag/aminer/author/aminer_authors_0.zip" >> /opt/download-files.sh
sudo echo -e  "\n" >> /opt/download-files.sh
sudo echo "aws s3 mv * s3://bigdata-oag-zips/" >> /opt/download-files.sh
sudo echo -e  "\n" >> /opt/download-files.sh
sudo echo "wget https://academicgraphv2.blob.core.windows.net/oag/aminer/author/aminer_authors_1.zip" >> /opt/download-files.sh
sudo echo -e  "\n" >> /opt/download-files.sh
sudo echo "aws s3 mv * s3://bigdata-oag-zips/" >> /opt/download-files.sh
sudo echo -e  "\n" >> /opt/download-files.sh
sudo echo "wget https://academicgraphv2.blob.core.windows.net/oag/aminer/author/aminer_authors_2.zip" >> /opt/download-files.sh
sudo echo -e  "\n" >> /opt/download-files.sh
sudo echo "aws s3 mv * s3://bigdata-oag-zips/" >> /opt/download-files.sh
sudo echo -e  "\n" >> /opt/download-files.sh
sudo echo "wget https://academicgraphv2.blob.core.windows.net/oag/aminer/author/aminer_authors_3.zip" >> /opt/download-files.sh
sudo echo -e  "\n" >> /opt/download-files.sh
sudo echo "aws s3 mv * s3://bigdata-oag-zips/" >> /opt/download-files.sh

sudo touch ~/.aws/config
sudo echo "[default]" >> ~/.aws/config
sudo echo -e  "\n" >> ~/.aws/config
sudo echo "[default]" >> ~/.aws/config
sudo echo -e  "\n" >> ~/.aws/config
sudo echo "[default]" >> ~/.aws/config
sudo echo -e  "\n" >> ~/.aws/config

sudo touch ~/.aws/credentials
sudo echo "[default]" >> ~/.aws/credentials
sudo echo -e  "\n" >> ~/.aws/credentials
sudo echo "aws_access_key_id = " >> ~/.aws/credentials
sudo echo -e  "\n" >> ~/.aws/credentials
sudo echo "aws_secret_access_key = " >> ~/.aws/credentials
sudo echo -e  "\n" >> ~/.aws/credentials
