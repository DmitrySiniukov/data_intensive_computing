## Install elasticsearch
wget -qO - https://packages.elastic.co/GPG-KEY-elasticsearch | sudo apt-key add -
echo "deb https://artifacts.elastic.co/packages/5.x/apt stable main" | sudo tee -a /etc/apt/sources.list.d/elasticsearch-5.x.list
sudo apt-get update && sudo apt-get install elasticsearch

### Start 
sudo service elasticsearch start

## Install kibana
sudo apt-get install kibana

### Start
sudo service kibana start


# Delete all index in elasticsearch
curl -X DELETE 'http://localhost:9200/_all'
