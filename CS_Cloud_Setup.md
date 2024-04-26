Elasticsearch 
wget -qO - https://artifacts.elastic.co/GPG-KEY-elasticsearch | sudo gpg --dearmor -o /usr/share/keyrings/elasticsearch-keyring.gpg
echo "deb [signed-by=/usr/share/keyrings/elasticsearch-keyring.gpg] https://artifacts.elastic.co/packages/8.x/apt stable main" | sudo tee /etc/apt/sources.list.d/elastic-8.x.list

sudo apt-get update && sudo apt-get install elasticsearch=8.9.0

Change ip address in /etc/elasticsearch/elasticsearch.yml for key network.host 

sudo systemctl daemon-reload

sudo systemctl enable elasticsearch.service

sudo systemctl start elasticsearch.service

Generate token for enrollment to cluster /usr/share/elasticsearch/bin/elasticsearch-create-enrollment-token -s node

Join a cluster
/usr/share/elasticsearch/bin/elasticsearch-reconfigure-node --enrollment-token eyJ2ZXIiOiI4LjkuMCIsImFkciI6WyIxNzIuMjYuMTcwLjI1Mzo5MjAwIl0sImZnciI6IjhkYTZlMzc2NjJkYjAwZDBhZjVlYjE2ZmM3MmZlMzE3YjFjNDc5MmMyNWVjMTY2NDhjOGJhMzY3MWY3YmJlMjgiLCJrZXkiOiJKNk5MV280QkNpajJqTy1ka25aTDpFMmZwNWtzM1QyLWtjN2I5QUlQdGlRIn0=


Once all the nodes are joined. login to each node one-by-one and add discovery.seed_hosts property in elasticsearch.yml. The property would be a list of IPs of all the nodes in the cluster. After adding the new property restart elasticsearch and proceed with the same steps on the remaining nodes. 


Kibana 

wget -qO - https://artifacts.elastic.co/GPG-KEY-elasticsearch | sudo gpg --dearmor -o /usr/share/keyrings/elasticsearch-keyring.gpg

echo "deb [signed-by=/usr/share/keyrings/elasticsearch-keyring.gpg] https://artifacts.elastic.co/packages/8.x/apt stable main" | sudo tee /etc/apt/sources.list.d/elastic-8.x.list

sudo apt-get update && sudo apt-get install kibana=8.9.0


root@kibana-host:/usr/share/kibana/bin# ./kibana-setup -t eyJ2ZXIiOiI4LjkuMCIsImFkciI6WyIxNzIuMjYuMTcwLjI1Mzo5MjAwIl0sImZnciI6IjhkYTZlMzc2NjJkYjAwZDBhZjVlYjE2ZmM3MmZlMzE3YjFjNDc5MmMyNWVjMTY2NDhjOGJhMzY3MWY3YmJlMjgiLCJrZXkiOiJLNk54V280QkNpajJqTy1kMEhaZDpUTDhkd2FUVFM5eWcyUHZnMWNjYnlnIn0=



Kibana dev env setup 

sudo apt-get install gh
gh auth login
sudo apt-get install virtualenv
gh repo clone ashyo98/BMCP
virtualenv -p python3 env
source env/bin/activate
pip install -r final_pipeline/requirements.txt
