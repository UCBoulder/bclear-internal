# Instructions for setting up the elasticsearch cluster on CS openstack cloud

## CS openstack cloud 

- Get access to "Broadband Measurement Portal" project at https://cloud.cs.colorado.edu/project/. (Check with Jason to get added as an admin to the project.)
- Currently there are 11 VM instances already provisioned. 10 of them run elasticsearch and 1 runs kibana. The kibana instance also has the MBA raw data and the environment setup to run the data pipeline for ingesting the MBA data into elasticsearch. 
- To learn about using CS cloud, register for the course here - https://canvas.colorado.edu/courses/64136/pages/openstack-cloud-quick-start
- For any assistance email to cscihelp@colorado.edu.  
- To be able to connect to this instances, CU VPN needs to be installed. The instructions for this can be found at - https://oit.colorado.edu/services/network-internet-services/vpn 
- Next, create a new ssh-key value pair at: https://cloud.cs.colorado.edu/project/key_pairs



## Steps followed to setup the Elasticsearch cluster

### Elasticsearch 
Install elasticsearch
- `wget -qO - https://artifacts.elastic.co/GPG-KEY-elasticsearch | sudo gpg --dearmor -o /usr/share/keyrings/elasticsearch-keyring.gpg`
- `echo "deb [signed-by=/usr/share/keyrings/elasticsearch-keyring.gpg] https://artifacts.elastic.co/packages/8.x/apt stable main" | sudo tee /etc/apt/sources.list.d/elastic-8.x.list`
- `sudo apt-get update && sudo apt-get install elasticsearch=8.9.0`
- Change ip address in /etc/elasticsearch/elasticsearch.yml for key network.host . The IP address would be the private address of the server starting with 172.* 

setup elasticsearch as a service to start every time the instance starts
- `sudo systemctl daemon-reload`
- `sudo systemctl enable elasticsearch.service`
- `sudo systemctl start elasticsearch.service`

Generate token for enrollment to cluster `/usr/share/elasticsearch/bin/elasticsearch-create-enrollment-token -s node`

Use this token on the new installed elasticsearch node to join a existing cluster. The command for this is: 
`/usr/share/elasticsearch/bin/elasticsearch-reconfigure-node --enrollment-token <enrollment-token>`

Once all the nodes are joined. login to each node one-by-one and add discovery.seed_hosts property in elasticsearch.yml. The property would be a list of IPs of all the nodes in the cluster. After adding the new property restart elasticsearch and proceed with the same steps on the remaining nodes. 
e.g 
```yaml
discovery.seed_hosts: ["172.26.*:9300", "172.26.*:9300", "172.26.*:9300"]
```
write the full IP address.

The complete instructions can be found here - https://www.elastic.co/guide/en/elasticsearch/reference/8.9/deb.html

### Kibana 
install kibana
- `wget -qO - https://artifacts.elastic.co/GPG-KEY-elasticsearch | sudo gpg --dearmor -o /usr/share/keyrings/elasticsearch-keyring.gpg`
- `echo "deb [signed-by=/usr/share/keyrings/elasticsearch-keyring.gpg] https://artifacts.elastic.co/packages/8.x/apt stable main" | sudo tee /etc/apt/sources.list.d/elastic-8.x.list`
- `sudo apt-get update && sudo apt-get install kibana=8.9.0`
- Change ip address in /etc/kibana/kibana.yml for key network.host . The IP address would be the private address of the server starting with 172.* 

setup kibana as a service to start every time the instance starts
- `sudo systemctl daemon-reload`
- `sudo systemctl enable kibana.service`
- `sudo systemctl start kibana.service`

enroll the kibana instance to the elasticsearch cluster 
```sh
root@kibana-host:/usr/share/kibana/bin# ./kibana-setup -t <enrollment-token>
```
The complete instructions can be found here - https://www.elastic.co/guide/en/kibana/8.9/deb.html

### Debugging any issues 
- check the logs in the `/var/log/elasticsearch/elasticsearch.log` or `/var/log/kibana/kibana.log` files. 
- some common elasticsearch APIs to use when cluster routing fails because of out of disk.
```curl
curl -k -u elastic:<passwd> --location --request PUT 'https://172.26.156.241:9200/_cluster/settings?pretty' \
--header 'Content-Type: application/json' \
--data '{
  "persistent": {
    "cluster.routing.allocation.disk.watermark.low": "100%",
    "cluster.routing.allocation.disk.watermark.high": "100%",
    "cluster.routing.allocation.disk.watermark.flood_stage": "100%",
    "cluster.routing.allocation.disk.watermark.flood_stage.frozen": "100%"
  }
}'

curl -k -u elastic:<passwd> --location --request PUT 'https://172.26.156.241:9200/_all/_settings' \
--header 'Content-Type: application/json' \
--data '{
    "index.blocks.read_only_allow_delete": null,
    "index.blocks.read_only": false
}'

curl -k -u elastic:<passwd> -XPUT -H "Content-Type: application/json" https://172.26.156.241:9200/_cluster/settings -d '{ "transient": { "cluster.routing.allocation.disk.threshold_enabled": false } }'

POST /_cluster/reroute?retry_failed=true
```
- command to restart elasticsearch is `sudo service elasticsearch restart`. similarly for kibana it is `sudo service kibana restart`.


### ingest pipeline setup on the kibana instance 

- `sudo apt-get install gh`
- `gh auth login`
- `sudo apt-get install virtualenv`
- `gh repo clone UCBoulder/bclear-internal `
- follow the steps in [README.md](final_pipeline/README.md) 
