#!/usr/bin/env bash
sudo apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv EA312927;
echo "deb http://repo.mongodb.org/apt/ubuntu xenial/mongodb-org/3.2 multiverse" | sudo tee /etc/apt/sources.list.d/mongodb-org-3.2.list;
sudo apt-get update;
sudo apt-get install -y mongodb-org;
sudo systemctl start mongod;
sudo systemctl status mongod;
sudo ufw allow from your_other_server_ip/32 to any port 27017;
sudo ufw status;