#!/bin/bash

# tidb
sudo apt update
sudo apt install curl -y
curl --proto '=https' --tlsv1.2 -sSf https://tiup-mirrors.pingcap.com/install.sh | sh
source ~/.profile

# odbc
wget https://downloads.mysql.com/archives/get/p/23/file/mysql-community-client-plugins_8.0.31-1ubuntu20.04_amd64.deb
sudo dpkg -i mysql-community-client-plugins_8.0.31-1ubuntu20.04_amd64.deb
wget https://downloads.mysql.com/archives/get/p/10/file/mysql-connector-odbc_8.0.31-1ubuntu20.04_amd64.deb
sudo dpkg -i mysql-connector-odbc_8.0.31-1ubuntu20.04_amd64.deb
sudo apt-get -f install -y

# mysql client
sudo apt install mysql-client -y
