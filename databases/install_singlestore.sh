#!/bin/bash

if [ $# -eq 0 ]; then
    echo "Missing license key: Usage: ./run_singlestore.sh <LICENSE_KEY>"
    exit 1
fi

sed -i "s/LICENSE/$1/" singlestore.yaml

# singlestore
sudo apt update
sudo apt install -y wget
wget -O - 'https://release.memsql.com/release-aug2018.gpg'  2>/dev/null | sudo apt-key add - && apt-key list
echo "deb [arch=amd64] https://release.memsql.com/production/debian memsql main" | sudo tee /etc/apt/sources.list.d/memsql.list
sudo apt update && sudo apt -y install singlestoredb-toolbox singlestore-client singlestoredb-studio
sdb-deploy setup-cluster --cluster-file singlestore.yaml -y

# obdc driver
sudo apt install libsecret-1-0 -y
wget https://github.com/memsql/singlestore-odbc-connector/releases/download/v1.1.1/singlestore-connector-odbc-1.1.1-debian10-amd64.tar.gz
tar -xzvf singlestore-connector-odbc-1.1.1-debian10-amd64.tar.gz
cd singlestore-connector-odbc-1.1.1-debian10-amd64
sudo cp libssodbcw.so /usr/lib/x86_64-linux-gnu/libssodbcw.so
cd ..
