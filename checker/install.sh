#!/bin/sh

sudo apt update
sudo apt install -y unixodbc unixodbc-dev python3-pip
pip3 install pyodbc
sudo apt install -y openjdk-17-jdk graphviz
sudo apt install -y wget
wget https://raw.githubusercontent.com/technomancy/leiningen/stable/bin/lein
sudo mv lein /usr/bin/
sudo chmod +x /usr/bin/lein
lein
