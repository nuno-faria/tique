#!/bin/sh

sudo apt update
sudo apt install -y python3 python3-pip unixodbc unixodbc-dev
pip3 install pyodbc
