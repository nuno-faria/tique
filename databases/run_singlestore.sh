#!/bin/bash

sdb-admin start-node --all -y
sdb-admin optimize -y
sudo memsqlctl update-config --set-global --key maximum_memory --value `python3 -c "m = dict((i.split()[0].rstrip(':'),int(i.split()[1])) for i in open('/proc/meminfo').readlines()); print((m['MemTotal'] + m['SwapTotal'])//1024 - 1024)"` --all -y
singlestore -proot -e "CREATE DATABASE tpcc"
echo "Singlestore started."
