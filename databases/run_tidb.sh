#!/bin/bash

# start cluster
~/.tiup/bin/tiup playground v6.5.0 -T cluster --without-monitor &
echo $! > tidb.pid

# create tpcc database
out=$(mysql --host 127.0.0.1 --port 4000 -u root -e "CREATE DATABASE tpcc" 2>&1)
while [[ $out == "ERROR 2003 (HY000): Can't connect to MySQL"* ]]; do
    sleep 3
    out=$(mysql --host 127.0.0.1 --port 4000 -u root -e "CREATE DATABASE tpcc" 2>&1)
done

sleep 3

echo "TiDB started."
