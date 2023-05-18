#!/bin/sh

sudo apt-get update
sudo apt-get install -y wget ca-certificates
wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | sudo apt-key add -
sudo sh -c 'echo "deb http://apt.postgresql.org/pub/repos/apt/ `lsb_release -cs`-pgdg main" >> /etc/apt/sources.list.d/pgdg.list'
sudo apt-get update
sudo apt-get install -y postgresql-14
sudo -u postgres psql -c "alter user postgres with password 'postgres'"
sudo sed -i '1ihost    all             all             0.0.0.0/0               md5' /etc/postgresql/14/main/pg_hba.conf
sudo sed -i '1ilocal   all             postgres                                md5' /etc/postgresql/14/main/pg_hba.conf
sudo sed -i "s/#listen_addresses = 'localhost'/listen_addresses = '*'/" /etc/postgresql/14/main/postgresql.conf
sudo sed -i "s/max_connections = 100/max_connections = 2100/" /etc/postgresql/14/main/postgresql.conf
sudo sed -i "s/#jit = .*/jit = off/" /etc/postgresql/14/main/postgresql.conf # improves performance for some queries
sudo sed -i "s/shared_buffers = .*/shared_buffers = 8GB/" /etc/postgresql/14/main/postgresql.conf
sudo sed -i "s/max_wal_size = .*/max_wal_size = 10GB/" /etc/postgresql/14/main/postgresql.conf
sudo sed -i "s/#max_worker_processes = .*/max_worker_processes = 32/" /etc/postgresql/14/main/postgresql.conf
sudo systemctl restart postgresql
PGPASSWORD=postgres createdb -U postgres tpcc
sudo apt install -y odbc-postgresql
