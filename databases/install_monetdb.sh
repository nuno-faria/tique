#!/bin/sh

sudo apt update
sudo apt install -y unzip
unzip MonetDB.zip
sudo apt-get install -y unixodbc unixodbc-dev bison cmake gcc libssl-dev pkg-config python3 libbz2-dev uuid-dev libpcre3-dev libreadline-dev liblzma-dev zlib1g-dev

cmake MonetDB -DCMAKE_BUILD_TYPE=Release
cmake --build . -j
sudo cmake --build . --target install -j
sudo cp clients/odbc/driver/libMonetODBC.so /usr/lib/x86_64-linux-gnu/libMonetODBC.so
