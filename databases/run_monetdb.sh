#!/bin/bash

if [[ $1 = "clean" ]]; then
    rm -r data_folder
    mkdir data_folder
fi

mserver5 --dbpath=data_folder/tpcc --set embedded_c=true --set gdk_nr_threads=32
