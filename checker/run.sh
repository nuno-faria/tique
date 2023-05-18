#!/bin/bash

lein run generator 10000 input.json
python3 executor.py input.json
lein run checker history.edn
