#!/bin/bash

# Load demo data into Splice Machine
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )/.." && pwd )"

./bin/sqlshell.sh ./demodata/sql/loadall.sql

