#!/bin/bash

# Load demo data into Splice Machine
ROOT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )/.." && pwd )"

"${ROOT_DIR}"/bin/sqlshell.sh ./demodata/sql/loadall.sql

