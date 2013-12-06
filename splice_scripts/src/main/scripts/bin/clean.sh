#!/bin/bash

# Splice Machine DB cleaner

ROOT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )/.." && pwd )"
# sets VM specific env vars
source "${ROOT_DIR}"/setEnv

echo "Cleaning Splice Machine $SPLICE_ENV DB"

"${ZOOKEEPER_HOME}"/bin/zkCli.sh <<flattenZoo
ls /
rmr /startupPath
ls /
flattenZoo

