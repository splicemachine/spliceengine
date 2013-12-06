#!/bin/bash

# MapR VM Splice Machine state of the service

ROOT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )/.." && pwd )"
# sets VM specific env vars
source "${ROOT_DIR}"/setEnv

echo "Checking the state of Splice Machine on $SPLICE_ENV"

