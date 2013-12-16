#!/bin/bash

# MapR VM Splice Machine restarter

ROOT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )/.." && pwd )"
# sets VM specific env vars
source "${ROOT_DIR}"/setEnv

echo "Restarting Splice Machine on $SPLICE_ENV"

