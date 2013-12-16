#!/bin/bash

# MapR VM Splice Machine configurator
ROOT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )/.." && pwd )"
# sets VM specific env vars
source "${ROOT_DIR}"/setEnv

echo "Configuring Splice Machine on $SPLICE_ENV"

# Add splice coprocessors
