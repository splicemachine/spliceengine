#!/bin/bash

# Clean the Splice Machine database
ROOT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )/.." && pwd )"

/bin/rm -rf "${ROOT_DIR}"/db
