#!/bin/bash

# Clean the Splice Machine database
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )/.." && pwd )"

/bin/rm -rf ./db
