#!/bin/sh

ROOT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )/.." && pwd )"
# sets VM specific env vars
source "${ROOT_DIR}"/setEnv


/bin/more "${ROOT_DIR}"/readme.txt
