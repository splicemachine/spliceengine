#!/bin/bash

ROOT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )/.." && pwd )"
# sets VM specific env vars
source "${ROOT_DIR}"/setEnv

usage() {
    # $1 is an error, if any
    if [ ! -z "$1" ] 
        then
        echo "Error: $1"
    fi
    echo "Usage: $0 {-readme | -state | -clean | -sqlshell}"
    echo "Where: "
    echo "  -readme => display platform-specific doc"
    echo "  -state => check the service state"
    echo "  -clean => clean the splice DB"
    echo "  -sqlshell => start the splice SQL shell"
}

case $1 in
 -h | -help | -\?)
     usage
     exit 0      # This is not an error, User asked help. Don't do "exit 1"
     ;;
 -readme)
     # display platform-specific geting started doc
     "${ROOT_DIR}"/readme
     ;;
 -state)
     # call platform-specific service checker
     "${ROOT_DIR}"/serviceState.sh
     ;;
 -clean)
     # call DB cleaner
     "${ROOT_DIR}"/clean.sh
     ;;
 -sqlshell)
     # call splice shell
     "${ROOT_DIR}"/sqlshell.sh
     ;;
 *)
     usage "Unknown option (ignored): $1"
     exit 1
     ;;
esac
