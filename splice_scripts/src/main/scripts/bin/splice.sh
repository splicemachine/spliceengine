#!/bin/bash

ROOT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )/.." && pwd )"
# sets VM specific env vars
source "${ROOT_DIR}"/setEnv

usage() {
    # $1 is an error, if any
    if [ -n "$1" ]
        then
        echo "Error: $1"
    fi
    echo "Usage: $0 {-readme | -config | -state | -clean | -sqlshell | -restart | -h[elp]}"
    echo "Where: "
    echo "  -readme => display platform-specific doc"
    echo "  -config => configure the system with Splice Coprocessors"
    echo "  -state => check the service state"
    echo "  -clean => clean the splice DB"
    echo "  -sqlshell => start the splice SQL shell"
    echo "  -restart => restart the splice machine"
    echo "  -h => print this message"
}

case $1 in
    `-h | -help | -\?)
        usage
        exit 0 # This is not an error, User asked help. Don't do "exit 1"
    ;;
    -readme)
    # display platform-specific geting started doc
        "${ROOT_DIR}"/readme
    ;;
    -config)
    # call configurator
        "${ROOT_DIR}"/config.sh
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
    -restart)
    # restart.sh hbase
        "${ROOT_DIR}"/restart.sh
    ;;
    *)
        usage "Unknown option (ignored): $1"
        exit 1
    ;;
esac
