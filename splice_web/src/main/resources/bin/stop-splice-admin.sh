#!/bin/bash

ROOT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )/.." && pwd )"
source ${ROOT_DIR}/bin/functions-admin.sh

# Shut down Splice Admin
_stopServer "${ROOT_DIR}" "${ROOT_DIR}"

# Check for stragglers
SIG=15
S=$(ps -ef | awk '/splice_web/ && !/awk/ {print $2}')
if [[ -n ${S} ]]; then
    echo "Found splice_web straggler. Killing."
    for pid in ${S}; do
        kill -${SIG} ${pid}
    done
fi
