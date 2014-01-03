#!/bin/bash
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )/" && pwd )"
pushd "${SCRIPT_DIR}/structured_derby" &>/dev/null
ROOT_DIR="$( pwd )"

SPLICELOG="${ROOT_DIR}"/splice.log
ZOOLOG="${ROOT_DIR}"/zoo.log
DERBYLOG="${ROOT_DIR}"/derby.log

currentDateTime=$(date +'%m-%d-%Y:%H:%M:%S')

echo "Shutting down splice at $currentDateTime" >> ${SPLICELOG}

"${ROOT_DIR}"/target/classes/bin/_stopServer.sh "${ROOT_DIR}/target/classes" "${ROOT_DIR}/target/classes"

if [ ! -d "logs" ]; then
  mkdir logs
fi
currentDateTime=$(date +'%m-%d-%Y-%H_%M_%S')
cp ${SPLICELOG} logs/${currentDateTime}.$( basename "${SPLICELOG}")
cp ${ZOOLOG} logs/${currentDateTime}.$( basename "${ZOOLOG}")
cp ${DERBYLOG} logs/${currentDateTime}.$( basename "${DERBYLOG}")

popd &>/dev/null
