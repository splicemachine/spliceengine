#!/bin/bash

ROOT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )/.." && pwd )"

# shut down both zookeeper and splice/hbase
${ROOT_DIR}/bin/_stopServer.sh "${ROOT_DIR}" "${ROOT_DIR}"

# Check for stragglers
SIG=15
S=`jps | grep SpliceTestPlatform | grep -v grep  | awk '{print $1}'`
[[ -n ${S} ]] && echo "Found SpliceSinglePlatform straggler. Killing." && for pid in ${S}; do kill -${SIG} `echo ${pid}`; done
Z=`jps | grep ZooKeeperServerMain | grep -v grep  | awk '{print $1}'`
[[ -n ${Z} ]] && echo "Found ZooKeeperServerMain straggler. Killing." && for pid in ${Z}; do kill -${SIG} `echo ${pid}`; done
