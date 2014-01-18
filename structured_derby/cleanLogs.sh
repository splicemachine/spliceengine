#!/bin/sh

#
# Remove old server, derby and zoo log files from the logs directory that are older than 3 weeks
#
ROOT_DIR="${WORKSPACE}"
if [[ -z ${ROOT_DIR} ]]; then
  ROOT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
else
  ROOT_DIR="${ROOT_DIR}"/structured_derby
fi
# Test
#find "${ROOT_DIR}"/logs -type f -mtime +7 -exec ls -lt {} \;
find "${ROOT_DIR}"/logs -type f -mtime +14 -exec rm -f {} \;
