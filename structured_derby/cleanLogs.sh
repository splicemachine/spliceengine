#!/bin/sh

#
# Remove old server, derby and zoo log files from the logs directory that are older than 3 weeks
#
ROOT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
# Test
#find "${ROOT_DIR}"/logs -type f -mtime +7 -exec ls -lt {} \;
find "${ROOT_DIR}"/logs -type f -mtime +21 -exec rm -f {} \;
