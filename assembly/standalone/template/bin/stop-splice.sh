#!/bin/bash
echo "Stopping the Splice Machine database..."

BASE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )/.." && pwd )"
source ${BASE_DIR}/bin/functions.sh

_kill_em_all 9

echo "done."
exit 0

