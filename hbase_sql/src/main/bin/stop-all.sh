#!/bin/bash

ROOT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )/.." && pwd )"
${ROOT_DIR}/bin/stop-splice-admin.sh && ${ROOT_DIR}/bin/stop-splice.sh
