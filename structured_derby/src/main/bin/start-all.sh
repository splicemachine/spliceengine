#!/bin/bash

ROOT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )/.." && pwd )"
${ROOT_DIR}/bin/start-splice.sh && ${ROOT_DIR}/bin/start-splice-admin.sh
