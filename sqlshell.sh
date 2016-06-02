#!/bin/sh
DIR=$(pwd)
cd splice_machine; rlwrap mvn exec:java ; cd ${DIR}

