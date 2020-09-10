#!/bin/bash

platform=cdh6.3.0

function help() {
  echo "sck.sh [--platform <platform>] command"
  echo "<platform> The cluster platform, default is cdh6.3.0"
}

if [ $# -eq 0 ]
then
  help
else
  if [ $1 = "--platform" ]
  then
     platform=$2
     shift 2
  fi
  mvn -q -Pcore,${platform} -f ./splice_ck/pom.xml exec:java -Dexec.args="$*"
fi


