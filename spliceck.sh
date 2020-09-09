#!/bin/bash

platform=cdh6.3.0

function help() {
  echo "spliceck.sh [--platform <platform>] command"
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
  # https://stackoverflow.com/a/8723305/337194
  C=''
  for i in "$@"; do 
      i="${i//\\/\\\\}"
      C="$C '${i//\'/\\\"}'"
  done
  mvn -q -Pcore,${platform} -f ./splice_ck/pom.xml exec:java -Dexec.args="${C}"
fi


