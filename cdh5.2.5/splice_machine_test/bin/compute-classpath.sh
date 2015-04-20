#!/usr/bin/env bash

cwd=$(pwd)

mvn -q dependency:build-classpath -Dmdep.outputFile=classpath

THE_CLASSPATH=`cat classpath`:${cwd}/target/classes

echo ${THE_CLASSPATH}
