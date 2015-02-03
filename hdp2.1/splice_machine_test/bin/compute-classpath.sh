#!/usr/bin/env bash

cwd=$(pwd)

for i in `ls target/dependency/*.jar`
do
  THE_CLASSPATH=${THE_CLASSPATH}:${cwd}/${i}
done

THE_CLASSPATH=${THE_CLASSPATH}:${cwd}/target/classes

echo ${THE_CLASSPATH}