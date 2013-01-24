#!/bin/bash

REPODIR=/Users/gdavis/.m2/repository

DERBYDIR=/Users/gdavis/derby/db-derby-10.9.1.0-lib-debug
DERBYLIBDIR=$DERBYDIR/lib
DERBYTESTLIB=$DERBYDIR/test/derbyTesting.jar
OROLIB=$DERBYDIR/test/jakarta-oro-2.0.8.jar

JUNITLIB=$REPODIR/junit/junit/3.8.1/junit-3.8.1.jar
LOG4JLIB=$REPODIR/log4j/log4j/1.2.17/log4j-1.2.17.jar

export CLASSPATH="\
$JUNITLIB:\
$DERBYLIBDIR/*:\
$DERBYTESTLIB:\
$OROLIB:\
$LOG4JLIB"

