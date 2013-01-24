#!/bin/bash

REPODIR=/Users/gdavis/.m2/repository
SPLICELIBDIR=$REPODIR/com/splicemachine
SCONSTLIB=$SPLICELIBDIR/structured_constants/1.0.0-SNAPSHOT/structured_constants-1.0.0-SNAPSHOT.jar
STRXLIB=$SPLICELIBDIR/structured_trx/1.0.0-SNAPSHOT/structured_trx-1.0.0-SNAPSHOT.jar
SDERBYLIB=$SPLICELIBDIR/structured_derby/1.0.0-SNAPSHOT/structured_derby-1.0.0-SNAPSHOT.jar
SDERBYTESTLIB=$SPLICELIBDIR/structured_derby/1.0.0-SNAPSHOT/structured_derby-1.0.0-SNAPSHOT-tests.jar

DERBYDIR=/Users/gdavis/Documents/workspace/derby10.9
DERBYTESTLIB=$DERBYDIR/jars/sane/derbyTesting.jar
DERBYCLASSDIR=$DERBYDIR/classes
JUNITLIB=$REPODIR/junit/junit/3.8.1/junit-3.8.1.jar
OROLIB=$DERBYDIR/db-derby-10.9.1.0-lib-debug/test/jakarta-oro-2.0.8.jar
ORDERLYLIB=$REPODIR/com/gotometrics/orderly/orderly/0.11/orderly-0.11.jar
LOG4JLIB=$REPODIR/log4j/log4j/1.2.17/log4j-1.2.17.jar
DATANUCLIB=$REPODIR/org/datanucleus/datanucleus-core/2.0.3/datanucleus-core-2.0.3.jar
GSONLIB=$REPODIR/com/google/code/gson/gson/2.2.1/gson-2.2.1.jar

HBASEDIR=/Users/gdavis/hbstand/hbase-0.92.1-cdh4.0.1
HBASELIBDIR=$HBASEDIR/lib
HBASELIB=$HBASEDIR/hbase-0.92.1-cdh4.0.1-security.jar

export CLASSPATH="$SCONSTLIB:\
$STRXLIB:\
$SDERBYLIB:\
$SDERBYTESTLIB:\
$DERBYTESTLIB:\
$JUNITLIB:\
$OROLIB:\
$DERBYCLASSDIR:\
$ORDERLYLIB:\
$LOG4JLIB:\
$DATANUCLIB:\
$GSONLIB:\
$HBASELIBDIR/*:\
$HBASELIB"
