#!/bin/sh

HBASE_HOME=~/apps/hbase

REPO_HOME="/Users/scottfines/.m2/repository"
VERSION="2.0.0.k2_refactor-SNAPSHOT"
CLASSPATH="${REPO_HOME}/com/splicemachine/db-project/db-client/${VERSION}/db-client-${VERSION}.jar:${REPO_HOME}/com/splicemachine/db-project/db-engine/${VERSION}/db-engine-${VERSION}.jar:${REPO_HOME}/com/splicemachine/db-project/db-tools-ij/2.0.0.1-SNAPSHOT/db-tools-ij-2.0.0.1-SNAPSHOT.jar"
ARGS="-cp $CLASSPATH"
#ARGS="$ARGS -Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=8072"
ARGS="$ARGS -Dhbase.log.dir=\".\" -Dsplice.log.file=\"client.log\""
ARGS="$ARGS -Dij.connection.splice=jdbc:splice://localhost:1527/splicedb;user=splice;password=admin"

rlwrap java $ARGS com.splicemachine.db.tools.ij $@
#rlwrap java -cp $CLASSPATH -Dhbase.log.dir="." -Dsplice.log.file="client.log" -Dij.connection.splice="jdbc:derby://localhost:1527/splicedb;user=splice;password=admin" com.splicemachine.db.tools.ij $@
