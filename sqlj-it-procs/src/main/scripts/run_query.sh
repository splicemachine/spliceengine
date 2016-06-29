#!/bin/sh
export CLASSPATH=~/.m2/repository/org/apache/derby/derbyclient/10.9.1.0-splice/derbyclient-10.9.1.0-splice.jar:./target/classes
java -Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=4001 com.splicemachine.tools.jdbc.QueryRunner "org.apache.derby.jdbc.ClientDriver" "jdbc:splice://localhost:1527/splicedb;create=true" "select * from sys.sysschemas"
# java com.splicemachine.tools.jdbc.QueryRunner "sun.jdbc.odbc.JdbcOdbcDriver" "jdbc:odbc:SPLICEDBTEST" "select * from sys.sysschemas"
