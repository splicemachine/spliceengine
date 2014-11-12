Setting up HBase...

add these libraries to the hbase lib/ directory.

gson-2.2.2.jar
orderly-0.11.jar
structured_constants-1.0.0-SNAPSHOT.jar
structured_trx-1.0.0-SNAPSHOT.jar
derby-10.9.1.0.splice.jar
structured_derby-1.0.0-SNAPSHOT.jar
datanucleus-core-2.0.3.jar
datanucleus-rdbms-2.0.3.jar
datanucleus-enhancer-2.0.3.jar
datanucleus-connectionpool-2.0.3.jar


hbase-site.xml modification...

<configuration>
<property>
    <name>hbase.coprocessor.region.classes</name>
    <value>com.splicemachine.derby.hbase.SpliceOperationRegionObserver,com.splicemachine.derby.hbase.SpliceOperationCoprocessor</value>
 </property>

  <property>
    <name>hbase.rootdir</name>
    <value>file:///Users/hadoop/hbase</value>
  </property>
</configuration>

To Create a New Operation...

- Place the code into the com/splicemachine/derby/impl/sql/execute/operations package.

- They should all extend SpliceBaseOperation (See TableScanOperation.java and ScanOperation.java for an example).  
 	
- You will need to implement the corresponding ResultSet in the SpliceGenericResultSetFactory to point to the new class.
