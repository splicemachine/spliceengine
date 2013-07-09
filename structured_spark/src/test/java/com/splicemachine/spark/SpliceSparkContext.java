package com.splicemachine.spark;

import spark.api.java.JavaSparkContext;

public class SpliceSparkContext {
	protected static JavaSparkContext ctx;
	protected static String[] jars = {"/Users/johnleach/.m2/repository/org/apache/derby/derby/10.9.1.0.splice/derby-10.9.1.0.splice.jar","/Users/johnleach/splicemachine/workspace/structured_hbase/structured_derby/target/splice_machine-1.0.1-SNAPSHOT.jar","/Users/johnleach/.m2/repository/org/apache/hbase/hbase/0.94.6-cdh4.3.0/hbase-0.94.6-cdh4.3.0.jar","/Users/johnleach/splicemachine/workspace/structured_hbase/structured_spark/target/structured_spark-1.0.1-SNAPSHOT.jar"};

    private SpliceSparkContext() {
    	ctx = new JavaSparkContext("spark://Johns-MacBook-Pro-2.local:7077", "JavaWordCount", "/Users/johnleach/SpliceMachine/spark-0.7.2",jars);
    }
    
    public static JavaSparkContext getSparkContext() {
    	if (ctx == null)
    		new SpliceSparkContext();
    	return ctx;
    }
    
}
