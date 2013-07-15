package com.splicemachine.spark;

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.junit.Ignore;

import scala.Tuple2;
import spark.api.java.JavaPairRDD;
import spark.api.java.JavaSparkContext;
@Ignore
public class HBaseTest {
	  private static String tableName;
	  public HBaseTest(String tableName) {
		  this.tableName = tableName;
	  }
	
	  public static void main(String[] args) throws Exception {
		String[] jars = {"/Users/johnleach/.m2/repository/org/apache/hbase/hbase/0.94.6-cdh4.3.0/hbase-0.94.6-cdh4.3.0.jar","/Users/johnleach/splicemachine/workspace/structured_hbase/structured_spark/target/structured_spark-1.0.1-SNAPSHOT.jar"};
	    JavaSparkContext ctx = new JavaSparkContext("spark://Johns-MacBook-Pro-2.local:7077", "JavaWordCount", "/Users/johnleach/SpliceMachine/spark-0.7.2",jars);
//	    JavaSparkContext ctx = new JavaSparkContext("local", "JavaWordCount");
	    Configuration conf = HBaseConfiguration.create();
	    conf.set(TableInputFormat.INPUT_TABLE, tableName);
	    conf.setInt(TableInputFormat.SCAN_CACHEDROWS, 16000);
	    conf.set(TableInputFormat.SCAN_COLUMN_FAMILY, "S");	    
	    JavaPairRDD<ImmutableBytesWritable, Result> rows = ctx.newAPIHadoopRDD(conf, TableInputFormat.class, ImmutableBytesWritable.class, Result.class);	    
	    System.out.println("Count : " + rows.count());
	    System.exit(0);
	  }	  	  
	}