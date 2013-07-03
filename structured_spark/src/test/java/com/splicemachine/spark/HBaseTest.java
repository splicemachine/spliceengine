package com.splicemachine.spark;

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;

import scala.Tuple2;
import spark.api.java.JavaPairRDD;
import spark.api.java.JavaSparkContext;

public class HBaseTest {
		public static String test = "test1";
	
	
	  public static void main(String[] args) throws Exception {
	    JavaSparkContext ctx = new JavaSparkContext("local", "JavaWordCount");
	    Configuration conf = HBaseConfiguration.create();
	    conf.set(TableInputFormat.INPUT_TABLE, "32");
	    conf.setInt(TableInputFormat.SCAN_CACHEDROWS, 16000);
	    conf.set(TableInputFormat.SCAN_COLUMN_FAMILY, "S");	    
	    JavaPairRDD<ImmutableBytesWritable, Result> rows = ctx.newAPIHadoopRDD(conf, TableInputFormat.class, ImmutableBytesWritable.class, Result.class);	    
	    if (test.equals("test")) {
	    	System.out.println("Count : " + rows.count());
	    } else {
	    	 List<Tuple2<ImmutableBytesWritable, Result>> output = rows.collect();
	    	    for (Tuple2 tuple : output) {
	    	      System.out.println(tuple._1 + ": " + tuple._2);
	    	    }
	    }
	    System.exit(0);
	  }	  	  
	}