package com.splicemachine.spark;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import spark.api.java.JavaPairRDD;
import spark.api.java.JavaSparkContext;

public class HBaseTest {
/*
	protected static MiniYARNCluster cluster;
	
	  public static void main(String[] args) throws Exception {
	    JavaSparkContext ctx = new JavaSparkContext("local", "JavaWordCount");
	    Configuration conf = HBaseConfiguration.create();
	    conf.set(TableInputFormat.INPUT_TABLE, "929");
	    JavaPairRDD<ImmutableBytesWritable, Result> rows = ctx.newAPIHadoopRDD(conf,SpliceInputFormatBase.class,ImmutableBytesWritable.class, Result.class);
	    rows.count();
	    System.exit(0);
	  }	  
	  */
	}

