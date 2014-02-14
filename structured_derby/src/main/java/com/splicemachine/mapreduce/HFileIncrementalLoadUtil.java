package com.splicemachine.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.google.gson.Gson;
import com.splicemachine.derby.impl.load.ImportContext;

public class HFileIncrementalLoadUtil {
	public static final String HBASE_TABLE_NAME = "hbase.table.name";
	public static Gson gson = new Gson();
	
	
	public void runHFileIncrementalLoad(ImportContext importContext) throws IOException, InterruptedException, ClassNotFoundException {
		Configuration conf = new Configuration();
		conf.set(HBASE_TABLE_NAME, importContext.getTableId()+"");
		HBaseConfiguration.addHbaseResources(conf);
		Job job = Job.getInstance(conf);
		job.getConfiguration().set(HBaseBulkLoadMapper.IMPORT_CONTEXT, gson.toJson(importContext));
		job.setJarByClass(HFileIncrementalLoadUtil.class);
		job.setMapperClass(HBaseBulkLoadMapper.class);
	    job.setMapOutputKeyClass(ImmutableBytesWritable.class);
	    job.setMapOutputValueClass(KeyValue.class);	
	    job.setInputFormatClass(TextInputFormat.class);
	    HTable hTable = new HTable(job.getConfiguration(),Bytes.toBytes(importContext.getTableId()+""));
	    HFileOutputFormat.configureIncrementalLoad(job, hTable);
	    FileInputFormat.addInputPath(job, importContext.getFilePath());
	    FileOutputFormat.setOutputPath(job, new Path("/john/"));
	    job.waitForCompletion(true);
	}
	
	
	
}
