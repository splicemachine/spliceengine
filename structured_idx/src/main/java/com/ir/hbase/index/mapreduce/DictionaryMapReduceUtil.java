package com.ir.hbase.index.mapreduce;

import java.io.IOException;
import java.util.HashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryPrefixComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.log4j.Logger;
import com.ir.constants.SchemaConstants;
import com.ir.hbase.client.index.Index;
import com.ir.hbase.client.structured.Column;
import com.ir.hbase.coprocessor.SchemaUtils;
//Noop class for test use
public class DictionaryMapReduceUtil {
	private static Logger LOG = Logger.getLogger(DictionaryMapReduceUtil.class);

	public static long MIN_TIMESTAMP = 0L; 

	public static void addIndex(byte[] tableName, Index index, long timestamp, Configuration conf) throws IOException, InterruptedException, ClassNotFoundException {
		if (LOG.isDebugEnabled())
			LOG.debug("Run MapReduce adding Index " + index.getIndexName() + " to table " + Bytes.toString(tableName));
		conf.set(TableOutputFormat.QUORUM_ADDRESS, 	"ubuntu2,ubuntu3,ubuntu4");
		conf.set(SchemaConstants.SERIALIZED_INDEX, index.toJSon());
		conf.set(TableOutputFormat.OUTPUT_TABLE, SchemaUtils.appendSuffix(Bytes.toString(tableName), SchemaConstants.INDEX));
		Job job = Job.getInstance(conf);
		Scan scan = new Scan();
		for (Column column: index.getAllColumns()) {
			scan.addColumn(Bytes.toBytes(column.getFamily()), Bytes.toBytes(column.getColumnName()));
		}	
		scan.setTimeRange(MIN_TIMESTAMP, timestamp);
		scan.setCaching(100);
		job.setJobName("HBaseInternal:AddIndex: " + index.getIndexName());		
		TableMapReduceUtil.initTableMapperJob(tableName, scan, AddIndexMapper.class, NullWritable.class, Put.class, job);
		job.setJarByClass(DictionaryMapReduceUtil.class);
		job.setOutputFormatClass(TableOutputFormat.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Put.class);
		job.waitForCompletion(true);
	}
	
	public static void deleteIndex(byte[] itableName, String indexName, long timestamp, Configuration conf) throws IOException, InterruptedException, ClassNotFoundException {
		if (LOG.isDebugEnabled())
			LOG.debug("Run MapReduce deleting Index " + indexName + " from table " + Bytes.toString(itableName));
		
		conf.set(TableOutputFormat.OUTPUT_TABLE, Bytes.toString(itableName));
		Job job = Job.getInstance(conf);
		Scan scan = new Scan();
		scan.setCaching(100);
		scan.setFilter(new RowFilter(CompareFilter.CompareOp.EQUAL, new BinaryPrefixComparator(Bytes.toBytes(indexName))));
		scan.setTimeRange(MIN_TIMESTAMP, timestamp);
		scan.setCaching(100);
		job.setJarByClass(DictionaryMapReduceUtil.class);
		job.setJobName("HBaseInternal:DeleteIndex: " + indexName);		
		TableMapReduceUtil.initTableMapperJob(itableName, scan, DeleteIndexMapper.class, NullWritable.class, Delete.class, job);
		job.setOutputFormatClass(TableOutputFormat.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Delete.class);
		job.waitForCompletion(true);
	}
	
	public static void deleteColumn(byte[] tableName, Column column, long timestamp, Configuration conf) throws IOException, InterruptedException, ClassNotFoundException {
		if (LOG.isDebugEnabled())
			LOG.debug("Run MapReduce deleting column " + column.getColumnName() + " from table " + Bytes.toString(tableName));
		
		conf.set(TableOutputFormat.OUTPUT_TABLE, Bytes.toString(tableName));
		Job job = Job.getInstance(conf);
		Scan scan = new Scan();
		scan.addColumn(Bytes.toBytes(column.getFamily()), Bytes.toBytes(column.getColumnName()));
		scan.setTimeRange(MIN_TIMESTAMP, timestamp);
		scan.setCaching(100);
		job.setJarByClass(DictionaryMapReduceUtil.class);
		job.setJobName("HBaseInternal:DeleteColumn: " + column.getColumnName());		
		TableMapReduceUtil.initTableMapperJob(tableName, scan, DeleteColumnMapper.class, NullWritable.class, Delete.class, job);
		job.setOutputFormatClass(TableOutputFormat.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Delete.class);
		job.waitForCompletion(false);
	}
}
