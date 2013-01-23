package com.ir.hbase.hive;

import java.io.IOException;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.Progressable;

public class HiveHBaseTableOutputFormat extends TableOutputFormat<ImmutableBytesWritable> implements HiveOutputFormat<ImmutableBytesWritable, Put>, OutputFormat<ImmutableBytesWritable, Put> {

	static final Log LOG = LogFactory.getLog(HiveHBaseTableOutputFormat.class);
	public static final String HBASE_WAL_ENABLED = "hive.hbase.wal.enabled";
	
	
/**
* Update the out table, and output an empty key as the key.
*
* @param jc the job configuration file
* @param finalOutPath the final output table name
* @param valueClass the value class
* @param isCompressed whether the content is compressed or not
* @param tableProperties the table info of the corresponding table
* @param progress progress used for status report
* @return the RecordWriter for the output file
*/
@Override
public RecordWriter getHiveRecordWriter(JobConf jc,Path finalOutPath,Class<? extends Writable> valueClass,boolean isCompressed,Properties tableProperties,final Progressable progressable) throws IOException {
	String hbaseTableName = jc.get(HBaseSerDe.HBASE_TABLE_NAME);
	if (hbaseTableName == null) 
		throw new IOException("Hbase Table Name is null");
	jc.set(TableOutputFormat.OUTPUT_TABLE, hbaseTableName);
	final boolean walEnabled = HiveConf.getBoolVar(jc, HiveConf.ConfVars.HIVE_HBASE_WAL_ENABLED);
	Configuration conf = HBaseConfiguration.create(jc);
	conf.set(QUORUM_ADDRESS, "ubuntu2:2181,ubuntu3:2181,ubuntu4:2181");
	if (!HTable.isTableEnabled(conf,hbaseTableName.getBytes())) {
		throw new IOException("Table is not enabled!");
	}
	HTable table2 = new HTable(conf, hbaseTableName);
	return new IRRecordWriter(table2);
}

private static class IRRecordWriter implements RecordWriter {
	HTable table;
	public IRRecordWriter(HTable table) {
		this.table = table;
	}

	@Override
	public void write(Writable w) throws IOException {
		Put put = (Put) w;
		put.setWriteToWAL(false);
		table.put(put);
	}

	@Override
	public void close(boolean abort) throws IOException {
		if (!abort) {
			table.flushCommits();
		}
	}
	
}

@Override
public void checkOutputSpecs(FileSystem fs, JobConf jc) throws IOException {

String hbaseTableName = jc.get(HBaseSerDe.HBASE_TABLE_NAME);
jc.set(TableOutputFormat.OUTPUT_TABLE, hbaseTableName);
Job job = new Job(jc);
JobContext jobContext = ShimLoader.getHadoopShims().newJobContext(job);

try {
  checkOutputSpecs(jobContext);
} catch (InterruptedException e) {
  throw new IOException(e);
}
}

@Override
public
org.apache.hadoop.mapred.RecordWriter<ImmutableBytesWritable, Put>
getRecordWriter(
  FileSystem fileSystem,
  JobConf jobConf,
  String name,
  Progressable progressable) throws IOException {

throw new RuntimeException("Error: Hive should not invoke this method.");
}


}
