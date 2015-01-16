package com.splicemachine.mrio.api;

import java.io.IOException;
import java.util.List;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableSplit;
import org.apache.hadoop.hbase.regionserver.KeyValueScanner;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class SMRecordReader extends RecordReader<ImmutableBytesWritable, ExecRow> {
	protected SpliceClientSideRegionScanner[] scanners;
	protected List<KeyValueScanner> keyValueScanners;
	protected HTable htable = null;
	protected FileSystem fs;
	protected Path path;
	protected HTableDescriptor htd;
	protected HRegionInfo hri;
	protected Scan scan;
	
	
	
	@Override
	public void initialize(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {	
		TableSplit tableSplit = (TableSplit) split;
		SpliceClientSideRegionScanner scanner = new SpliceClientSideRegionScanner(context.getConfiguration(),fs,path,htd,hri,scan,null,null);

	
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		return false;
	}

	@Override
	public ImmutableBytesWritable getCurrentKey() throws IOException,
			InterruptedException {
		return null;
	}

	@Override
	public ExecRow getCurrentValue() throws IOException, InterruptedException {
		return null;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return 0;
	}

	@Override
	public void close() throws IOException {
//		scanner.close();
	}
	
}
