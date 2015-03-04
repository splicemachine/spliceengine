package com.splicemachine.mrio.api;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.splicemachine.db.iapi.sql.execute.ExecRow;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.regionserver.KeyValueScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.StringUtils;
import com.splicemachine.hbase.MeasuredRegionScanner;
import com.splicemachine.hbase.SimpleMeasuredRegionScanner;
import com.splicemachine.metrics.Metrics;
import com.splicemachine.si.impl.TxnRegion;

public class SMRecordReaderImpl extends RecordReader<ImmutableBytesWritable, ExecRow> {
	protected HTable htable;
	protected Configuration config;
	
	public SMRecordReaderImpl(Configuration config) {
		this.config = config;
	}
	
	@Override
	public void initialize(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {		
		
		
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
		
	}
	
	public void setHTable(HTable htable) {
		this.htable = htable;
	}

/*	
	protected SITableScanner getSITableScanner() {
		TxnRegion localRegion = new TxnRegion(null, null, null, null, null, null);
		SimpleM
		
			SITableScanner tableScanner = new TableScannerBuilder()
							.scanner(regionScanner)
        .region(localRegion)
        .transaction(operationInformation.getTransaction())
							.metricFactory(spliceRuntimeContext)
							.scan(scan)
							.template(currentRow)
							.tableVersion(scanInformation.getTableVersion())
							.indexName(indexName)
							.keyColumnEncodingOrder(scanInformation.getColumnOrdering())
							.keyColumnSortOrder(scanInformation.getConglomerate().getAscDescInfo())
							.keyColumnTypes(getKeyFormatIds())
							.accessedKeyColumns(scanInformation.getAccessedPkColumns())
							.keyDecodingMap(getKeyDecodingMap())
							.rowDecodingMap(baseColumnMap).build();
	}
	*/
	
}