package com.splicemachine.mrio.api;

import java.io.IOException;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableSplit;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;
import com.splicemachine.derby.hbase.DerbyFactoryDriver;
import com.splicemachine.derby.impl.sql.execute.operations.scanner.SITableScanner;
import com.splicemachine.derby.impl.sql.execute.operations.scanner.TableScannerBuilder;
import com.splicemachine.hbase.MeasuredRegionScanner;
import com.splicemachine.hbase.SimpleMeasuredRegionScanner;
import com.splicemachine.metrics.Metrics;
import com.splicemachine.mrio.MRConstants;
import com.splicemachine.si.impl.HTransactorFactory;
import com.splicemachine.si.impl.TransactionStorage;
import com.splicemachine.si.impl.TxnDataStore;
import com.splicemachine.si.impl.TxnRegion;
import com.splicemachine.si.impl.readresolve.NoOpReadResolver;
import com.splicemachine.si.impl.rollforward.NoopRollForward;
import com.splicemachine.utils.SpliceLogUtils;

public class SMRecordReaderImpl extends RecordReader<ImmutableBytesWritable, ExecRow> {
    protected static final Logger LOG = Logger.getLogger(SMRecordReaderImpl.class);
	protected HTable htable;
	protected HRegion hregion;
	protected Configuration config;
	protected MeasuredRegionScanner mrs;
	protected SITableScanner siTableScanner;
	protected long txnId;
	protected Scan scan;
	protected SMSQLUtil sqlUtil = null;
	protected ExecRow currentRow;
	protected TableScannerBuilder builder;
	
	public SMRecordReaderImpl(Configuration config) {
		this.config = config;
	}	
	
	@Override
	public void initialize(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {	
		if (LOG.isTraceEnabled())
			SpliceLogUtils.trace(LOG, "initialize with split=%s", split);
		init(context.getConfiguration(),split);
	}
	
	public void init(Configuration config, InputSplit split) throws IOException, InterruptedException {	
		if (LOG.isTraceEnabled())
			SpliceLogUtils.trace(LOG, "init");
		String tableScannerAsString = config.get(MRConstants.SPLICE_SCAN_INFO);
		if (tableScannerAsString == null)
			throw new IOException("splice scan info was not serialized to task, failing");
		try {
			builder = TableScannerBuilder.getTableScannerBuilderFromBase64String(tableScannerAsString);
			if (LOG.isTraceEnabled())
				SpliceLogUtils.trace(LOG, "config loaded builder=%s",builder);
			TableSplit tSplit = (TableSplit)split;
			Scan scan = builder.getScan();
			scan.setStartRow(tSplit.getStartRow());
			scan.setStopRow(tSplit.getEndRow());
			this.scan = scan;
			restart(tSplit.getStartRow());
		} catch (StandardException e) {
			throw new IOException(e);
		}
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		try {
			currentRow = siTableScanner.next(null);
			return currentRow != null;
		} catch (StandardException e) {
			throw new IOException(e);
		}
	}

	@Override
	public ImmutableBytesWritable getCurrentKey() throws IOException,
			InterruptedException {
		return null;
	}

	@Override
	public ExecRow getCurrentValue() throws IOException, InterruptedException {
		return currentRow;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return 0;
	}

	@Override
	public void close() throws IOException {
		try {
			siTableScanner.close();
		} catch (StandardException e) {
			throw new IOException(e);
		}
	}
	
	public void setScan(Scan scan) {
		this.scan = scan;
	}
	
	public void setHTable(HTable htable) {
		this.htable = htable;
	}
	
	public void restart(byte[] firstRow) throws IOException {
		Scan newscan = new Scan(scan);
		newscan.setStartRow(firstRow);
		scan = newscan;
		if(htable != null) {
			SpliceRegionScanner splitRegionScanner = DerbyFactoryDriver.derbyFactory.getSplitRegionScanner(new Scan(),htable);
			this.hregion = splitRegionScanner.getRegion();
			this.mrs = new SimpleMeasuredRegionScanner(splitRegionScanner,Metrics.noOpMetricFactory());
			TxnRegion localRegion = new TxnRegion(hregion, NoopRollForward.INSTANCE,NoOpReadResolver.INSTANCE, 
				TransactionStorage.getTxnSupplier(), TxnDataStore.getDataStore(), HTransactorFactory.getTransactor());		
			siTableScanner = builder.tableVersion("2.0").region(localRegion).template(SMSQLUtil.getExecRow(builder.getExecRowTypeFormatIds())).scanner(mrs).scan(scan).metricFactory(Metrics.noOpMetricFactory()).build();		
		} else {
			throw new IOException("htable not set");
		}
	}
	
}