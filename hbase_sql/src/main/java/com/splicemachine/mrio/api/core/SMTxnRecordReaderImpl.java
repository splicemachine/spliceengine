package com.splicemachine.mrio.api.core;

import com.splicemachine.access.hbase.HBaseConnectionFactory;
import com.splicemachine.concurrent.Clock;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.store.raw.Transaction;
import com.splicemachine.db.iapi.types.RowLocation;
import com.splicemachine.derby.impl.sql.execute.operations.scanner.TableScannerBuilder;
import com.splicemachine.derby.impl.store.access.hbase.HBaseRowLocation;
import com.splicemachine.metrics.Metrics;
import com.splicemachine.mrio.MRConstants;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.storage.*;
import com.splicemachine.storage.util.MeasuredResultScanner;
import com.splicemachine.stream.utils.StreamPartitionUtils;
import com.splicemachine.utils.ByteSlice;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.mapreduce.TableSplit;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class SMTxnRecordReaderImpl extends RecordReader<RowLocation, Transaction> {
    protected static final Logger LOG = Logger.getLogger(SMTxnRecordReaderImpl.class);
	protected Table htable;
	protected HRegion hregion;
	protected Configuration config;
	protected long txnId;
	protected Scan scan;
    protected MeasuredResultScanner mrs;
	protected Transaction currentTransaction;
	protected TableScannerBuilder builder;
	protected RowLocation rowLocation;
	private List<AutoCloseable> closeables = new ArrayList<>();

	public SMTxnRecordReaderImpl(Configuration config) {
		this.config = config;
	}	
	
	@Override
	public void initialize(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {	
		if (LOG.isDebugEnabled())
			SpliceLogUtils.debug(LOG, "initialize with split=%s", split);
		init(config==null?context.getConfiguration():config,split);
	}
	
	public void init(Configuration config, InputSplit split) throws IOException, InterruptedException {	
		if (LOG.isDebugEnabled())
			SpliceLogUtils.debug(LOG, "init");
		String tableScannerAsString = config.get(MRConstants.SPLICE_SCAN_INFO);
        if (tableScannerAsString == null)
			throw new IOException("splice scan info was not serialized to task, failing");
		try {
			builder = TableScannerBuilder.getTableScannerBuilderFromBase64String(tableScannerAsString);
			if (LOG.isTraceEnabled())
				SpliceLogUtils.trace(LOG, "config loaded builder=%s",builder);
			TableSplit tSplit = ((SMSplit)split).getSplit();
			DataScan scan = builder.getScan();
            this.scan = ((HScan)scan).unwrapDelegate();
            this.scan.setStartRow(tSplit.getStartRow());
            this.scan.setStopRow(tSplit.getEndRow());
            restart(tSplit.getStartRow());
		} catch (StandardException e) {
			throw new IOException(e);
		}
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		Cell nextRow = mrs.next().current();
		RowLocation nextLocation = new HBaseRowLocation(ByteSlice.wrap(nextRow.getRowArray(), nextRow.getRowOffset(), nextRow.getRowLength()));
		if (nextRow != null) {
			currentTransaction = null; //TODO
			rowLocation = nextLocation;
		} else {
			currentTransaction = null;
			rowLocation = null;
		}
		return currentTransaction != null;
	}

	@Override
	public RowLocation getCurrentKey() throws IOException, InterruptedException {
		return rowLocation;
	}

	@Override
	public Transaction getCurrentValue() throws IOException, InterruptedException {
		return currentTransaction;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return 0;
	}

	@Override
	public void close() throws IOException {
		IOException lastThrown = null;
		if (LOG.isDebugEnabled())
			SpliceLogUtils.debug(LOG, "close");
		for (AutoCloseable c : closeables) {
			if (c != null) {
				try {
					c.close();
				} catch (Exception e) {
					lastThrown = e instanceof IOException ? (IOException) e : new IOException(e);
				}
			}
		}
		if (lastThrown != null) {
			throw lastThrown;
		}
	}
	
	public void setScan(Scan scan) {
		this.scan = scan;
	}
	
	public void setHTable(Table htable) {
		this.htable = htable;
		addCloseable(htable);
	}
	
	public void restart(byte[] firstRow) throws IOException {		
		Scan newscan = new Scan(scan);
		newscan.setStartRow(firstRow);
        scan = newscan;
		if(htable != null) {
            SIDriver driver = SIDriver.driver();
            HBaseConnectionFactory instance=HBaseConnectionFactory.getInstance(driver.getConfiguration());
            Clock clock = driver.getClock();
            SplitRegionScanner srs = new SplitRegionScanner(
                scan,
                htable,
                instance.getConnection(),
                clock,
                StreamPartitionUtils.getRegionsInRange(instance.getConnection(), htable.getName(), scan.getStartRow(), scan.getStopRow()));
            this.hregion = srs.getRegion();
            this.mrs = new MeasuredResultScanner(htable.getScanner(this.scan), Metrics.noOpMetricFactory());
		} else {
			throw new IOException("htable not set");
		}
	}
	
	public int[] getExecRowTypeFormatIds() {
		if (builder == null) {
			String tableScannerAsString = config.get(MRConstants.SPLICE_SCAN_INFO);
			if (tableScannerAsString == null)
				throw new RuntimeException("splice scan info was not serialized to task, failing");
			try {
				builder = TableScannerBuilder.getTableScannerBuilderFromBase64String(tableScannerAsString);
			} catch (IOException | StandardException  e) {
				throw new RuntimeException(e);
			}
			if (LOG.isTraceEnabled())
				SpliceLogUtils.trace(LOG, "config loaded builder=%s",builder);
		}
		return builder.getExecRowTypeFormatIds();
	}

	public void addCloseable(AutoCloseable closeable) {
		closeables.add(closeable);
	}
	
}