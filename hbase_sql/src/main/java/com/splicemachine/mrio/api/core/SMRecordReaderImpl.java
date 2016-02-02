package com.splicemachine.mrio.api.core;

import com.splicemachine.access.hbase.HBaseConnectionFactory;
import com.splicemachine.concurrent.Clock;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.RowLocation;
import com.splicemachine.derby.impl.sql.execute.operations.scanner.SITableScanner;
import com.splicemachine.derby.impl.sql.execute.operations.scanner.TableScannerBuilder;
import com.splicemachine.derby.impl.store.access.hbase.HBaseRowLocation;
import com.splicemachine.metrics.Metrics;
import com.splicemachine.mrio.MRConstants;
import com.splicemachine.si.api.server.TransactionalRegion;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.storage.*;
import com.splicemachine.stream.utils.StreamPartitionUtils;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.mapreduce.TableSplit;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class SMRecordReaderImpl extends RecordReader<RowLocation, ExecRow> {
    protected static final Logger LOG = Logger.getLogger(SMRecordReaderImpl.class);
	protected Table htable;
	protected HRegion hregion;
	protected Configuration config;
	protected RegionScanner mrs;
	protected SITableScanner siTableScanner;
//	protected long txnId;
	protected Scan scan;
//	protected SMSQLUtil sqlUtil = null;
	protected ExecRow currentRow;
	protected TableScannerBuilder builder;
	protected RowLocation rowLocation;
	private List<AutoCloseable> closeables = new ArrayList<>();
	
	public SMRecordReaderImpl(Configuration config) {
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
			scan.startKey(tSplit.getStartRow()).stopKey(tSplit.getEndRow());
            this.scan = ((HScan)scan).unwrapDelegate();
			restart(tSplit.getStartRow());
		} catch (StandardException e) {
			throw new IOException(e);
		}
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		try {
			ExecRow nextRow = siTableScanner.next();
            RowLocation nextLocation = siTableScanner.getCurrentRowLocation();
			if (nextRow != null) {
				currentRow = nextRow.getClone(); 
				rowLocation = new HBaseRowLocation(nextLocation.getBytes());
			} else {
				currentRow = null;
				rowLocation = null;
			}			
			return currentRow != null;
		} catch (StandardException e) {
			throw new IOException(e);
		}
	}

	@Override
	public RowLocation getCurrentKey() throws IOException, InterruptedException {
		return rowLocation;
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
		Scan newscan = scan;
		newscan.setStartRow(firstRow);
        scan = newscan;
		if(htable != null) {
			SIDriver driver=SIDriver.driver();
			HBaseConnectionFactory instance=HBaseConnectionFactory.getInstance(driver.getConfiguration());
			Clock clock = driver.getClock();
			SplitRegionScanner srs = new SplitRegionScanner(scan,
					htable,
					instance.getConnection(),
					clock,
					StreamPartitionUtils.getRegionsInRange(instance.getConnection(),htable.getName(),scan.getStartRow(),scan.getStopRow()));
			this.hregion = srs.getRegion();
			this.mrs = srs;

			ExecRow template = SMSQLUtil.getExecRow(builder.getExecRowTypeFormatIds());
			long conglomId = Long.parseLong(hregion.getTableDesc().getTableName().getQualifierAsString());
            TransactionalRegion region=SIDriver.driver().transactionalPartition(conglomId,new RegionPartition(hregion));
            builder.region(region)
                    .template(template)
                    .scan(new HScan(scan))
                    .scanner(new RegionDataScanner(new RegionPartition(hregion),mrs,Metrics.basicMetricFactory()));
			if (LOG.isTraceEnabled())
				SpliceLogUtils.trace(LOG, "restart with builder=%s",builder);
			siTableScanner = builder.build();
			addCloseable(siTableScanner);
			addCloseable(siTableScanner.getRegionScanner());
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