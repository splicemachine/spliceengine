/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.mrio.api.core;

import com.splicemachine.access.hbase.HBaseConnectionFactory;
import com.splicemachine.client.SpliceClient;
import com.splicemachine.concurrent.Clock;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.RowLocation;
import com.splicemachine.derby.impl.sql.execute.operations.scanner.SITableScanner;
import com.splicemachine.derby.impl.sql.execute.operations.scanner.TableScannerBuilder;
import com.splicemachine.metrics.Metrics;
import com.splicemachine.mrio.MRConstants;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.si.api.server.TransactionalRegion;
import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.si.constants.SIConstants;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.si.impl.region.SamplingFilter;
import com.splicemachine.storage.*;
import com.splicemachine.storage.SplitRegionScanner;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.mapreduce.TableSplit;
import org.apache.hadoop.hbase.regionserver.HBasePlatformUtils;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;
import org.apache.spark.TaskContext;
import org.apache.spark.util.TaskFailureListener;

import java.io.IOException;
import java.util.*;

public class SMRecordReaderImpl extends RecordReader<RowLocation, ExecRow> implements TaskFailureListener {
    protected static final Logger LOG = Logger.getLogger(SMRecordReaderImpl.class);
	protected Table htable;
	protected HRegion hregion;
	protected Configuration config;
	protected RegionScanner mrs;
	protected SITableScanner siTableScanner;
	protected Scan scan;
	protected ExecRow currentRow;
	protected TableScannerBuilder builder;
	protected RowLocation rowLocation;
	private List<AutoCloseable> closeables = new ArrayList<>();
    private boolean statisticsRun = false;
    private boolean sampling = false;
    private double samplingRate = 0;
	private Txn localTxn;
	private volatile boolean closed = false;
	private String closeExceptionString;
	private InputSplit split;
	private byte[] token = null;

	public SMRecordReaderImpl(Configuration config) {
		this.config = config;
	}
	public long getRegionSize() {
		long size = 0;
		for (Store store:hregion.getStores()) {
			size += store.getSize();
		}
		return size;
	}
	
	@Override
	public void initialize(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {	
		if (LOG.isDebugEnabled())
			SpliceLogUtils.debug(LOG, "initialize with split=%s", split);
		closeExceptionString = String.format("Unexpected exception on close for split %s", split.toString());
		this.split = split;
		init(config==null?context.getConfiguration():config,split);
	}
	
	public void init(Configuration config, InputSplit split) throws IOException, InterruptedException {	
		if (LOG.isDebugEnabled())
			SpliceLogUtils.debug(LOG, "init");
		if (TaskContext.get() != null) {
			TaskContext.get().addTaskFailureListener(this);
		}
		String tableScannerAsString = config.get(MRConstants.SPLICE_SCAN_INFO);
        if (tableScannerAsString == null)
			throw new IOException("splice scan info was not serialized to task, failing");
		byte[] scanStartKey = null;
		byte[] scanStopKey = null;
		try {
			builder = TableScannerBuilder.getTableScannerBuilderFromBase64String(tableScannerAsString);
			if (LOG.isTraceEnabled())
				SpliceLogUtils.trace(LOG, "config loaded builder=%s", builder);
			TableSplit tSplit = ((SMSplit) split).getSplit();
			token = builder.getToken();
			DataScan scan = builder.getScan();
			scanStartKey = scan.getStartKey();
			scanStopKey = scan.getStopKey();
			if (Bytes.startComparator.compare(scanStartKey, tSplit.getStartRow()) < 0) {
				// the split itself is more restrictive
				scan.startKey(tSplit.getStartRow());
			}
			if (Bytes.endComparator.compare(scanStopKey, tSplit.getEndRow()) > 0) {
				// the split itself is more restrictive
				scan.stopKey(tSplit.getEndRow());
			}
			setScan(((HScan) scan).unwrapDelegate());
			// TODO (wjk): this seems weird (added with DB-4483)
			this.statisticsRun = AbstractSMInputFormat.oneSplitPerRegion(config);
			Double sampling = AbstractSMInputFormat.sampling(config);
			if (sampling != null) {
				this.sampling = true;
				this.samplingRate = sampling;
			}
			restart(scan.getStartKey());
		} catch (IOException ioe) {
			LOG.error(String.format("Received exception with scan %s, original start key %s, original stop key %s, split %s",
					scan, Bytes.toStringBinary(scanStartKey), Bytes.toStringBinary(scanStopKey), split), ioe);
			throw ioe;
        } catch (StandardException e) {
			throw new IOException(e);
		}
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		try {
			if (closed) {
				LOG.error("Calling next() on closed record reader");
				throw new IOException("RecordReader is closed");
			}
			currentRow = siTableScanner.next();
			rowLocation = siTableScanner.getCurrentRowLocation();
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
		try {
			IOException lastThrown = null;
			closed = true;
			if (LOG.isDebugEnabled())
				SpliceLogUtils.debug(LOG, "close");
			if (localTxn != null) {
				try {
					localTxn.commit();
				} catch (IOException ioe) {
					try {
						localTxn.rollback();
					} catch (Exception e) {
						ioe.addSuppressed(e);
					}
					lastThrown = ioe;
				}
			}

			for (AutoCloseable c : closeables) {
				if (c != null) {
					try {
						c.close();
					} catch (Exception e) {
						if (lastThrown != null)
							lastThrown.addSuppressed(e);
						else
							lastThrown = e instanceof IOException ? (IOException) e : new IOException(e);
					}
				}
			}
			if (lastThrown != null) {
				throw lastThrown;
			}
		} catch (Throwable t) {
			// Try to allocate as little as possible in case the error is caused by an OOM situation
			LOG.error(closeExceptionString);
			LOG.error("Exception: ", t);
			throw t;
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
        setScan(newscan);
		if(htable != null) {
			SIDriver driver=SIDriver.driver();

			HBaseConnectionFactory instance=HBaseConnectionFactory.getInstance(driver.getConfiguration());
			boolean debugConnections = driver.getConfiguration().getAuthenticationTokenDebugConnections();
			int maxConnections = driver.getConfiguration().getAuthenticationTokenMaxConnections();
			Clock clock = driver.getClock();
            // Hack added to fix statistics run... DB-4752
            if (statisticsRun)
                driver.getPartitionInfoCache().invalidate(htable.getName());
            Partition clientPartition;
            if (SpliceClient.isClient() && validToken()) {
            	scan.setAttribute(SIConstants.TOKEN_ACL_NAME, token);
		ClientPartition delegate = new ClientPartition(instance.getConnection(), htable.getName(), htable, clock, driver.getPartitionInfoCache());
		clientPartition = new AdapterPartition(delegate, instance.getConnection(), SpliceClient.getConnectionPool(debugConnections, maxConnections),htable.getName(), driver.getPartitionInfoCache());
			} else {
            	clientPartition = new ClientPartition(instance.getConnection(),htable.getName(),htable,clock,driver.getPartitionInfoCache());
			}
			if (sampling) {
            	scan.setFilter(new SamplingFilter(samplingRate));
			}
			SplitRegionScanner srs = new SplitRegionScanner(scan,
					htable,
					clock,
					clientPartition,
					driver.getConfiguration(),
					config
			);
			this.hregion = srs.getRegion();
			this.mrs = srs;
			ExecRow template = getExecRow();
            assert this.hregion !=null:"Returned null HRegion for htable "+htable.getName();
			String tableName = hregion.getTableDescriptor().getTableName().getQualifierAsString();
			long conglomId = Long.parseLong(tableName);
            TransactionalRegion region=SIDriver.driver().transactionalPartition(conglomId,new RegionPartition(hregion));
            TxnView parentTxn = builder.getTxn();
            this.localTxn = SIDriver.driver().lifecycleManager().beginChildTransaction(parentTxn, parentTxn.getIsolationLevel(), true, null);
            builder.region(region)
                    .template(template)
                    .transaction(localTxn)
                    .scan(new HScan(scan))
                    .scanner(new RegionDataScanner(new RegionPartition(hregion),mrs,statisticsRun?Metrics.basicMetricFactory():Metrics.noOpMetricFactory()));
			if (LOG.isTraceEnabled())
				SpliceLogUtils.trace(LOG, "restart with builder=%s",builder);
			siTableScanner = builder.build();
			addCloseable(siTableScanner);
		} else {
			throw new IOException("htable not set");
		}
	}

	private boolean validToken() {
		return token != null && token.length > 0;
	}


	public ExecRow getExecRow() {
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
		return builder.getTemplate();
	}

	public void addCloseable(AutoCloseable closeable) {
		closeables.add(closeable);
	}


	@Override
	public void onTaskFailure(TaskContext context, Throwable error) {
		LOG.error("Task failed for split: " + split, error);
	}
}
