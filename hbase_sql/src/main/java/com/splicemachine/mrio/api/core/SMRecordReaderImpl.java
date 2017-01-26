/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
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
import com.splicemachine.concurrent.Clock;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.RowLocation;
import com.splicemachine.db.impl.sql.compile.QueryTreeNode;
import com.splicemachine.derby.impl.sql.execute.operations.scanner.SITableScanner;
import com.splicemachine.derby.impl.sql.execute.operations.scanner.TableScannerBuilder;
import com.splicemachine.derby.impl.store.access.hbase.HBaseRowLocation;
import com.splicemachine.derby.stream.ActivationHolder;
import com.splicemachine.derby.stream.spark.SparkOperationContext;
import com.splicemachine.metrics.Metrics;
import com.splicemachine.mrio.MRConstants;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.si.api.server.TransactionalRegion;
import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.storage.*;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.SerializationUtils;
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
import java.util.*;

public class SMRecordReaderImpl extends RecordReader<RowLocation, ExecRow> {
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
	private Txn localTxn;
	private ActivationHolder activationHolder;


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
		String operationContextAsString = config.get(MRConstants.SPLICE_OPERATION_CONTEXT);
        if (tableScannerAsString == null)
			throw new IOException("splice scan info was not serialized to task, failing");
		try {
			builder = TableScannerBuilder.getTableScannerBuilderFromBase64String(tableScannerAsString);
			SparkOperationContext operationContext = null;
			if (operationContextAsString != null) {
				operationContext = (SparkOperationContext) SerializationUtils.deserialize(Base64.decodeBase64(operationContextAsString));
			}
			if (LOG.isTraceEnabled())
				SpliceLogUtils.trace(LOG, "config loaded builder=%s",builder);
			TableSplit tSplit = ((SMSplit)split).getSplit();
			DataScan scan = builder.getScan();
			if (Bytes.startComparator.compare(scan.getStartKey(), tSplit.getStartRow()) < 0) {
				// the split itself is more restrictive
				scan.startKey(tSplit.getStartRow());
			}
			if (Bytes.endComparator.compare(scan.getStopKey(), tSplit.getEndRow()) > 0) {
				// the split itself is more restrictive
				scan.stopKey(tSplit.getEndRow());
			}
            setScan(((HScan)scan).unwrapDelegate());
            // TODO (wjk): this seems weird (added with DB-4483)
            this.statisticsRun = AbstractSMInputFormat.oneSplitPerRegion(config);
	    restart(scan.getStartKey());

            if (operationContext != null) {
                activationHolder = operationContext.getActivationHolder();
                if (activationHolder != null)
                    activationHolder.reinitialize(null);
            }

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
                if (nextLocation!=null)
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
        if (activationHolder!=null) {
            //activationHolder.close();
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
			Clock clock = driver.getClock();
            // Hack added to fix statistics run... DB-4752
            if (statisticsRun)
                driver.getPartitionInfoCache().invalidate(htable.getName());
            Partition clientPartition = new ClientPartition(instance.getConnection(),htable.getName(),htable,clock,driver.getPartitionInfoCache());
			SplitRegionScanner srs = new SplitRegionScanner(scan,
					htable,
					clock,
					clientPartition,
					driver.getConfiguration()
			);
			this.hregion = srs.getRegion();
			this.mrs = srs;
			ExecRow template = getExecRow();
            assert this.hregion !=null:"Returned null HRegion for htable "+htable.getName();
			long conglomId = Long.parseLong(hregion.getTableDesc().getTableName().getQualifierAsString());
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
			addCloseable(siTableScanner.getRegionScanner());
		} else {
			throw new IOException("htable not set");
		}
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


}
