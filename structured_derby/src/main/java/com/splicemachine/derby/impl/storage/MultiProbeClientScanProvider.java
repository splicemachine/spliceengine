package com.splicemachine.derby.impl.storage;

import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.impl.store.access.SpliceAccessManager;
import com.splicemachine.derby.metrics.OperationMetric;
import com.splicemachine.derby.metrics.OperationRuntimeStats;
import com.splicemachine.derby.utils.Exceptions;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.derby.utils.marshall.PairDecoder;
import com.splicemachine.stats.TimeView;
import com.splicemachine.utils.SpliceLogUtils;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.Activation;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.List;

/**
 *
 *
 *
 */
public class MultiProbeClientScanProvider extends AbstractMultiScanProvider {
		private static final Logger LOG = Logger.getLogger(MultiProbeClientScanProvider.class);
		private final byte[] tableName;
		private HTableInterface htable;
		private final List<Scan> scans;
		private SpliceResultScanner scanner;
		private long startTimestamp;
		private long stopTimestamp;

		public MultiProbeClientScanProvider(String type,byte[] tableName,
																				List<Scan> scans,
																				PairDecoder decoder,
																				SpliceRuntimeContext spliceRuntimeContext) {
				super(decoder, type, spliceRuntimeContext);
				SpliceLogUtils.trace(LOG, "instantiated");
				this.tableName = tableName;
				this.scans = scans;
		}

		@Override
		public Result getResult() throws StandardException {
				try {
						Result n = scanner.next();
						if(n==null)
								stopTimestamp = System.currentTimeMillis();
						return n;
				} catch (IOException e) {
						SpliceLogUtils.logAndThrow(LOG,"Unable to getResult",Exceptions.parseException(e));
						return null;//won't happen
				}
		}

		@Override
		public void open() {
				SpliceLogUtils.trace(LOG, "open");
				if(htable==null)
						htable = SpliceAccessManager.getHTable(tableName);
				try {
						scanner = ProbeDistributedScanner.create(htable, scans,spliceRuntimeContext);
				} catch (IOException e) {
						SpliceLogUtils.logAndThrowRuntime(LOG,"unable to open table "+ Bytes.toString(tableName),e);
				}
				startTimestamp = System.currentTimeMillis();
		}

		@Override
		public void close() {
				super.close();
				SpliceLogUtils.trace(LOG, "closed after calling hasNext %d times",called);
				if(scanner!=null)scanner.close();
				if(htable!=null)
						try {
								htable.close();
						} catch (IOException e) {
								SpliceLogUtils.logAndThrowRuntime(LOG,"unable to close htable for "+ Bytes.toString(tableName),e);
						}
		}

		@Override
		public List<Scan> getScans() throws StandardException {
				return scans;
		}

		@Override
		public byte[] getTableName() {
				return tableName;
		}
		@Override
		public SpliceRuntimeContext getSpliceRuntimeContext() {
				return spliceRuntimeContext;
		}

		@Override
		public void reportStats(long statementId, long operationId, long taskId, String xplainSchema,String regionName) {
				if(regionName==null)
						regionName = "ControlRegion";
				OperationRuntimeStats stats = new OperationRuntimeStats(statementId,operationId,taskId,regionName,9);
				stats.addMetric(OperationMetric.REMOTE_SCAN_BYTES,scanner.getRemoteBytesRead());
				stats.addMetric(OperationMetric.REMOTE_SCAN_ROWS,scanner.getRemoteRowsRead());
				TimeView timeView = scanner.getRemoteReadTime();
				stats.addMetric(OperationMetric.REMOTE_SCAN_WALL_TIME,timeView.getWallClockTime());
				stats.addMetric(OperationMetric.REMOTE_SCAN_CPU_TIME,timeView.getCpuTime());
				stats.addMetric(OperationMetric.REMOTE_SCAN_USER_TIME, timeView.getUserTime());
				stats.addMetric(OperationMetric.INPUT_ROWS,scanner.getRemoteRowsRead());
				stats.addMetric(OperationMetric.OUTPUT_ROWS,scanner.getRemoteRowsRead());
				stats.addMetric(OperationMetric.START_TIMESTAMP,startTimestamp);
				stats.addMetric(OperationMetric.STOP_TIMESTAMP,stopTimestamp);
				stats.setHostName(SpliceUtils.getHostName());

				SpliceDriver.driver().getTaskReporter().report(xplainSchema,stats);
		}
}
