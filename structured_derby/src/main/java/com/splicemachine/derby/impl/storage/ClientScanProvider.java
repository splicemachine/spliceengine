package com.splicemachine.derby.impl.storage;

import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.impl.store.access.SpliceAccessManager;
import com.splicemachine.derby.metrics.OperationMetric;
import com.splicemachine.derby.metrics.OperationRuntimeStats;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.derby.utils.marshall.PairDecoder;
import com.splicemachine.stats.TimeView;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.derby.iapi.error.StandardException;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * RowProvider which uses an HBase client ResultScanner to
 * pull rows in serially.
 *
 * @author Scott Fines
 * Date Created: 1/17/13:1:23 PM
 */
public class ClientScanProvider extends AbstractScanProvider {
		private static final Logger LOG = Logger.getLogger(ClientScanProvider.class);
		private final byte[] tableName;
		private HTableInterface htable;
		private final Scan scan;
		private SpliceResultScanner scanner;
		private long stopExecutionTime;
		private long startExecutionTime;

		public ClientScanProvider(String type,
															byte[] tableName, Scan scan,
															PairDecoder decoder, SpliceRuntimeContext spliceRuntimeContext) {
				super(decoder, type, spliceRuntimeContext);
				SpliceLogUtils.trace(LOG, "instantiated");
				this.tableName = tableName;
				this.scan = scan;
		}

		@Override
		public Result getResult() throws StandardException, IOException {
				return scanner.next();
		}

		@Override
		public void open() {
				SpliceLogUtils.trace(LOG, "open");
				if(htable==null)
						htable = SpliceAccessManager.getHTable(tableName);
				try {
						scanner = new MeasuredResultScanner(htable, scan, htable.getScanner(scan),spliceRuntimeContext);
				} catch (IOException e) {
						SpliceLogUtils.logAndThrowRuntime(LOG,"unable to open table "+ Bytes.toString(tableName),e);
				}
				startExecutionTime = System.currentTimeMillis();
		}

		@Override
		public void close() throws StandardException {
				super.close();
				SpliceLogUtils.trace(LOG, "closed after calling hasNext %d times",called);
				if(scanner!=null)scanner.close();
				if(htable!=null)
						try {
								htable.close();
						} catch (IOException e) {
								SpliceLogUtils.logAndThrowRuntime(LOG,"unable to close htable for "+ Bytes.toString(tableName),e);
						}
				stopExecutionTime = System.currentTimeMillis();
		}

		@Override
		public Scan toScan() {
				return scan;
		}

		@Override
		public byte[] getTableName() {
				return tableName;
		}

		@Override
		public void reportStats(long statementId, long operationId, long taskId, String xplainSchema,String regionName) {
				if(regionName==null)
						regionName="ControlRegion";
				OperationRuntimeStats stats = new OperationRuntimeStats(statementId,operationId,taskId,regionName,8);
				stats.addMetric(OperationMetric.REMOTE_SCAN_BYTES,scanner.getRemoteBytesRead());
				stats.addMetric(OperationMetric.REMOTE_SCAN_ROWS,scanner.getRemoteRowsRead());
				TimeView remoteView = scanner.getRemoteReadTime();
				stats.addMetric(OperationMetric.REMOTE_SCAN_WALL_TIME,remoteView.getWallClockTime());
				stats.addMetric(OperationMetric.REMOTE_SCAN_CPU_TIME,remoteView.getCpuTime());
				stats.addMetric(OperationMetric.REMOTE_SCAN_USER_TIME,remoteView.getUserTime());
				stats.addMetric(OperationMetric.TOTAL_WALL_TIME,remoteView.getWallClockTime());
				stats.addMetric(OperationMetric.TOTAL_CPU_TIME,remoteView.getCpuTime());
				stats.addMetric(OperationMetric.TOTAL_USER_TIME,remoteView.getUserTime());
				stats.addMetric(OperationMetric.OUTPUT_ROWS,scanner.getRemoteRowsRead());
				stats.addMetric(OperationMetric.INPUT_ROWS,scanner.getRemoteRowsRead());
				stats.addMetric(OperationMetric.START_TIMESTAMP,startExecutionTime);
				stats.addMetric(OperationMetric.STOP_TIMESTAMP,stopExecutionTime);

				SpliceDriver.driver().getTaskReporter().report(xplainSchema,stats);
		}

}
