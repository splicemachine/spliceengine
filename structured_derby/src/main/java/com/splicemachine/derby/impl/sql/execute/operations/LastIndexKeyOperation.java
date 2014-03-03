package com.splicemachine.derby.impl.sql.execute.operations;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.SortedSet;
import java.util.concurrent.ExecutionException;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.io.StoredFormatIds;
import org.apache.derby.iapi.services.loader.GeneratedMethod;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.RowLocation;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.iapi.storage.RowProvider;
import com.splicemachine.derby.impl.storage.ClientScanProvider;
import com.splicemachine.derby.metrics.OperationMetric;
import com.splicemachine.derby.metrics.OperationRuntimeStats;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.derby.utils.marshall.PairDecoder;
import com.splicemachine.derby.utils.marshall.RowMarshaller;
import com.splicemachine.hbase.HBaseRegionCache;
import com.splicemachine.stats.TimeView;
import com.splicemachine.storage.EntryDecoder;
import com.splicemachine.utils.SpliceLogUtils;

public class LastIndexKeyOperation extends ScanOperation{

		private static Logger LOG = Logger.getLogger(LastIndexKeyOperation.class);
		private EntryDecoder rowDecoder;
		private List<KeyValue> keyValues;
		private int[] baseColumnMap;
		protected static List<NodeType> nodeTypes;
		private boolean returnedRow;

		static {
				nodeTypes = Arrays.asList(NodeType.MAP,NodeType.SCAN);
		}

		public LastIndexKeyOperation() {
				super();
		}

		public LastIndexKeyOperation
						(
										Activation 			activation,
										int 				resultSetNumber,
										GeneratedMethod 	resultRowAllocator,
										long 				conglomId,
										String 				tableName,
										String 				userSuppliedOptimizerOverrides,
										String 				indexName,
										int 				colRefItem,
										int 				lockMode,
										boolean				tableLocked,
										int					isolationLevel,
										double				optimizerEstimatedRowCount,
										double 				optimizerEstimatedCost
						) throws StandardException {
				super(conglomId, activation, resultSetNumber, null, -1, null, -1,
								true, null, resultRowAllocator, lockMode, tableLocked, isolationLevel,
								colRefItem, optimizerEstimatedRowCount, optimizerEstimatedCost);
				this.tableName = Long.toString(scanInformation.getConglomerateId());
				this.indexName = indexName;
				init(SpliceOperationContext.newContext(activation));
				returnedRow = false;
				recordConstructorTime();
		}

		@Override
		public void init(SpliceOperationContext context) throws StandardException{
				super.init(context);
				keyValues = new ArrayList<KeyValue>(1);
				this.baseColumnMap = operationInformation.getBaseColumnMap();
				startExecutionTime = System.currentTimeMillis();
		}

		@Override
		public List<SpliceOperation> getSubOperations() {
				return Collections.emptyList();
		}

		@Override
		public List<NodeType> getNodeTypes() {
				return nodeTypes;
		}

		@Override
		public ExecRow nextRow(SpliceRuntimeContext spliceRuntimeContext) throws StandardException,IOException {

				if(timer==null)
						timer = spliceRuntimeContext.newTimer();

				KeyValue [] prev = new KeyValue[2];

				if (returnedRow) {
						currentRow = null;
						currentRowLocation = null;
						stopExecutionTime = System.currentTimeMillis();
				} else {
						timer.startTiming();
						keyValues.clear();
						regionScanner.next(keyValues);

						if (keyValues.isEmpty()) {
								timer.stopTiming();
								stopExecutionTime = System.currentTimeMillis();
								return null;
						}
						while (!keyValues.isEmpty()) {
								keyValues.toArray(prev);
								keyValues.clear();
								rowsFiltered++;
								regionScanner.next(keyValues);
						}

						Collections.addAll(keyValues, prev);

						if(rowDecoder==null)
								rowDecoder = new EntryDecoder(SpliceDriver.getKryoPool());

						currentRow.resetRowArray();
						DataValueDescriptor[] fields = currentRow.getRowArray();

						for(KeyValue kv:keyValues){
								//should only be 1
                            if (kv != null)
								RowMarshaller.sparsePacked().decode(kv,fields,baseColumnMap,rowDecoder);
						}

						if(indexName!=null && currentRow.nColumns() > 0 && currentRow.getColumn(currentRow.nColumns()).getTypeFormatId() == StoredFormatIds.ACCESS_HEAP_ROW_LOCATION_V1_ID){
        			/*
        			 * If indexName !=null, then we are currently scanning an index,
        			 *so our RowLocation should point to the main table, and not to the
        			 * index (that we're actually scanning)
        			 */
								currentRowLocation = (RowLocation) currentRow.getColumn(currentRow.nColumns());
						} else {
								currentRowLocation.setValue(keyValues.get(0).getRow());
						}
						returnedRow = true;
						setCurrentRow(currentRow);
						if(currentRow==null){
								timer.stopTiming();
						}else
								timer.tick(1);

						stopExecutionTime = System.currentTimeMillis();
				}
				return currentRow;
		}

		@Override
		public RowProvider getMapRowProvider(SpliceOperation top,PairDecoder decoder, SpliceRuntimeContext spliceRuntimeContext) throws StandardException {
				SpliceLogUtils.trace(LOG, "getMapRowProvider");
				beginTime = System.currentTimeMillis();

				try {
						// Get last region
						SortedSet<HRegionInfo> regions = HBaseRegionCache.getInstance().getRegions(Bytes.toBytes(tableName));
						HRegionInfo regionInfo = regions.last();
						byte[] startKey = regionInfo.getStartKey();
						byte[] endKey = regionInfo.getEndKey();
						Scan scan = buildScan(spliceRuntimeContext);

						// Modify scan range
						if (startKey.length > 0 && Bytes.compareTo(startKey, scan.getStartRow()) > 0) {
								scan.setStartRow(startKey);
						}

						if (endKey.length > 0 &&
										(scan.getStopRow().length == 0 || Bytes.compareTo(endKey, scan.getStopRow()) < 0)) {
								scan.setStopRow(endKey);
						}
						SpliceUtils.setInstructions(scan, activation, top,spliceRuntimeContext);
						ClientScanProvider provider = new ClientScanProvider("LastIndexKey",Bytes.toBytes(tableName),scan,
										OperationUtils.getPairDecoder(this,spliceRuntimeContext),spliceRuntimeContext);
						nextTime += System.currentTimeMillis() - beginTime;
						return provider;
				} catch (ExecutionException e) {
						SpliceLogUtils.error(LOG,"Unable to get regions",e);
				}
				return null;
		}

		@Override
		public RowProvider getReduceRowProvider(SpliceOperation top, PairDecoder decoder, SpliceRuntimeContext spliceRuntimeContext) throws StandardException {
				return getMapRowProvider(top, decoder, spliceRuntimeContext);
		}

		@Override
		public void readExternal(ObjectInput in) throws IOException,ClassNotFoundException {
				super.readExternal(in);
				tableName = in.readUTF();
				if(in.readBoolean())
						indexName = in.readUTF();
		}

		@Override
		public void writeExternal(ObjectOutput out) throws IOException {
				super.writeExternal(out);
				out.writeUTF(tableName);
				out.writeBoolean(indexName!=null);
				if(indexName!=null)
						out.writeUTF(indexName);
		}

		@Override
		protected int getNumMetrics() {
				return super.getNumMetrics() + 5;
		}

		@Override
		protected void updateStats(OperationRuntimeStats stats) {
				stats.addMetric(OperationMetric.LOCAL_SCAN_ROWS,regionScanner.getRowsOutput());
				stats.addMetric(OperationMetric.LOCAL_SCAN_BYTES,regionScanner.getBytesOutput());
				TimeView localScanTime = regionScanner.getReadTime();
				stats.addMetric(OperationMetric.LOCAL_SCAN_WALL_TIME,localScanTime.getWallClockTime());
				stats.addMetric(OperationMetric.LOCAL_SCAN_CPU_TIME,localScanTime.getCpuTime());
				stats.addMetric(OperationMetric.LOCAL_SCAN_USER_TIME,localScanTime.getUserTime());
				stats.addMetric(OperationMetric.FILTERED_ROWS,rowsFiltered);
		}

		@Override
		public ExecRow getExecRowDefinition() {
				return currentTemplate;
		}

}
