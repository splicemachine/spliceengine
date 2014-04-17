package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.iapi.storage.RowProvider;
import com.splicemachine.derby.impl.sql.execute.operations.scanner.SITableScanner;
import com.splicemachine.derby.impl.sql.execute.operations.scanner.TableScannerBuilder;
import com.splicemachine.derby.impl.storage.ClientScanProvider;
import com.splicemachine.derby.metrics.OperationMetric;
import com.splicemachine.derby.metrics.OperationRuntimeStats;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.derby.utils.StandardSupplier;
import com.splicemachine.derby.utils.marshall.*;
import com.splicemachine.derby.utils.marshall.dvd.DescriptorSerializer;
import com.splicemachine.derby.utils.marshall.dvd.SerializerMap;
import com.splicemachine.derby.utils.marshall.dvd.TypeProvider;
import com.splicemachine.derby.utils.marshall.dvd.VersionedSerializers;
import com.splicemachine.hbase.CellUtils;
import com.splicemachine.stats.TimeView;
import com.splicemachine.utils.ByteSlice;
import com.splicemachine.utils.IntArrays;
import com.splicemachine.utils.Snowflake;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.services.io.StoredFormatIds;
import org.apache.derby.iapi.services.loader.GeneratedMethod;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.store.access.StaticCompiledOpenConglomInfo;
import org.apache.derby.iapi.types.RowLocation;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

public class TableScanOperation extends ScanOperation {
		private static final long serialVersionUID = 3l;

		protected static Logger LOG = Logger.getLogger(TableScanOperation.class);
		protected static List<NodeType> nodeTypes;
		protected int indexColItem;
		public String userSuppliedOptimizerOverrides;
		public int rowsPerRead;
		protected boolean runTimeStatisticsOn;
		private Properties scanProperties;
		public String startPositionString;
		public String stopPositionString;
		public ByteSlice slice;

		static { nodeTypes = Arrays.asList(NodeType.MAP,NodeType.SCAN); }

		private int[] baseColumnMap;

		private Scan scan;

		private SITableScanner tableScanner;

		public TableScanOperation() { super(); }

		public TableScanOperation(ScanInformation scanInformation,
															OperationInformation operationInformation,
															int lockMode,
															int isolationLevel) throws StandardException {
				super(scanInformation, operationInformation, lockMode, isolationLevel);
		}

		@SuppressWarnings("UnusedParameters")
		public  TableScanOperation(long conglomId,
                                   StaticCompiledOpenConglomInfo scoci,
                                   Activation activation,
                                   GeneratedMethod resultRowAllocator,
                                   int resultSetNumber,
                                   GeneratedMethod startKeyGetter, int startSearchOperator,
                                   GeneratedMethod stopKeyGetter, int stopSearchOperator,
                                   boolean sameStartStopPosition,
                                   String qualifiersField,
                                   String tableName,
                                   String userSuppliedOptimizerOverrides,
                                   String indexName,
                                   boolean isConstraint,
                                   boolean forUpdate,
                                   int colRefItem,
                                   int indexColItem,
                                   int lockMode,
                                   boolean tableLocked,
                                   int isolationLevel,
                                   int rowsPerRead,
                                   boolean oneRowScan,
                                   double optimizerEstimatedRowCount,
                                   double optimizerEstimatedCost) throws StandardException {
				super(conglomId, activation, resultSetNumber, startKeyGetter, startSearchOperator, stopKeyGetter, stopSearchOperator,
                        sameStartStopPosition, qualifiersField, resultRowAllocator, lockMode, tableLocked, isolationLevel,
                        colRefItem, optimizerEstimatedRowCount, optimizerEstimatedCost);
				SpliceLogUtils.trace(LOG, "instantiated for tablename %s or indexName %s with conglomerateID %d",
                        tableName, indexName, conglomId);
				this.forUpdate = forUpdate;
				this.isConstraint = isConstraint;
				this.rowsPerRead = rowsPerRead;
				this.tableName = Long.toString(scanInformation.getConglomerateId());
				this.indexColItem = indexColItem;
				this.indexName = indexName;
				runTimeStatisticsOn = operationInformation.isRuntimeStatisticsEnabled();
				if (LOG.isTraceEnabled())
						SpliceLogUtils.trace(LOG, "statisticsTimingOn=%s,isTopResultSet=%s,runTimeStatisticsOn%s",statisticsTimingOn,isTopResultSet,runTimeStatisticsOn);
				init(SpliceOperationContext.newContext(activation));
				recordConstructorTime();
		}

		@Override
		public void readExternal(ObjectInput in) throws IOException,ClassNotFoundException {
				super.readExternal(in);
				tableName = in.readUTF();
				indexColItem = in.readInt();
				if(in.readBoolean())
						indexName = in.readUTF();
		}

		@Override
		public void writeExternal(ObjectOutput out) throws IOException {
				super.writeExternal(out);
				out.writeUTF(tableName);
				out.writeInt(indexColItem);
				out.writeBoolean(indexName != null);
				if(indexName!=null)
						out.writeUTF(indexName);
		}

		@Override
		public void init(SpliceOperationContext context) throws StandardException{
				super.init(context);
				this.baseColumnMap = operationInformation.getBaseColumnMap();
				this.slice = ByteSlice.empty();
				this.startExecutionTime = System.currentTimeMillis();
				this.scan = context.getScan();
		}


		@Override
		public List<SpliceOperation> getSubOperations() {
				return Collections.emptyList();
		}

		@Override
		public RowProvider getMapRowProvider(SpliceOperation top,PairDecoder decoder, SpliceRuntimeContext spliceRuntimeContext) throws StandardException {
				SpliceLogUtils.trace(LOG, "getMapRowProvider");
				beginTime = System.currentTimeMillis();
				Scan scan = getNonSIScan(spliceRuntimeContext);
				SpliceUtils.setInstructions(scan, activation, top,spliceRuntimeContext);
				ClientScanProvider provider = new ClientScanProvider("tableScan",Bytes.toBytes(tableName),scan, decoder,spliceRuntimeContext);
				nextTime += System.currentTimeMillis() - beginTime;
				return provider;
		}


		@Override
		public RowProvider getReduceRowProvider(SpliceOperation top, PairDecoder decoder, SpliceRuntimeContext spliceRuntimeContext, boolean returnDefaultValue) throws StandardException {
				return getMapRowProvider(top, decoder, spliceRuntimeContext);
		}

		@Override
		public KeyEncoder getKeyEncoder(SpliceRuntimeContext spliceRuntimeContext) throws StandardException {

				final Snowflake.Generator generator = SpliceDriver.driver().getUUIDGenerator().newGenerator(128);
				DataHash hash = new SuppliedDataHash(new StandardSupplier<byte[]>() {
						@Override
						public byte[] get() throws StandardException {
								return generator.nextBytes();
						}
				});

				return new KeyEncoder(NoOpPrefix.INSTANCE,hash,NoOpPostfix.INSTANCE);
		}

		@Override
		public DataHash getRowHash(SpliceRuntimeContext spliceRuntimeContext) throws StandardException {
                columnOrdering = scanInformation.getColumnOrdering();
				ExecRow defnRow = getExecRowDefinition();
//				int [] cols = getDecodingColumns(defnRow.nColumns());
				DescriptorSerializer[] serializers = VersionedSerializers.latestVersion(false).getSerializers(defnRow);
				return BareKeyHash.encoder(IntArrays.count(defnRow.nColumns()),null,serializers);
		}

		@Override
		public List<NodeType> getNodeTypes() {
				return nodeTypes;
		}

		@Override
		public ExecRow getExecRowDefinition() {
				return currentTemplate;
		}

		@Override
		public String prettyPrint(int indentLevel) {
				return "Table"+super.prettyPrint(indentLevel);
		}

		@Override protected int getNumMetrics() { return 5; }

		@Override
		protected void updateStats(OperationRuntimeStats stats) {
				if(regionScanner!=null){
						stats.addMetric(OperationMetric.LOCAL_SCAN_ROWS,regionScanner.getRowsVisited());
						stats.addMetric(OperationMetric.LOCAL_SCAN_BYTES,regionScanner.getBytesVisited());
						TimeView readTimer = regionScanner.getReadTime();
						stats.addMetric(OperationMetric.LOCAL_SCAN_CPU_TIME,readTimer.getCpuTime());
						stats.addMetric(OperationMetric.LOCAL_SCAN_USER_TIME,readTimer.getUserTime());
						stats.addMetric(OperationMetric.LOCAL_SCAN_WALL_TIME,readTimer.getWallClockTime());
				}
				if(tableScanner!=null){
						stats.addMetric(OperationMetric.FILTERED_ROWS,tableScanner.getRowsFiltered());
						stats.addMetric(OperationMetric.OUTPUT_ROWS,tableScanner.getRowsVisited());
				}
		}

		@Override
		public ExecRow nextRow(SpliceRuntimeContext spliceRuntimeContext) throws StandardException,IOException {
				if(tableScanner==null){
						tableScanner = new TableScannerBuilder()
										.scanner(regionScanner)
										.transactionID(transactionID)
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

				currentRow = tableScanner.next(spliceRuntimeContext);
				if(currentRow!=null){
						setCurrentRow(currentRow);
						setCurrentRowLocation(tableScanner.getCurrentRowLocation());
				}else{
						clearCurrentRow();
						currentRowLocation = null;
				}

				return currentRow;
		}


		protected void setRowLocation(Cell sampleKv) throws StandardException {
				if(indexName!=null && currentRow.nColumns() > 0 && currentRow.getColumn(currentRow.nColumns()).getTypeFormatId() == StoredFormatIds.ACCESS_HEAP_ROW_LOCATION_V1_ID){
					 /*
						* If indexName !=null, then we are currently scanning an index,
						* so our RowLocation should point to the main table, and not to the
						* index (that we're actually scanning)
						*/
						currentRowLocation = (RowLocation) currentRow.getColumn(currentRow.nColumns());
				} else {
						slice.set(CellUtils.getBuffer(sampleKv), sampleKv.getRowOffset(), sampleKv.getRowLength());
						currentRowLocation.setValue(slice);
				}
		}

		@Override
		public String toString() {
				try {
						return String.format("TableScanOperation {tableName=%s,isKeyed=%b,resultSetNumber=%s}",tableName,scanInformation.isKeyed(),resultSetNumber);
				} catch (Exception e) {
						return String.format("TableScanOperation {tableName=%s,isKeyed=%s,resultSetNumber=%s}", tableName, "UNKNOWN", resultSetNumber);
				}
		}

		@Override
		public void	close() throws StandardException, IOException {
				if(rowDecoder!=null)
						rowDecoder.close();
				SpliceLogUtils.trace(LOG, "close in TableScan");
				beginTime = getCurrentTimeMillis();

				if (runTimeStatisticsOn)
				{
						// This is where we get the scan properties for a subquery
						scanProperties = getScanProperties();
						startPositionString = printStartPosition();
						stopPositionString = printStopPosition();
				}

				if (forUpdate && scanInformation.isKeyed()) {
						activation.clearIndexScanInfo();
				}

				super.close();

				closeTime += getElapsedMillis(beginTime);
		}

		public Properties getScanProperties()
		{
				if (scanProperties == null)
						scanProperties = new Properties();

				scanProperties.setProperty("numPagesVisited", "0");
				scanProperties.setProperty("numRowsVisited", "0");
				scanProperties.setProperty("numRowsQualified", "0");
				scanProperties.setProperty("numColumnsFetched", "0");//FIXME: need to loop through accessedCols to figure out
				try {
						scanProperties.setProperty("columnsFetchedBitSet", ""+scanInformation.getAccessedColumns());
				} catch (StandardException e) {
						SpliceLogUtils.logAndThrowRuntime(LOG,e);
				}
				//treeHeight

				return scanProperties;
		}



		@Override
		public int[] getAccessedNonPkColumns() throws StandardException{
				FormatableBitSet accessedNonPkColumns = scanInformation.getAccessedNonPkColumns();
				int num = accessedNonPkColumns.getNumBitsSet();
				int[] cols = null;
				if (num > 0) {
						cols = new int[num];
						int pos = 0;
						for (int i = accessedNonPkColumns.anySetBit(); i != -1; i = accessedNonPkColumns.anySetBit(i)) {
								cols[pos++] = baseColumnMap[i];
						}
				}
				return cols;
		}
}
