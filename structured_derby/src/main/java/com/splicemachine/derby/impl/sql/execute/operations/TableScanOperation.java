package com.splicemachine.derby.impl.sql.execute.operations;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Properties;

import com.google.common.collect.Lists;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.services.io.StoredFormatIds;
import org.apache.derby.iapi.services.loader.GeneratedMethod;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.store.access.StaticCompiledOpenConglomInfo;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.RowLocation;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import com.splicemachine.constants.SIConstants;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.iapi.storage.RowProvider;
import com.splicemachine.derby.impl.storage.ClientScanProvider;
import com.splicemachine.derby.impl.store.ExecRowAccumulator;
import com.splicemachine.derby.metrics.OperationMetric;
import com.splicemachine.derby.metrics.OperationRuntimeStats;
import com.splicemachine.derby.utils.DerbyBytesUtil;
import com.splicemachine.derby.utils.EntryPredicateUtils;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.derby.utils.StandardSupplier;
import com.splicemachine.derby.utils.marshall.BareKeyHash;
import com.splicemachine.derby.utils.marshall.DataHash;
import com.splicemachine.derby.utils.marshall.KeyEncoder;
import com.splicemachine.derby.utils.marshall.KeyHashDecoder;
import com.splicemachine.derby.utils.marshall.KeyMarshaller;
import com.splicemachine.derby.utils.marshall.NoOpPostfix;
import com.splicemachine.derby.utils.marshall.NoOpPrefix;
import com.splicemachine.derby.utils.marshall.PairDecoder;
import com.splicemachine.derby.utils.marshall.SuppliedDataHash;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.hbase.CellUtils;
import com.splicemachine.si.api.HTransactorFactory;
import com.splicemachine.si.data.hbase.HRowAccumulator;
import com.splicemachine.si.impl.FilterState;
import com.splicemachine.si.impl.FilterStatePacked;
import com.splicemachine.si.impl.IFilterState;
import com.splicemachine.si.impl.TransactionId;
import com.splicemachine.stats.Counter;
import com.splicemachine.stats.TimeView;
import com.splicemachine.storage.EntryPredicateFilter;
import com.splicemachine.utils.ByteSlice;
import com.splicemachine.utils.SpliceLogUtils;

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
		private int[] keyColumns;

		private List<Cell> keyValues;
		private FilterStatePacked siFilter;
		private Scan scan;

		private Counter filterCount;

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

		protected Scan getNonSIScan(SpliceRuntimeContext spliceRuntimeContext) {
				/*
				 * Intended to get a scan which does NOT set up SI underneath us (since
				 * we are doing it ourselves).
				 */
				Scan scan = buildScan(spliceRuntimeContext);
				deSiify(scan);
				return scan;
		}

		protected void deSiify(Scan scan) {
				/*
				 * Remove SI-specific behaviors from the scan, so that we can handle it ourselves correctly.
				 */
				//exclude this from SI treatment, since we're doing it internally
				scan.setAttribute(SIConstants.SI_NEEDED,null);
				scan.setMaxVersions();
				Map<byte[], NavigableSet<byte[]>> familyMap = scan.getFamilyMap();
				if(familyMap!=null){
						NavigableSet<byte[]> bytes = familyMap.get(SpliceConstants.DEFAULT_FAMILY_BYTES);
						if(bytes!=null)
								bytes.clear(); //make sure we get all columns
				}
		}

		@Override
		public RowProvider getReduceRowProvider(SpliceOperation top, PairDecoder decoder, SpliceRuntimeContext spliceRuntimeContext, boolean returnDefaultValue) throws StandardException {
				return getMapRowProvider(top, decoder, spliceRuntimeContext);
		}

		@Override
		public KeyEncoder getKeyEncoder(SpliceRuntimeContext spliceRuntimeContext) throws StandardException {

            columnOrdering = scanInformation.getColumnOrdering();
            if(columnOrdering != null && columnOrdering.length > 0) {
								getKeyColumns();
            }
		    /*
			 * Table Scans only use a key encoder when encoding to SpliceOperationRegionScanner,
			 * in which case, the KeyEncoder should be either the row location of the last emitted
		     * row, or a random field (if no row location is specified).
			 */
            DataHash hash = new KeyDataHash(new StandardSupplier<byte[]>() {
                @Override
                public byte[] get() throws StandardException {
                    if(currentRowLocation!=null)
                        return currentRowLocation.getBytes();
                    return SpliceDriver.driver().getUUIDGenerator().nextUUIDBytes();
                }
            }, keyColumns, scanInformation.getKeyColumnDVDs());

            return new KeyEncoder(NoOpPrefix.INSTANCE,hash,NoOpPostfix.INSTANCE);
		}

		protected int[] getKeyColumns() throws StandardException {
				if(keyColumns==null){
						columnOrdering = scanInformation.getColumnOrdering();
						keyColumns = new int[columnOrdering.length];
						for (int i = 0; i < keyColumns.length; ++i) {
								keyColumns[i] = -1;
						}
						for (int i = 0; i < keyColumns.length && columnOrdering[i] < baseColumnMap.length; ++i) {
								keyColumns[i] = baseColumnMap[columnOrdering[i]];
						}
				}
				return keyColumns;
		}

		private int[] getDecodingColumns(int n) {
				int[] columns = new int[baseColumnMap.length];
				System.arraycopy(baseColumnMap, 0, columns, 0, columns.length);
				// Skip primary key columns to save space
				for(int pkCol:columnOrdering) {
						if (pkCol < n)
								columns[pkCol] = -1;
				}

				return columns;
		}

		@Override
		public DataHash getRowHash(SpliceRuntimeContext spliceRuntimeContext) throws StandardException {
                columnOrdering = scanInformation.getColumnOrdering();
				ExecRow defnRow = getExecRowDefinition();
				int [] cols = getDecodingColumns(defnRow.nColumns());
				return BareKeyHash.encoder(cols,null);
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
						TimeView readTimer = regionScanner.getReadTime();
//						long bytesRead = regionScanner.getBytesOutput();
						stats.addMetric(OperationMetric.LOCAL_SCAN_ROWS,regionScanner.getRowsVisited());
						stats.addMetric(OperationMetric.LOCAL_SCAN_BYTES,regionScanner.getBytesVisited());
						stats.addMetric(OperationMetric.LOCAL_SCAN_CPU_TIME,readTimer.getCpuTime());
						stats.addMetric(OperationMetric.LOCAL_SCAN_USER_TIME,readTimer.getUserTime());
						stats.addMetric(OperationMetric.LOCAL_SCAN_WALL_TIME,readTimer.getWallClockTime());
						long filtered = regionScanner.getRowsFiltered();
						if(filterCount !=null)
								filtered+= filterCount.getTotal();
						stats.addMetric(OperationMetric.FILTERED_ROWS,filtered);
				}
		}

		@Override
		public ExecRow nextRow(SpliceRuntimeContext spliceRuntimeContext) throws StandardException,IOException {
				if(timer==null){
						timer = spliceRuntimeContext.newTimer();
						filterCount = spliceRuntimeContext.newCounter();
				}

				timer.startTiming();

				int[] columnOrder = getColumnOrdering();
				KeyMarshaller marshaller = getKeyMarshaller();
				MultiFieldDecoder keyDecoder = getKeyDecoder();
				if(keyValues==null)
						keyValues = Lists.newArrayListWithExpectedSize(2);

				FilterStatePacked filter = getSIFilter();
				boolean hasRow;
				do{
						keyValues.clear();
						hasRow = regionScanner.next(keyValues);

						if (keyValues.size()<=0) {
								if (LOG.isTraceEnabled())
										SpliceLogUtils.trace(LOG,"%s:no more data retrieved from table",tableName);
								currentRow = null;
								currentRowLocation = null;
						} else {
								currentRow.resetRowArray();
								DataValueDescriptor[] fields = currentRow.getRowArray();
								if (fields.length != 0) {
										// Apply predicate to the row key first
										if (!filterRowKey(spliceRuntimeContext, keyValues.get(0), columnOrder, keyDecoder)||!filterRow(filter)){
												filterCount.increment();
												continue;
										}

										// Decode key data if primary key is accessed
										if (scanInformation.getAccessedPkColumns() != null &&
														scanInformation.getAccessedPkColumns().getNumBitsSet() > 0) {
												marshaller.decode(keyValues.get(0), fields, baseColumnMap, keyDecoder, columnOrder, getColumnDVDs());
										}
								}else if(!filterRow(filter)){
										//still need to filter rows to deal with transactional issue
										filterCount.increment();
										continue;
								}
								setRowLocation(keyValues.get(0));
								break;
						}
				}while(hasRow);
				if(!hasRow){
						clearCurrentRow();
				}else{
						setCurrentRow(currentRow);
				}

				//measure time
				if(currentRow==null){
						timer.tick(0);
						stopExecutionTime = System.currentTimeMillis();
				}else
						timer.tick(1);
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

		protected boolean filterRow(FilterStatePacked filter) throws IOException {
				filter.nextRow();
				Iterator<Cell> kvIter = keyValues.iterator();
				while(kvIter.hasNext()){
						Cell kv = kvIter.next();
						Filter.ReturnCode returnCode = filter.filterCell(kv);
						switch(returnCode){
								case NEXT_COL:
								case NEXT_ROW:
								case SEEK_NEXT_USING_HINT:
										return false; //failed the predicate
								case SKIP:
										kvIter.remove();
								default:
										//these are okay--they mean the encoding is good
						}
				}
				return filter.getAccumulator().result() != null && keyValues.size()>0;
		}

		@SuppressWarnings("unchecked")
		private FilterStatePacked getSIFilter() throws IOException, StandardException {
				if(siFilter==null){
						boolean isCountStar = scan.getAttribute(SIConstants.SI_COUNT_STAR)!=null;
						EntryPredicateFilter epf = decodePredicateFilter();
						TransactionId txnId= new TransactionId(this.transactionID);
						IFilterState iFilterState = HTransactorFactory.getTransactionReadController().newFilterState(null, txnId);
						ExecRowAccumulator rowAccumulator = ExecRowAccumulator.newAccumulator(epf,false,currentRow, baseColumnMap);

						HRowAccumulator accumulator = new HRowAccumulator(epf, getRowDecoder(), rowAccumulator, isCountStar);
						siFilter = new FilterStatePacked((FilterState)iFilterState, accumulator){
//								@Override protected Filter.ReturnCode skipRow() { return Filter.ReturnCode.NEXT_COL; }

								@Override
								protected Filter.ReturnCode doAccumulate(Cell dataKeyValue) throws IOException {
										if (!accumulator.isFinished() && accumulator.isOfInterest(dataKeyValue)) {
												if (!accumulator.accumulate(dataKeyValue)) {
														return Filter.ReturnCode.NEXT_ROW;
												}
												return Filter.ReturnCode.INCLUDE;
										}else return Filter.ReturnCode.INCLUDE;
								}
						};
				}
				return siFilter;
		}

		private EntryPredicateFilter decodePredicateFilter() throws IOException {
				return EntryPredicateFilter.fromBytes(scan.getAttribute(SpliceConstants.ENTRY_PREDICATE_LABEL));
		}


		protected boolean filterRowKey(SpliceRuntimeContext spliceRuntimeContext,
																	 Cell keyValue,
																	 int[] columnOrder,
																	 MultiFieldDecoder keyDecoder) throws StandardException, IOException {
				return columnOrder != null &&
								getPredicateFilter(spliceRuntimeContext) != null &&
								EntryPredicateUtils.qualify(predicateFilter, keyValue.getRowArray(),keyValue.getRowOffset(), keyValue.getRowLength(),
												getColumnDVDs(), columnOrder, keyDecoder);
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

    private class KeyDataHash extends SuppliedDataHash {

        private int[] keyColumns;
        private DataValueDescriptor[] kdvds;

        public KeyDataHash(StandardSupplier<byte[]> supplier, int[] keyColumns, DataValueDescriptor[] kdvds) {
            super(supplier);
            this.keyColumns = keyColumns;
            this.kdvds = kdvds;
        }

        @Override
        public KeyHashDecoder getDecoder() {
            return new SuppliedKeyHashDecoder(keyColumns, kdvds);
        }

    }
    private class SuppliedKeyHashDecoder implements KeyHashDecoder {

        private int[] keyColumns;
        private DataValueDescriptor[] kdvds;
        MultiFieldDecoder decoder;

        public SuppliedKeyHashDecoder(int[] keyColumns, DataValueDescriptor[] kdvds) {
            this.keyColumns = keyColumns;
            this.kdvds = kdvds;
        }

        @Override
        public void set(byte[] bytes, int hashOffset, int length){
            if (decoder == null)
                decoder = MultiFieldDecoder.create(SpliceDriver.getKryoPool());
            decoder.set(bytes, hashOffset, length);
        }

        @Override
        public void decode(ExecRow destination) throws StandardException {
             unpack(decoder, destination);
        }

        private void unpack(MultiFieldDecoder decoder, ExecRow destination) throws StandardException {
            if (keyColumns == null) return;
            DataValueDescriptor[] fields = destination.getRowArray();
            for (int i = 0; i < keyColumns.length; ++i) {
                if (keyColumns[i] == -1) {
                    // skip the key columns that are not in the result
                    if(kdvds[i] != null && i!=keyColumns.length-1)
                        DerbyBytesUtil.skip(decoder, kdvds[i]);
                }
                else {
                    DataValueDescriptor field = fields[keyColumns[i]];
                    decodeNext(decoder, field);
                }
            }
        }
        void decodeNext(MultiFieldDecoder decoder, DataValueDescriptor field) throws StandardException {
            if(DerbyBytesUtil.isNextFieldNull(decoder, field)){
                field.setToNull();
                DerbyBytesUtil.skip(decoder, field);
            }else
                DerbyBytesUtil.decodeInto(decoder,field);
        }
    }
}
