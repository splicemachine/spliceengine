package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.constants.bytes.BytesUtil;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.iapi.storage.RowProvider;
import com.splicemachine.derby.impl.sql.execute.LazyDataValueFactory;
import com.splicemachine.derby.impl.storage.ClientScanProvider;
import com.splicemachine.utils.ByteSlice;
import com.splicemachine.derby.metrics.OperationMetric;
import com.splicemachine.derby.metrics.OperationRuntimeStats;
import com.splicemachine.derby.utils.DerbyBytesUtil;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.derby.utils.StandardSupplier;
import com.splicemachine.derby.utils.marshall.*;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.stats.TimeView;
import com.splicemachine.storage.EntryDecoder;
import com.splicemachine.storage.EntryPredicateFilter;
import com.splicemachine.storage.Predicate;
import com.splicemachine.utils.IntArrays;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.services.io.StoredFormatIds;
import org.apache.derby.iapi.services.loader.GeneratedMethod;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.store.access.StaticCompiledOpenConglomInfo;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.RowLocation;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.*;

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
        private EntryPredicateFilter predicateFilter;
        private boolean cachedPredicateFilter = false;
		static {
				nodeTypes = Arrays.asList(NodeType.MAP,NodeType.SCAN);
		}

		private EntryDecoder rowDecoder;
        private MultiFieldDecoder keyDecoder;
        private KeyMarshaller keyMarshaller;
		private int[] baseColumnMap;
        private DataValueDescriptor[] kdvds;

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
		}

		@Override
		public List<SpliceOperation> getSubOperations() {
				return Collections.emptyList();
		}

		@Override
		public RowProvider getMapRowProvider(SpliceOperation top,PairDecoder decoder, SpliceRuntimeContext spliceRuntimeContext) throws StandardException {
				SpliceLogUtils.trace(LOG, "getMapRowProvider");
				beginTime = System.currentTimeMillis();
				Scan scan = buildScan(spliceRuntimeContext);
				SpliceUtils.setInstructions(scan, activation, top,spliceRuntimeContext);
				ClientScanProvider provider = new ClientScanProvider("tableScan",Bytes.toBytes(tableName),scan, decoder,spliceRuntimeContext);
				nextTime += System.currentTimeMillis() - beginTime;
				return provider;
		}

		@Override
		public RowProvider getReduceRowProvider(SpliceOperation top, PairDecoder decoder, SpliceRuntimeContext spliceRuntimeContext) throws StandardException {
				return getMapRowProvider(top, decoder, spliceRuntimeContext);
		}

		@Override
		public KeyEncoder getKeyEncoder(SpliceRuntimeContext spliceRuntimeContext) throws StandardException {
            /*FormatableBitSet accessedPkCols = scanInformation.getAccessedPkColumns();
            int size = accessedPkCols.getNumBitsSet();*/
            columnOrdering = scanInformation.getColumnOrdering();

            int[] keyColumns = new int[columnOrdering.length];
            for (int i = 0; i < keyColumns.length; ++i) {
                keyColumns[i] = baseColumnMap[columnOrdering[i]];
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
            /*HashPrefix prefix;
            DataHash dataHash;
            KeyPostfix postfix = NoOpPostfix.INSTANCE;

            FormatableBitSet accessedPkCols = scanInformation.getAccessedPkColumns();
            int size = accessedPkCols.getNumBitsSet();
            columnOrdering = scanInformation.getColumnOrdering();
            if(size == 0){
                prefix = new SaltedPrefix(operationInformation.getUUIDGenerator());
                dataHash = NoOpDataHash.INSTANCE;
            }else{
                int[] keyColumns = new int[columnOrdering.length];
                for (int i = 0; i < keyColumns.length; ++i) {
                    keyColumns[i] = baseColumnMap[columnOrdering[i]];
                }
            }

            return new KeyEncoder(prefix,dataHash,postfix);*/
		}

        private int[] getDecodingColumns(int n) {

            int[] columns = new int[baseColumnMap.length];
            for (int i = 0; i < columns.length; ++i)
                columns[i] = baseColumnMap[i];
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
						long bytesRead = regionScanner.getBytesOutput();
						stats.addMetric(OperationMetric.LOCAL_SCAN_ROWS,regionScanner.getRowsVisited());
						stats.addMetric(OperationMetric.LOCAL_SCAN_BYTES,regionScanner.getBytesVisited());
						stats.addMetric(OperationMetric.LOCAL_SCAN_CPU_TIME,readTimer.getCpuTime());
						stats.addMetric(OperationMetric.LOCAL_SCAN_USER_TIME,readTimer.getUserTime());
						stats.addMetric(OperationMetric.LOCAL_SCAN_WALL_TIME,readTimer.getWallClockTime());
                        stats.addMetric(OperationMetric.FILTERED_ROWS,regionScanner.getRowsFiltered());
				}
		}

        private EntryPredicateFilter getPredicateFilter(SpliceRuntimeContext spliceRuntimeContext) throws StandardException,IOException{
            if (!cachedPredicateFilter) {
                Scan scan = getScan(spliceRuntimeContext);
                predicateFilter = EntryPredicateFilter.fromBytes(scan.getAttribute(SpliceConstants.ENTRY_PREDICATE_LABEL));
                cachedPredicateFilter = true;
            }
            return predicateFilter;
        }
		@Override
		public ExecRow nextRow(SpliceRuntimeContext spliceRuntimeContext) throws StandardException,IOException {
				if(timer==null)
						timer = spliceRuntimeContext.newTimer();

				timer.startTiming();

                KeyValue keyValue = null;
                do{
                    keyValue = regionScanner.next();

                    if (keyValue == null) {
                        if (LOG.isTraceEnabled())
                            SpliceLogUtils.trace(LOG,"%s:no more data retrieved from table",tableName);
                        currentRow = null;
                        currentRowLocation = null;
                    } else {

                        if(rowDecoder==null)
                            rowDecoder = new EntryDecoder(SpliceDriver.getKryoPool());
                        currentRow.resetRowArray();
                        DataValueDescriptor[] fields = currentRow.getRowArray();
                        if (fields.length != 0) {
                            // Apply predicate to the row key first
                            if (getColumnOrdering() != null && getPredicateFilter(spliceRuntimeContext) != null) {
                                boolean passed = filterRow(keyValue.getRow());
                                if (!passed)
                                    continue;
                            }
                            // Decode row data
                            RowMarshaller.sparsePacked().decode(keyValue,fields,baseColumnMap,rowDecoder);
                            // Decode key data if primary key is accessed
                            if (scanInformation.getAccessedPkColumns() != null && scanInformation.getAccessedPkColumns().getNumBitsSet() > 0) {
                                getKeyMarshaller().decode(keyValue, fields, baseColumnMap, getKeyDecoder(), columnOrdering, kdvds);
                            }
                        }
                        if(indexName!=null && currentRow.nColumns() > 0 && currentRow.getColumn(currentRow.nColumns()).getTypeFormatId() == StoredFormatIds.ACCESS_HEAP_ROW_LOCATION_V1_ID){
                            /*
                             * If indexName !=null, then we are currently scanning an index,
                             * so our RowLocation should point to the main table, and not to the
                             * index (that we're actually scanning)
                             */
                            currentRowLocation = (RowLocation) currentRow.getColumn(currentRow.nColumns());
                        } else {
                            slice.set(keyValue.getBuffer(), keyValue.getRowOffset(), keyValue.getRowLength());
                            currentRowLocation.setValue(slice);
                        }
                        break;
                    }
                } while (keyValue != null);
				setCurrentRow(currentRow);

				//measure time
				if(currentRow==null){
						timer.tick(0);
						stopExecutionTime = System.currentTimeMillis();
				}else
						timer.tick(1);
				return currentRow;
		}

        private boolean filterRow(byte[] data) throws StandardException{
            getColumnDVDs();
            MultiFieldDecoder keyDecoder = getKeyDecoder();
            keyDecoder.set(data);
            for (int i = 0; i < kdvds.length; ++i) {
                int offset = keyDecoder.offset();
                DerbyBytesUtil.skip(keyDecoder,kdvds[i]);
                int size = keyDecoder.offset()-1-offset;
                Object[] buffer = predicateFilter.getValuePredicates().buffer;
                int ibuffer = predicateFilter.getValuePredicates().size();
                for (int j =0; j<ibuffer; j++) {
                    if(((Predicate)buffer[j]).applies(i) && !((Predicate)buffer[j]).match(i,data,offset,size))
                        return false;
                }
            }

            return true;
        }

        private MultiFieldDecoder getKeyDecoder() {
            if (keyDecoder == null)
                keyDecoder = MultiFieldDecoder.create(SpliceDriver.getKryoPool());
            return keyDecoder;
        }

        private KeyMarshaller getKeyMarshaller () {
            if (keyMarshaller == null)
                keyMarshaller = new KeyMarshaller();

            return keyMarshaller;
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

        protected int[] getColumnOrdering() throws StandardException{
            if (columnOrdering == null) {
                columnOrdering = scanInformation.getColumnOrdering();
            }
            return columnOrdering;
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

        private void getColumnDVDs() throws StandardException{
            if (kdvds == null) {
                int[] columnOrdering = getColumnOrdering();
                int[] format_ids = scanInformation.getConglomerate().getFormat_ids();
                kdvds = new DataValueDescriptor[columnOrdering.length];
                for (int i = 0; i < columnOrdering.length; ++i) {
                    kdvds[i] = LazyDataValueFactory.getLazyNull(format_ids[columnOrdering[i]]);
                }
            }
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
            DataValueDescriptor[] fields = destination.getRowArray();
            for (int i = 0; i < keyColumns.length; ++i) {
                if (keyColumns[i] == -1) {
                    // skip the key columns that are not in the result
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
