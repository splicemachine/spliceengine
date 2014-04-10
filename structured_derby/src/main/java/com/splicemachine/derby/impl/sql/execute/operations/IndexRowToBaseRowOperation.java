package com.splicemachine.derby.impl.sql.execute.operations;


import com.google.common.base.Strings;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.iapi.sql.execute.SpliceNoPutResultSet;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.iapi.storage.RowProvider;
import com.splicemachine.derby.impl.SpliceMethod;
import com.splicemachine.derby.impl.sql.execute.LazyDataValueFactory;
import com.splicemachine.derby.impl.store.access.SpliceTransactionManager;
import com.splicemachine.derby.impl.store.access.base.SpliceConglomerate;
import com.splicemachine.derby.impl.store.access.hbase.HBaseRowLocation;
import com.splicemachine.derby.metrics.OperationMetric;
import com.splicemachine.derby.metrics.OperationRuntimeStats;
import com.splicemachine.derby.utils.StandardSupplier;
import com.splicemachine.derby.utils.marshall.*;
import com.splicemachine.derby.utils.marshall.dvd.DescriptorSerializer;
import com.splicemachine.derby.utils.marshall.dvd.VersionedSerializers;
import com.splicemachine.stats.TimeView;
import com.splicemachine.utils.SpliceLogUtils;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.Timer;
import org.apache.derby.catalog.UUID;
import org.apache.derby.catalog.types.ReferencedColumnsDescriptorImpl;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.services.loader.GeneratedMethod;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.TableDescriptor;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.store.access.DynamicCompiledOpenConglomInfo;
import org.apache.derby.iapi.store.access.StaticCompiledOpenConglomInfo;
import org.apache.derby.iapi.store.access.TransactionController;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.RowLocation;
import org.apache.derby.impl.sql.GenericPreparedStatement;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
/**
 * Maps between an Index Table and a data Table.
 */
public class IndexRowToBaseRowOperation extends SpliceBaseOperation{
		private static Logger LOG = Logger.getLogger(IndexRowToBaseRowOperation.class);
		protected int lockMode;
		protected int isolationLevel;
		protected FormatableBitSet accessedCols;
		protected String resultRowAllocatorMethodName;
		protected StaticCompiledOpenConglomInfo scoci;
		protected DynamicCompiledOpenConglomInfo dcoci;
		protected SpliceOperation source;
		protected String indexName;
		protected boolean forUpdate;
		protected SpliceMethod<DataValueDescriptor> restriction;
		protected String restrictionMethodName;
		protected FormatableBitSet accessedHeapCols;
		protected FormatableBitSet heapOnlyCols;
		protected FormatableBitSet accessedAllCols;
		protected int[] indexCols;
		protected ExecRow resultRow;
		protected DataValueDescriptor[]	rowArray;
		protected int scociItem;
		protected long conglomId;
		protected int heapColRefItem;
		protected int allColRefItem;
		protected int heapOnlyColRefItem;
		protected int indexColMapItem;
		private ExecRow compactRow;
		RowLocation baseRowLocation = null;
		boolean copiedFromSource = false;
        int[] columnOrdering;
        int[] format_ids;
        DataValueDescriptor[] kdvds;
        SpliceConglomerate conglomerate;
		/*
			* Variable here to stash pre-generated DataValue definitions for use in
			* getExecRowDefinition(). Save a little bit of performance by caching it
			* once created.
			*/
		private int[] adjustedBaseColumnMap;

		private static final MetricName scanName = new MetricName("com.splicemachine.operations","indexLookup","totalTime");
		private final Timer totalTimer = SpliceDriver.driver().getRegistry().newTimer(scanName,TimeUnit.MILLISECONDS,TimeUnit.SECONDS);
		private IndexRowReader reader;
		private String mainTableVersion = "1.0"; //TODO -sf- make this configured


		public IndexRowToBaseRowOperation () {
				super();
		}

		public IndexRowToBaseRowOperation(long conglomId, int scociItem,
																			Activation activation, SpliceOperation source,
																			GeneratedMethod resultRowAllocator, int resultSetNumber,
																			String indexName, int heapColRefItem, int allColRefItem,
																			int heapOnlyColRefItem, int indexColMapItem,
																			GeneratedMethod restriction, boolean forUpdate,
																			double optimizerEstimatedRowCount, double optimizerEstimatedCost) throws StandardException {
				super(activation, resultSetNumber, optimizerEstimatedRowCount, optimizerEstimatedCost);
				SpliceLogUtils.trace(LOG,"instantiate with parameters");
				this.resultRowAllocatorMethodName = resultRowAllocator.getMethodName();
				this.source = source;
				this.indexName = indexName;
				this.forUpdate = forUpdate;
				this.scociItem = scociItem;
				this.conglomId = conglomId;
				this.heapColRefItem = heapColRefItem;
				this.allColRefItem = allColRefItem;
				this.heapOnlyColRefItem = heapOnlyColRefItem;
				this.indexColMapItem = indexColMapItem;
				this.restrictionMethodName = restriction==null? null: restriction.getMethodName();
				init(SpliceOperationContext.newContext(activation));
				recordConstructorTime();
                getKeyColumnDVDs();
		}

		@Override
		public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
				super.readExternal(in);
				scociItem = in.readInt();
				conglomId = in.readLong();
				heapColRefItem = in.readInt();
				allColRefItem = in.readInt();
				heapOnlyColRefItem = in.readInt();
				indexColMapItem = in.readInt();
				source = (SpliceOperation) in.readObject();
				accessedCols = (FormatableBitSet) in.readObject();
				resultRowAllocatorMethodName = in.readUTF();
				indexName = in.readUTF();
				restrictionMethodName = readNullableString(in);
		}

		@Override
		public void writeExternal(ObjectOutput out) throws IOException {
				super.writeExternal(out);
				out.writeInt(scociItem);
				out.writeLong(conglomId);
				out.writeInt(heapColRefItem);
				out.writeInt(allColRefItem);
				out.writeInt(heapOnlyColRefItem);
				out.writeInt(indexColMapItem);
				out.writeObject(source);
				out.writeObject(accessedCols);
				out.writeUTF(resultRowAllocatorMethodName);
				out.writeUTF(indexName);
				writeNullableString(restrictionMethodName, out);
		}

		@Override
		public void init(SpliceOperationContext context) throws StandardException{
				super.init(context);
				source.init(context);
				try {
						if(restrictionMethodName !=null){
								SpliceLogUtils.trace(LOG,"%s:restrictionMethodName=%s",indexName,restrictionMethodName);
								restriction = new SpliceMethod<DataValueDescriptor>(restrictionMethodName,activation);
						}
						SpliceMethod<ExecRow> generatedMethod = new SpliceMethod<ExecRow>(resultRowAllocatorMethodName,activation);
						final GenericPreparedStatement gp = (GenericPreparedStatement)activation.getPreparedStatement();
						final Object[] saved = gp.getSavedObjects();
						scoci = (StaticCompiledOpenConglomInfo)saved[scociItem];
						TransactionController tc = activation.getTransactionController();
						dcoci = tc.getDynamicCompiledConglomInfo(conglomId);
						// the saved objects, if it exists
						if (heapColRefItem != -1) {
								this.accessedHeapCols = (FormatableBitSet)saved[heapColRefItem];
						}
						if (allColRefItem != -1) {
								this.accessedAllCols = (FormatableBitSet)saved[allColRefItem];
						}

						// retrieve the array of columns coming from the index
						indexCols = ((ReferencedColumnsDescriptorImpl) saved[indexColMapItem]).getReferencedColumnPositions();
			/* Get the result row template */
						resultRow = generatedMethod.invoke();

						compactRow = operationInformation.compactRow(resultRow, accessedAllCols, false);

						int[] baseColumnMap = operationInformation.getBaseColumnMap();
						if(heapOnlyColRefItem!=-1){
								this.heapOnlyCols = (FormatableBitSet)saved[heapOnlyColRefItem];
								adjustedBaseColumnMap = new int[heapOnlyCols.getNumBitsSet()];
								int pos=0;
								for(int i=heapOnlyCols.anySetBit();i>=0;i=heapOnlyCols.anySetBit(i)){
										adjustedBaseColumnMap[pos] = baseColumnMap[i];
										pos++;
								}
						}

						if (accessedHeapCols == null) {
								rowArray = resultRow.getRowArray();

						}
						else {
								// Figure out how many columns are coming from the heap
								final DataValueDescriptor[] resultRowArray = resultRow.getRowArray();
								final int heapOnlyLen = heapOnlyCols.getLength();

								// Need a separate DataValueDescriptor array in this case
								rowArray = new DataValueDescriptor[heapOnlyLen];
								final int minLen = Math.min(resultRowArray.length, heapOnlyLen);

								// Make a copy of the relevant part of rowArray
								for (int i = 0; i < minLen; ++i) {
										if (resultRowArray[i] != null && heapOnlyCols.isSet(i)) {
												rowArray[i] = resultRowArray[i];
										}
								}
								if (indexCols != null) {
										for (int index = 0; index < indexCols.length; index++) {
												if (indexCols[index] != -1) {
														compactRow.setColumn(index + 1,source.getExecRowDefinition().getColumn(indexCols[index] + 1));
												}
										}
								}
						}
						SpliceLogUtils.trace(LOG,"accessedAllCols=%s,accessedHeapCols=%s,heapOnlyCols=%s,accessedCols=%s",accessedAllCols,accessedHeapCols,heapOnlyCols,accessedCols);
						SpliceLogUtils.trace(LOG,"rowArray=%s,compactRow=%s,resultRow=%s,resultSetNumber=%d",
										Arrays.asList(rowArray),compactRow,resultRow,resultSetNumber);

						//get the mainTable version
						DataDictionary dataDictionary = activation.getLanguageConnectionContext().getDataDictionary();
						UUID tableID = dataDictionary.getConglomerateDescriptor(conglomId).getTableID();
						TableDescriptor td = dataDictionary.getTableDescriptor(tableID);
						mainTableVersion = td.getVersion();
				} catch (StandardException e) {
						SpliceLogUtils.logAndThrowRuntime(LOG, "Operation Init Failed!",e);
				}
				startExecutionTime = System.currentTimeMillis();
		}

		@Override
		public SpliceNoPutResultSet executeScan(SpliceRuntimeContext runtimeContext) throws StandardException {
				SpliceLogUtils.trace(LOG, "executeScan");
				final List<SpliceOperation> operationStack = getOperationStack();
				SpliceLogUtils.trace(LOG,"operationStack=%s",operationStack);
				SpliceOperation regionOperation = operationStack.get(0);
				SpliceLogUtils.trace(LOG,"regionOperation=%s",regionOperation);
				RowProvider provider;
				PairDecoder decoder = OperationUtils.getPairDecoder(this,runtimeContext);
				if(regionOperation.getNodeTypes().contains(NodeType.REDUCE)&&this!=regionOperation){
						SpliceLogUtils.trace(LOG,"Scanning temp tables");
						provider = regionOperation.getReduceRowProvider(this,decoder,runtimeContext, true);
				}else {
						SpliceLogUtils.trace(LOG,"scanning Map table");
						provider = regionOperation.getMapRowProvider(this,decoder,runtimeContext);
				}
				return new SpliceNoPutResultSet(activation,this, provider);
		}

		@Override
		public RowProvider getMapRowProvider(SpliceOperation top, PairDecoder decoder,SpliceRuntimeContext spliceRuntimeContext) throws StandardException {
				return source.getMapRowProvider(top, decoder, spliceRuntimeContext);
		}

		@Override
		public RowProvider getReduceRowProvider(SpliceOperation top, PairDecoder decoder, SpliceRuntimeContext spliceRuntimeContext, boolean returnDefaultValue) throws StandardException {
				return source.getReduceRowProvider(top, decoder, spliceRuntimeContext, returnDefaultValue);
		}

        private void getKeyColumnDVDs() throws StandardException{
            if (kdvds == null && getColumnOrdering() != null) {
                int len = columnOrdering.length;
                kdvds = new DataValueDescriptor[len];
                getFormatIds();
                for (int i = 0; i < len; ++i) {
                    kdvds[i] = LazyDataValueFactory.getLazyNull(format_ids[columnOrdering[i]]);
                }
            }
        }
		@Override
		public KeyEncoder getKeyEncoder(SpliceRuntimeContext spliceRuntimeContext) throws StandardException {
            int[] keyColumns = new int[getColumnOrdering().length];
            int[] baseColumnMap = operationInformation.getBaseColumnMap();
            for (int i = 0; i <keyColumns.length; ++i)
                keyColumns[i] = -1;
            for (int i = 0; i < keyColumns.length && columnOrdering[i] < baseColumnMap.length; ++i) {
                keyColumns[i] = baseColumnMap[columnOrdering[i]];
            }
			/*
			 * We only ask for this KeyEncoder if we are the top of a RegionScan.
			 * In this case, we encode with either the current row location or a
			 * random UUID (if the current row location is null).
			 */
            DataHash hash = new SuppliedDataHash(new StandardSupplier<byte[]>() {
                @Override
                public byte[] get() throws StandardException {
                    if(currentRowLocation!=null)
                        return currentRowLocation.getBytes();
                    return SpliceDriver.driver().getUUIDGenerator().nextUUIDBytes();
                }
            });

            return new KeyEncoder(NoOpPrefix.INSTANCE,hash,NoOpPostfix.INSTANCE);
		}

		@Override
		public DataHash getRowHash(SpliceRuntimeContext spliceRuntimeContext) throws StandardException {
				int[] nonPkCols = getAccessedNonPkColumns();
				DescriptorSerializer[] serializers = VersionedSerializers.forVersion(mainTableVersion,true).getSerializers(compactRow);
				return BareKeyHash.encoder(nonPkCols,null,serializers);

		}

		@Override
		public SpliceOperation getLeftOperation() {
				return this.source;
		}

		@Override
		public List<NodeType> getNodeTypes() {
				return Collections.singletonList(NodeType.SCAN);
		}

		@Override

		public List<SpliceOperation> getSubOperations() {
				SpliceLogUtils.trace(LOG,"getSubOperations");
				return Collections.singletonList(source);
		}

        private SpliceConglomerate getConglomerate() throws StandardException{
            if(conglomerate==null)
                conglomerate = (SpliceConglomerate)((SpliceTransactionManager)activation.getTransactionController()).findConglomerate(conglomId);
            return conglomerate;
        }

        private int[] getColumnOrdering() throws StandardException{
            if (columnOrdering == null)
                columnOrdering = getConglomerate().getColumnOrdering();
            return columnOrdering;
        }

        private int[] getFormatIds() throws StandardException {
            if (format_ids == null)
                format_ids = getConglomerate().getFormat_ids();
            return format_ids;
        }

		@Override
		public ExecRow nextRow(SpliceRuntimeContext spliceRuntimeContext) throws StandardException, IOException {
				if(timer==null)
						timer = spliceRuntimeContext.newTimer();

				SpliceLogUtils.trace(LOG,"<%s> nextRow",indexName);
				ExecRow sourceRow;
				ExecRow retRow = null;
				boolean restrict;
				DataValueDescriptor restrictBoolean;

				if(reader==null){
						reader = new IndexRowReaderBuilder()
										.source(source)
										.mainTableConglomId(conglomId)
										.outputTemplate(compactRow)
										.transactionId(getTransactionID())
										.indexColumns(indexCols)
										.mainTableKeyColumnEncodingOrder(getColumnOrdering())
										.mainTableKeyColumnTypes(getKeyColumnTypes())
										.mainTableKeyColumnSortOrder(getConglomerate().getAscDescInfo())
										.mainTableKeyDecodingMap(getMainTableKeyDecodingMap())
										.mainTableAccessedKeyColumns(getMainTableAccessedKeyColumns())
										.runtimeContext(spliceRuntimeContext)
										.mainTableVersion(mainTableVersion)
										.mainTableRowDecodingMap(operationInformation.getBaseColumnMap())
										.mainTableAccessedRowColumns(getMainTableRowColumns())
										.numConcurrentLookups(SpliceConstants.indexLookupBlocks)
										.lookupBatchSize(SpliceConstants.indexBatchSize)
										.build();

//						reader = IndexRowReader.create(source,conglomId,compactRow,
//										getTransactionID(),
//										indexCols,
//										operationInformation.getBaseColumnMap(),heapOnlyCols,
//										spliceRuntimeContext, getColumnOrdering(), getFormatIds(), mainTableVersion);
				}

				timer.startTiming();
				do{
						IndexRowReader.RowAndLocation roLoc = reader.next();
						boolean rowExists = roLoc!=null;
						if(!rowExists){
								//No Rows remaining
								clearCurrentRow();
								baseRowLocation= null;
								retRow = null;
								if(reader!=null){
										try {
												reader.close();
										} catch (IOException e) {
												SpliceLogUtils.warn(LOG,"Unable to close IndexRowReader");
										}
								}
								break;
						}
						sourceRow = roLoc.row;
						if(baseRowLocation==null)
								baseRowLocation = new HBaseRowLocation();

						baseRowLocation.setValue(roLoc.rowLocation);

						SpliceLogUtils.trace(LOG,"<%s> retrieved index row %s",indexName,sourceRow);
						SpliceLogUtils.trace(LOG, "<%s>compactRow=%s", indexName,compactRow);
						setCurrentRow(sourceRow);
						currentRowLocation = baseRowLocation;
						restrictBoolean = ((restriction == null) ? null: restriction.invoke());
						restrict = (restrictBoolean ==null) ||
										((!restrictBoolean.isNull()) && restrictBoolean.getBoolean());

						if(!restrict||!rowExists){
								clearCurrentRow();
								baseRowLocation=null;
								currentRowLocation=null;
								rowsFiltered++;
						}else{
								retRow = sourceRow;
								setCurrentRow(sourceRow);
								currentRowLocation = baseRowLocation;
								source.setCurrentRowLocation(baseRowLocation);
						}
				}while(!restrict);
				if(retRow==null){
						timer.tick(0);
						stopExecutionTime = System.currentTimeMillis();
				}else
						timer.tick(1);
				return retRow;
		}

		private FormatableBitSet getMainTableAccessedKeyColumns() throws StandardException {
				int[] keyColumnEncodingOrder = getColumnOrdering();
				FormatableBitSet accessedKeys = new FormatableBitSet(keyColumnEncodingOrder.length);
				for(int i=0;i<keyColumnEncodingOrder.length;i++){
					int keyColumn = keyColumnEncodingOrder[i];
						if(heapOnlyCols.get(keyColumn))
								accessedKeys.set(i);
				}
				return accessedKeys;
		}

		private FormatableBitSet getMainTableRowColumns() throws StandardException {
				int[] keyColumnEncodingOrder = getColumnOrdering();
				FormatableBitSet accessedKeys = new FormatableBitSet(heapOnlyCols);
				for(int i=0;i<keyColumnEncodingOrder.length;i++){
						int keyColumn = keyColumnEncodingOrder[i];
						if(heapOnlyCols.get(keyColumn))
								accessedKeys.clear(i);
				}
				return accessedKeys;
		}

		private int[] getKeyColumnTypes() throws StandardException {
				int[] keyColumnEncodingOrder = getColumnOrdering();
				if(keyColumnEncodingOrder==null) return null; //no keys to worry about
				int[] allFormatIds = getConglomerate().getFormat_ids();
				int[] keyFormatIds = new int[keyColumnEncodingOrder.length];
				for(int i=0,pos=0;i<keyColumnEncodingOrder.length;i++){
						int keyColumnPosition = keyColumnEncodingOrder[i];
						if(keyColumnPosition>=0){
								keyFormatIds[pos] = allFormatIds[keyColumnPosition];
								pos++;
						}
				}
				return keyFormatIds;
		}

		private int[] getMainTableKeyDecodingMap() throws StandardException {
				FormatableBitSet keyCols = getMainTableAccessedKeyColumns();

				int[] keyColumnEncodingOrder = getColumnOrdering();
				int[] baseColumnMap = operationInformation.getBaseColumnMap();

				int[] kDecoderMap = new int[keyColumnEncodingOrder.length];
				Arrays.fill(kDecoderMap, -1);
				for(int i=0;i<keyColumnEncodingOrder.length;i++){
						int baseKeyColumnPosition = keyColumnEncodingOrder[i]; //the position of the column in the base row
						if(keyCols.get(i)){
								kDecoderMap[i] = baseColumnMap[baseKeyColumnPosition];
								baseColumnMap[baseKeyColumnPosition] = -1;
						}else
								kDecoderMap[i] = -1;
				}

				return kDecoderMap;
		}

		@Override
		public void close() throws StandardException, IOException {
				SpliceLogUtils.trace(LOG, "close in IndexRowToBaseRow");
				beginTime = getCurrentTimeMillis();
				source.close();
				super.close();
				closeTime += getElapsedMillis(beginTime);
		}

		@Override
		public ExecRow getExecRowDefinition() {
				return compactRow.getClone();
		}

		@Override
		public int[] getRootAccessedCols(long tableNumber) throws StandardException {
				return source.getRootAccessedCols(tableNumber);
		}

		@Override
		public boolean isReferencingTable(long tableNumber) {
				return source.isReferencingTable(tableNumber);
		}

		public String getIndexName() {
				return this.indexName;
		}

		public  FormatableBitSet getAccessedHeapCols() {
				return this.accessedHeapCols;
		}

		public SpliceOperation getSource() {
				return this.source;
		}

		@Override
		protected void updateStats(OperationRuntimeStats stats) {
				if(reader==null) return;
				TimeView timing = reader.getTimeInfo();
				long bytesRead = reader.getBytesFetched();
				long rowsRead = reader.getTotalRows();
				stats.addMetric(OperationMetric.REMOTE_GET_BYTES,bytesRead);
				stats.addMetric(OperationMetric.REMOTE_GET_ROWS,rowsRead);
				stats.addMetric(OperationMetric.REMOTE_GET_WALL_TIME,timing.getWallClockTime());
				stats.addMetric(OperationMetric.REMOTE_GET_CPU_TIME,timing.getCpuTime());
				stats.addMetric(OperationMetric.REMOTE_GET_USER_TIME,timing.getUserTime());
				stats.addMetric(OperationMetric.FILTERED_ROWS,rowsFiltered);
				//for Index lookups, same number of input rows as output rows
				stats.addMetric(OperationMetric.INPUT_ROWS,timer.getNumEvents());
		}

		@Override protected int getNumMetrics() { return 6; }

		@Override
		public String toString() {
				return String.format("IndexRowToBaseRow {source=%s,indexName=%s,conglomId=%d,resultSetNumber=%d}",
								source,indexName,conglomId,resultSetNumber);
		}

		@Override
		public void open() throws StandardException, IOException {
				super.open();
				if(source!=null)source.open();;
		}

		@Override
		public String prettyPrint(int indentLevel) {
				String indent = "\n"+ Strings.repeat("\t",indentLevel);

				return new StringBuilder("IndexRowToBaseRow:")
								.append(indent).append("resultSetNumber:").append(resultSetNumber)
								.append(indent).append("accessedCols:").append(accessedCols)
								.append(indent).append("resultRowAllocatorMethodName:").append(resultRowAllocatorMethodName)
								.append(indent).append("indexName:").append(indexName)
								.append(indent).append("accessedHeapCols:").append(accessedHeapCols)
								.append(indent).append("heapOnlyCols:").append(heapOnlyCols)
								.append(indent).append("accessedAllCols:").append(accessedAllCols)
								.append(indent).append("indexCols:").append(Arrays.toString(indexCols))
								.append(indent).append("source:").append(source.prettyPrint(indentLevel+1))
								.toString();
		}

		@Override
		public String getName() {
				return super.getName()+"(baseTable="+indexName+")";
		}

        @Override
        public int[] getAccessedNonPkColumns() throws StandardException{
            int[] baseColumnMap = operationInformation.getBaseColumnMap();
            int[] nonPkCols = new int[baseColumnMap.length];
            for (int i = 0; i < nonPkCols.length; ++i)
                nonPkCols[i] = baseColumnMap[i];
            for (int col:getColumnOrdering()){
                if (col < nonPkCols.length) {
                    nonPkCols[col] = -1;
                }
            }
            return nonPkCols;
        }
}
