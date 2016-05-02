package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.hbase.SpliceObserverInstructions;
import com.splicemachine.derby.iapi.sql.execute.*;
import com.splicemachine.derby.iapi.storage.RowProvider;
import com.splicemachine.derby.impl.storage.ClientScanProvider;
import com.splicemachine.derby.impl.store.access.hbase.HBaseRowLocation;
import com.splicemachine.derby.utils.FormatableBitSetUtils;
import com.splicemachine.derby.utils.Scans;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.derby.utils.marshall.PairDecoder;
import com.splicemachine.job.JobResults;
import com.splicemachine.pipeline.exception.Exceptions;
import com.splicemachine.utils.SpliceLogUtils;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.FormatableArrayHolder;
import com.splicemachine.db.iapi.services.io.FormatableIntHolder;
import com.splicemachine.db.iapi.services.loader.GeneratedMethod;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.store.access.Qualifier;
import com.splicemachine.db.iapi.store.access.StaticCompiledOpenConglomInfo;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class HashScanOperation extends ScanOperation implements SinkingOperation {
		private static Logger LOG = Logger.getLogger(HashScanOperation.class);
		
		protected Scan mapScan;
		protected Boolean isKeyed;
		protected ExecRow currentRow;
		protected int hashKeyItem;
		protected static List<NodeType> nodeTypes;
		protected int[] keyColumns;
		protected String nextQualifierField;
		protected Qualifier[][] nextQualifier;
		protected Result result;
		protected boolean eliminateDuplicates;
		public static final	int	DEFAULT_INITIAL_CAPACITY = -1;
		public static final float DEFAULT_LOADFACTOR = (float) -1.0;
		public static final	int	DEFAULT_MAX_CAPACITY = -1;

		private boolean runTimeStatisticsOn;
		public Properties scanProperties;
		public String startPositionString;
		public String stopPositionString;

		static {
				nodeTypes = new ArrayList<NodeType>();
				nodeTypes.add(NodeType.SCAN);
				nodeTypes.add(NodeType.MAP);
				nodeTypes.add(NodeType.REDUCE);
		}

		private int[] baseColumnMap;

	    protected static final String NAME = HashScanOperation.class.getSimpleName().replaceAll("Operation","");

		@Override
		public String getName() {
				return NAME;
		}

		
		public HashScanOperation() {
				super();
				SpliceLogUtils.trace(LOG, "instantiated");
		}

		public  HashScanOperation(long conglomId,
															StaticCompiledOpenConglomInfo scoci, Activation activation,
															GeneratedMethod resultRowAllocator,
															int resultSetNumber,
															GeneratedMethod startKeyGetter, int startSearchOperator,
															GeneratedMethod stopKeyGetter, int stopSearchOperator,
															boolean sameStartStopPosition,
                                                            boolean rowIdKey,
															String scanQualifiersField,
															String nextQualifierField,
															int initialCapacity,
															float loadFactor,
															int maxCapacity,
															int hashKeyItem,
															String tableName,
															String userSuppliedOptimizerOverrides,
															String indexName,
															boolean isConstraint,
															boolean forUpdate,
															int colRefItem,
															int lockMode,
															boolean tableLocked,
															int isolationLevel,
															boolean skipNullKeyColumns,
															double optimizerEstimatedRowCount,
															double optimizerEstimatedCost) throws StandardException {
				super(conglomId,activation,resultSetNumber,startKeyGetter,startSearchOperator,stopKeyGetter,stopSearchOperator,sameStartStopPosition,rowIdKey,scanQualifiersField,
								resultRowAllocator,lockMode,tableLocked,isolationLevel,colRefItem, -1,false, optimizerEstimatedRowCount,optimizerEstimatedCost,userSuppliedOptimizerOverrides);
				SpliceLogUtils.trace(LOG, "scan operation instantiated for " + tableName);
				this.tableName = "" + conglomId;
				this.hashKeyItem = hashKeyItem;
				this.nextQualifierField = nextQualifierField;
				this.indexName = indexName;
				runTimeStatisticsOn = activation.getLanguageConnectionContext().getRunTimeStatisticsMode();
				try {
						init(SpliceOperationContext.newContext(activation));
				} catch (IOException e) {
						throw Exceptions.parseException(e);
				}
				recordConstructorTime();
		}

		@Override
		public void readExternal(ObjectInput in) throws IOException,ClassNotFoundException {
				SpliceLogUtils.trace(LOG, "readExternal");
				super.readExternal(in);
				tableName = in.readUTF();
				hashKeyItem = in.readInt();
				nextQualifierField = readNullableString(in);
				eliminateDuplicates = in.readBoolean();
				runTimeStatisticsOn = in.readBoolean();
		}

		@Override
		public void writeExternal(ObjectOutput out) throws IOException {
				SpliceLogUtils.trace(LOG, "writeExternal");
				super.writeExternal(out);
				out.writeUTF(tableName);
				out.writeInt(hashKeyItem);
				writeNullableString(nextQualifierField, out);
				out.writeBoolean(eliminateDuplicates);
				out.writeBoolean(runTimeStatisticsOn);
		}

		@Override
		public void init(SpliceOperationContext context) throws StandardException, IOException {
				SpliceLogUtils.trace(LOG, "init called");
				super.init(context);
				ExecRow candidate = scanInformation.getResultRow();//(ExecRow) generatedMethod.invoke(activation);
				FormatableArrayHolder fah = (FormatableArrayHolder)(activation.getPreparedStatement().getSavedObject(hashKeyItem));
				FormatableIntHolder[] fihArray = (FormatableIntHolder[]) fah.getArray(FormatableIntHolder.class);
				currentRow = operationInformation.compactRow(candidate, scanInformation.getAccessedColumns(), false);
				baseColumnMap = operationInformation.getBaseColumnMap();
				keyColumns = new int[fihArray.length];
				for (int index = 0; index < fihArray.length; index++) {
						keyColumns[index] = FormatableBitSetUtils.currentRowPositionFromBaseRow(scanInformation.getAccessedColumns(), fihArray[index].getInt());
				}
		}

		@Override
		public List<NodeType> getNodeTypes() {
				return nodeTypes;
		}

		@Override
		public List<SpliceOperation> getSubOperations() {
				SpliceLogUtils.trace(LOG, "getSubOperations");
				return new ArrayList<SpliceOperation>();
		}

		@Override
		public SpliceOperation getLeftOperation() {
				return null;
		}

		@Override
		public RowProvider getMapRowProvider(SpliceOperation top,PairDecoder decoder, SpliceRuntimeContext spliceRuntimeContext) throws StandardException, IOException {
				try{
						Scan scan = Scans.buildPrefixRangeScan(uniqueSequenceID,null);
						return new ClientScanProvider("hashScanMap",SpliceConstants.TEMP_TABLE_BYTES,scan,
										OperationUtils.getPairDecoder(this,spliceRuntimeContext), spliceRuntimeContext);
				} catch (IOException e) {
						throw Exceptions.parseException(e);
				}
		}

		@Override
		public RowProvider getReduceRowProvider(SpliceOperation top, PairDecoder decoder, SpliceRuntimeContext spliceRuntimeContext, boolean returnDefaultValue) throws StandardException, IOException {
				return getMapRowProvider(top,decoder, spliceRuntimeContext);
		}

		@Override
		protected JobResults doShuffle(SpliceRuntimeContext runtimeContext) throws StandardException, IOException {
				Scan scan = buildScan(runtimeContext);
				RowProvider provider =  new ClientScanProvider("shuffler",Bytes.toBytes(tableName),scan,null,runtimeContext);
				SpliceObserverInstructions soi = SpliceObserverInstructions.create(getActivation(),this,runtimeContext);
				return provider.shuffleRows(soi);
		}

		@Override
		public SpliceNoPutResultSet executeProbeScan() {
				SpliceLogUtils.trace(LOG, "executeProbeScan");
				try {
						sequence = new DataValueDescriptor[1];
						sequence[0] = activation.getDataValueFactory().getBitDataValue(uniqueSequenceID);
						SpliceRuntimeContext spliceRuntimeContext = new SpliceRuntimeContext(operationInformation.getTransaction());
						return new SpliceNoPutResultSet(activation,this,getReduceRowProvider(this,OperationUtils.getPairDecoder(this,spliceRuntimeContext), spliceRuntimeContext, true));
				} catch (Exception e) {
						SpliceLogUtils.logAndThrowRuntime(LOG, "executeProbeScan failed!", e);
						return null;
				}
		}

		@Override
		public void generateLeftOperationStack(List<SpliceOperation> operations) {
				SpliceLogUtils.trace(LOG,"generateLeftOperationStac");
				operations.add(this);
		}

		@Override
		public ExecRow getExecRowDefinition() {
				return currentRow;
		}

		@Override
		public ExecRow getNextSinkRow(SpliceRuntimeContext spliceRuntimeContext) throws StandardException {
				return nextRow(spliceRuntimeContext);
		}

		@Override
		public ExecRow nextRow(SpliceRuntimeContext spliceRuntimeContext) throws StandardException {
				SpliceLogUtils.trace(LOG, "nextRow");
				List keyValues = new ArrayList(2);
				try {
						regionScanner.next(keyValues);
						if (!keyValues.isEmpty()) {
								SpliceLogUtils.trace(LOG, "nextRow retrieved hbase values %s", keyValues);

								DataValueDescriptor[] rowArray = currentRow.getRowArray();
								SpliceLogUtils.trace(LOG, "nextRow retrieved derby row %s", currentRow);
								this.setCurrentRow(currentRow);
								currentRowLocation = new HBaseRowLocation(dataLib.getDataRow(keyValues.get(0)));
								return currentRow;
						}
				} catch (Exception e) {
						SpliceLogUtils.logAndThrowRuntime(LOG, e);
				}
				currentRow = null;
				this.setCurrentRow(currentRow);
				return currentRow;
		}

		@Override
		public SpliceNoPutResultSet executeScan(SpliceRuntimeContext runtimeContext) throws StandardException {
				try {
						RowProvider provider = getReduceRowProvider(this, OperationUtils.getPairDecoder(this, runtimeContext), runtimeContext, true);
						return new SpliceNoPutResultSet(activation,this,provider);
				} catch (IOException e) {
						throw Exceptions.parseException(e);
				}
		}

		public Qualifier[][] getNextQualifier() {
				return this.nextQualifier;
		}

		@Override
		public void	close() throws StandardException, IOException {
				SpliceLogUtils.trace(LOG, "close in HashScan");
				super.close();
				closeTime += getElapsedMillis(beginTime);
		}

		@Override
		public byte[] getUniqueSequenceId() {
				return uniqueSequenceID;
		}

		public Properties getScanProperties()
		{
				//TODO: need to get ScanInfo to store in runtime statistics
				if (scanProperties == null)
						scanProperties = new Properties();

				scanProperties.setProperty("numPagesVisited", "0");
				scanProperties.setProperty("numRowsVisited", "0");
				scanProperties.setProperty("numRowsQualified", "0");
				scanProperties.setProperty("numColumnsFetched", "0");//FIXME: need to loop through accessedCols to figure out
				scanProperties.setProperty("columnsFetchedBitSet", ""+getAccessedCols());
				//treeHeight

				return scanProperties;
		}
}
