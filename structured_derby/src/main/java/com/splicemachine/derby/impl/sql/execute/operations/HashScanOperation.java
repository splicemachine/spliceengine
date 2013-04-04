package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.derby.hbase.SpliceObserverInstructions;
import com.splicemachine.derby.hbase.SpliceOperationCoprocessor;
import com.splicemachine.derby.hbase.SpliceOperationProtocol;
import com.splicemachine.derby.iapi.sql.execute.SpliceNoPutResultSet;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.iapi.storage.RowProvider;
import com.splicemachine.derby.impl.sql.execute.Serializer;
import com.splicemachine.derby.impl.storage.ClientScanProvider;
import com.splicemachine.derby.impl.store.access.SpliceAccessManager;
import com.splicemachine.derby.impl.store.access.hbase.HBaseRowLocation;
import com.splicemachine.derby.stats.RegionStats;
import com.splicemachine.derby.stats.SinkStats;
import com.splicemachine.derby.utils.*;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.io.FormatableArrayHolder;
import org.apache.derby.iapi.services.io.FormatableIntHolder;
import org.apache.derby.iapi.services.loader.GeneratedMethod;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.NoPutResultSet;
import org.apache.derby.iapi.store.access.Qualifier;
import org.apache.derby.iapi.store.access.StaticCompiledOpenConglomInfo;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.impl.sql.GenericStorablePreparedStatement;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class HashScanOperation extends ScanOperation {
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
	
	protected Hasher hasher;
	
	static {
		nodeTypes = new ArrayList<NodeType>();
		nodeTypes.add(NodeType.SCAN);
		nodeTypes.add(NodeType.MAP);
		nodeTypes.add(NodeType.REDUCE);		
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
    	super(conglomId,activation,resultSetNumber,startKeyGetter,startSearchOperator,stopKeyGetter,stopSearchOperator,sameStartStopPosition,scanQualifiersField,
    			resultRowAllocator,lockMode,tableLocked,isolationLevel,colRefItem,optimizerEstimatedRowCount,optimizerEstimatedCost);
		SpliceLogUtils.trace(LOG, "scan operation instantiated for " + tableName);
		this.tableName = "" + conglomId;
		this.hashKeyItem = hashKeyItem;
		this.nextQualifierField = nextQualifierField;
		this.indexName = indexName;
		runTimeStatisticsOn = activation.getLanguageConnectionContext().getRunTimeStatisticsMode();
        init(SpliceOperationContext.newContext(activation));
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
	public void init(SpliceOperationContext context){
		SpliceLogUtils.trace(LOG, "init called");
		super.init(context);
		try {
			GenericStorablePreparedStatement statement = context.getPreparedStatement();
			GeneratedMethod generatedMethod = statement.getActivationClass().getMethod(resultRowAllocatorMethodName);
			ExecRow candidate = (ExecRow) generatedMethod.invoke(activation);
			FormatableArrayHolder fah = (FormatableArrayHolder)(activation.getPreparedStatement().getSavedObject(hashKeyItem));
			FormatableIntHolder[] fihArray = (FormatableIntHolder[]) fah.getArray(FormatableIntHolder.class);
			LanguageConnectionContext lcc = context.getLanguageConnectionContext();
			currentRow = getCompactRow(lcc,candidate, accessedCols, false);
			keyColumns = new int[fihArray.length];
			for (int index = 0; index < fihArray.length; index++) {
				keyColumns[index] = FormatableBitSetUtils.currentRowPositionFromBaseRow(accessedCols, fihArray[index].getInt());
			}
			LOG.info("activation.getClass()="+activation.getClass()+",aactivation="+activation);
			if (scanQualifiersField != null)
				scanQualifiers = (Qualifier[][]) activation.getClass().getField(scanQualifiersField).get(activation);
			this.mapScan = Scans.setupScan(startPosition==null?null:startPosition.getRowArray(), startSearchOperator,
					stopPosition==null?null:stopPosition.getRowArray(), stopSearchOperator,
					scanQualifiers, null, accessedCols, transactionID);
//			this.mapScan = SpliceUtils.setupScan(Bytes.toBytes(transactionID), accessedCols,scanQualifiers, startPosition == null ? null : startPosition.getRowArray(), startSearchOperator, stopPosition == null ? null : stopPosition.getRowArray(), stopSearchOperator, null);
		} catch (Exception e) {
			SpliceLogUtils.logAndThrowRuntime(LOG, "Operation Init Failed!", e);
		} 
	}
	
	@Override
	public List<NodeType> getNodeTypes() {
		SpliceLogUtils.trace(LOG, "getNodeTypes");
		return nodeTypes;
	}

	@Override
	public List<SpliceOperation> getSubOperations() {
		SpliceLogUtils.trace(LOG, "getSubOperations");
		return new ArrayList<SpliceOperation>();
	}

	@Override
	public void executeShuffle() {
		SpliceLogUtils.trace(LOG, "executeShuffle");
		List<SpliceOperation> operationStack = new ArrayList<SpliceOperation>();
		this.generateLeftOperationStack(operationStack);
		final SpliceOperation regionOperation = operationStack.get(0);
		HTableInterface htable = null;
		try {
			regionStats = new RegionStats(this.getClass().getName());
            regionStats.start();
            
			htable = SpliceAccessManager.getHTable(Bytes.toBytes(tableName));
			SpliceLogUtils.trace(LOG, "executing coprocessor on table=" + tableName + ", with scan=" + mapScan);
			final SpliceObserverInstructions soi = SpliceObserverInstructions.create(activation,regionOperation);
			htable.coprocessorExec(SpliceOperationProtocol.class,
					mapScan.getStartRow(), mapScan.getStopRow(),
					new Batch.Call<SpliceOperationProtocol, SinkStats>() {
						@Override
						public SinkStats call(SpliceOperationProtocol instance) throws IOException {
							try {
								return instance.run(mapScan,soi);
							} catch (StandardException e) {
								SpliceLogUtils.logAndThrowRuntime(LOG, "Error Executing Sink", e);
					}
					return null;
				}
			},new Batch.Callback<SinkStats>() {
                        @Override
                        public void update(byte[] region, byte[] row, SinkStats result) {
                            regionStats.addRegionStats(region,result);
                        }
                    });
            regionStats.finish();
            regionStats.recordStats(LOG);
			executed = true;
            nextTime += regionStats.getTotalTimeTakenMs();
		} catch (Exception e) {
			LOG.error("Problem Running Coprocessor " + e.getMessage());
			throw new RuntimeException(e);
		} catch (Throwable e) {
			LOG.error("Problem Running Coprocessor " + e.getMessage());
			throw new RuntimeException(e);
		} finally {
			if (htable != null)
				try {
					htable.close();
				} catch (IOException e) {
					LOG.error("Problem Closing HBase Table");
					throw new RuntimeException(e);
				}
		}
	}

	@Override		
	public SinkStats sink() {
        SinkStats.SinkAccumulator stats = SinkStats.uniformAccumulator();
        stats.start();
        SpliceLogUtils.trace(LOG, ">>>>statistics starts for sink for HashScan at "+stats.getStartTime());
		SpliceLogUtils.trace(LOG, "sink called on" + regionScanner.getRegionInfo().getTableNameAsString());
		HTableInterface tempTable = null;
		Put put = null;
		try{
			tempTable = SpliceAccessManager.getFlushableHTable(SpliceOperationCoprocessor.TEMP_TABLE);
			hasher = new Hasher(currentRow.getRowArray(),keyColumns,null,sequence[0]);
			byte[] scannedTableName = regionScanner.getRegionInfo().getTableName();
            Serializer serializer = new Serializer();
            List<KeyValue> keyValues;
            do{
                long start = System.nanoTime();

                //get the next row
                keyValues = new ArrayList<KeyValue>(currentRow.nColumns());
                regionScanner.next(keyValues);
                if(keyValues==null) continue;
                stats.processAccumulator().tick(System.nanoTime()-start);

                //sink the row by hashing
                start = System.nanoTime();
                SpliceLogUtils.trace(LOG, "Sinking Record ");
                result = new Result(keyValues);
                SpliceLogUtils.trace(LOG, "accessedColsToGrab=%s",accessedCols);
                SpliceUtils.populate(result, currentRow.getRowArray(),accessedCols,baseColumnMap,serializer);
                byte[] tempRowKey ;
                if (eliminateDuplicates) {
                    tempRowKey = hasher.generateSortedHashKeyWithPostfix(currentRow.getRowArray(),scannedTableName);
                } else {
                    tempRowKey = hasher.generateSortedHashKey(currentRow.getRowArray());
                }
                SpliceLogUtils.trace(LOG, "row to hash =%s, key=%s", currentRow, Arrays.toString(tempRowKey));
                put = Puts.buildTempTableInsert(tempRowKey, currentRow.getRowArray(), null, serializer);
                tempTable.put(put);	// TODO Buffer via list or configuration. JL

                stats.sinkAccumulator().tick(System.nanoTime()-start);

            }while(!keyValues.isEmpty());

			tempTable.flushCommits();
		}catch (StandardException se){
			SpliceLogUtils.logAndThrowRuntime(LOG,se);
		} catch (IOException e) {
			SpliceLogUtils.logAndThrowRuntime(LOG, e);
		}finally{
			try {
				if(tempTable!=null)
					tempTable.close();
			} catch (IOException e) {
				SpliceLogUtils.error(LOG, "Unexpected error closing TempTable", e);
			}
		}
        //return stats.finish();
		SinkStats ss = stats.finish();
		SpliceLogUtils.trace(LOG, ">>>>statistics finishes for sink for HashScan at "+stats.getFinishTime());
        return ss;
	}
	
	@Override
	public SpliceOperation getLeftOperation() {
		return null;
	}

	@Override
	public RowProvider getMapRowProvider(SpliceOperation top,ExecRow template){
		SpliceUtils.setInstructions(mapScan,getActivation(),top);
		return new ClientScanProvider(Bytes.toBytes(tableName),mapScan,template,null);
	}
	
	@Override
	public NoPutResultSet executeProbeScan() {
		SpliceLogUtils.trace(LOG, "executeProbeScan");
		try {
			sequence = new DataValueDescriptor[1];
			sequence[0] = activation.getDataValueFactory().getVarcharDataValue(uniqueSequenceID);
			Qualifier[][] probe = (Qualifier[][]) activation.getClass().getField(nextQualifierField).get(activation);
			Scan scan = Scans.newScan(DerbyBytesUtil.generateSortedHashScan(probe,sequence[0]),
																DerbyBytesUtil.generateIncrementedSortedHashScan(probe,sequence[0]),transactionID);
//			Scan scan = SpliceUtils.generateScan(sequence[0], DerbyBytesUtil.generateSortedHashScan(probe, sequence[0]),
//					DerbyBytesUtil.generateIncrementedSortedHashScan(probe, sequence[0]), transactionID);
			return new SpliceNoPutResultSet(scan,Bytes.toString(SpliceOperationCoprocessor.TEMP_TABLE),activation,this, currentRow);
		} catch (Exception e) {
			SpliceLogUtils.logAndThrowRuntime(LOG, "executeProbeScan failed!", e);
			return null;
		} 
	}
	
	@Override
	public void cleanup() {
		SpliceLogUtils.trace(LOG,"cleanup");
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
	public ExecRow getNextRowCore() throws StandardException {
		  SpliceLogUtils.trace(LOG, "getNextRowCore");
		  List<KeyValue> keyValues = new ArrayList<KeyValue>();	
		  try {
			regionScanner.next(keyValues);
			while (!keyValues.isEmpty()) {
				SpliceLogUtils.trace(LOG, "getNextRowCore retrieved hbase values " + keyValues);
				  Result result = new Result(keyValues);
				  SpliceUtils.populate(result, currentRow.getRowArray());
				  SpliceLogUtils.trace(LOG, "getNextRowCore retrieved derby row " + currentRow);
				  this.setCurrentRow(currentRow);
				  currentRowLocation = new HBaseRowLocation(result.getRow());
				  return currentRow;
			}
		  } catch (Exception e) {
			  SpliceLogUtils.logAndThrowRuntime(LOG, e);
		  }
	    currentRow = null;
	    this.setCurrentRow(currentRow);
		return currentRow;		  
	}
	
	public int[] getKeyColumns() {
		return this.keyColumns;
	}
	
	public Qualifier[][] getNextQualifier() {
		return this.nextQualifier;
	}
	
	@Override
	public void	close() throws StandardException
	{
		SpliceLogUtils.trace(LOG, "close in HashScan");
		beginTime = getCurrentTimeMillis();
		if ( isOpen )
	    {
			// we don't want to keep around a pointer to the
			// row ... so it can be thrown away.
			// REVISIT: does this need to be in a finally
			// block, to ensure that it is executed?
		    clearCurrentRow();
		    
		    //TODO: need to get ScanInfo to store in runtime statistics
		    scanProperties = getScanProperties();
			
			// This is where we get the positioner info for inner tables
			if (runTimeStatisticsOn)
			{
				startPositionString = printStartPosition();
				stopPositionString = printStopPosition();
			}

			startPosition = null;
			stopPosition = null;

			super.close();
	    }
		
		closeTime += getElapsedMillis(beginTime);
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
