package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.derby.hbase.SpliceObserverInstructions;
import com.splicemachine.derby.hbase.SpliceOperationCoprocessor;
import com.splicemachine.derby.iapi.sql.execute.SinkingOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceNoPutResultSet;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.iapi.storage.RowProvider;
import com.splicemachine.derby.impl.storage.ClientScanProvider;
import com.splicemachine.derby.impl.store.access.hbase.HBaseRowLocation;
import com.splicemachine.derby.utils.Exceptions;
import com.splicemachine.derby.utils.FormatableBitSetUtils;
import com.splicemachine.derby.utils.Scans;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.derby.utils.marshall.PairDecoder;
import com.splicemachine.derby.utils.marshall.RowDecoder;
import com.splicemachine.derby.utils.marshall.RowMarshaller;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.hbase.writer.CallBuffer;
import com.splicemachine.hbase.writer.KVPair;
import com.splicemachine.job.JobStats;
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
    public void init(SpliceOperationContext context) throws StandardException{
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
	public RowProvider getMapRowProvider(SpliceOperation top,PairDecoder decoder, SpliceRuntimeContext spliceRuntimeContext) throws StandardException {
        try{
            Scan scan = Scans.buildPrefixRangeScan(uniqueSequenceID,SpliceUtils.NA_TRANSACTION_ID);
            return new ClientScanProvider("hashScanMap",SpliceOperationCoprocessor.TEMP_TABLE,scan,
										OperationUtils.getPairDecoder(this,spliceRuntimeContext), spliceRuntimeContext);
        } catch (IOException e) {
            throw Exceptions.parseException(e);
        }
	}

    @Override
    public RowProvider getReduceRowProvider(SpliceOperation top, PairDecoder decoder, SpliceRuntimeContext spliceRuntimeContext) throws StandardException {
        return getMapRowProvider(top,decoder, spliceRuntimeContext);
    }

    @Override
    protected JobStats doShuffle(SpliceRuntimeContext runtimeContext) throws StandardException {
        Scan scan = buildScan();
        RowProvider provider =  new ClientScanProvider("shuffler",Bytes.toBytes(tableName),scan,null,runtimeContext);
        SpliceObserverInstructions soi = SpliceObserverInstructions.create(getActivation(),this,runtimeContext);
        return provider.shuffleRows(soi);
    }

    @Override
	public NoPutResultSet executeProbeScan() {
		SpliceLogUtils.trace(LOG, "executeProbeScan");
		try {
			sequence = new DataValueDescriptor[1];
			sequence[0] = activation.getDataValueFactory().getBitDataValue(uniqueSequenceID);
			SpliceRuntimeContext spliceRuntimeContext = new SpliceRuntimeContext();
			return new SpliceNoPutResultSet(activation,this,getReduceRowProvider(this,OperationUtils.getPairDecoder(this,spliceRuntimeContext), spliceRuntimeContext));
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
		public CallBuffer<KVPair> transformWriteBuffer(CallBuffer<KVPair> bufferToTransform) throws StandardException {
				return bufferToTransform;
		}

		@Override
	public ExecRow nextRow(SpliceRuntimeContext spliceRuntimeContext) throws StandardException {
		  SpliceLogUtils.trace(LOG, "nextRow");
		  List<KeyValue> keyValues = new ArrayList<KeyValue>();	
		  try {
			regionScanner.next(keyValues);
              if (!keyValues.isEmpty()) {
                  SpliceLogUtils.trace(LOG, "nextRow retrieved hbase values %s", keyValues);

                  DataValueDescriptor[] rowArray = currentRow.getRowArray();
                  for(KeyValue kv:keyValues){
                      RowMarshaller.sparsePacked().decode(kv, rowArray, baseColumnMap, (MultiFieldDecoder)null);
                  }
                  SpliceLogUtils.trace(LOG, "nextRow retrieved derby row %s", currentRow);
                  this.setCurrentRow(currentRow);
                  currentRowLocation = new HBaseRowLocation(keyValues.get(0).getRow());
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
    public NoPutResultSet executeScan(SpliceRuntimeContext runtimeContext) throws StandardException {
        RowProvider provider = getReduceRowProvider(this,OperationUtils.getPairDecoder(this,runtimeContext), runtimeContext);
        return new SpliceNoPutResultSet(activation,this,provider);
    }

    public int[] getKeyColumns() {
		return this.keyColumns;
	}
	
	public Qualifier[][] getNextQualifier() {
		return this.nextQualifier;
	}
	
	@Override
	public void	close() throws StandardException, IOException {
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
