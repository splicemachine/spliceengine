package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.derby.hbase.SpliceObserverInstructions;
import com.splicemachine.derby.hbase.SpliceOperationCoprocessor;
import com.splicemachine.derby.iapi.sql.execute.SinkingOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceNoPutResultSet;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.iapi.storage.RowProvider;
import com.splicemachine.derby.impl.sql.execute.Serializer;
import com.splicemachine.derby.impl.storage.ClientScanProvider;
import com.splicemachine.derby.impl.store.access.hbase.HBaseRowLocation;
import com.splicemachine.derby.utils.*;
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
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.*;

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
    public void init(SpliceOperationContext context) throws StandardException{
        SpliceLogUtils.trace(LOG, "init called");
        super.init(context);
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

    public OperationSink.Translator getTranslator() throws IOException {
        hasher = new Hasher(getExecRowDefinition().getRowArray(),keyColumns,null,sequence[0]);
        final Serializer serializer = Serializer.get();
        final byte[] scannedTableName = regionScanner.getRegionInfo().getTableName();
        //TODO -sf- does this even work?
        return new OperationSink.Translator() {
            @Nonnull
            @Override
            public List<Mutation> translate(@Nonnull ExecRow row,byte[] postfix) throws IOException {
                try{
                    byte[] tempRow;
                    if(eliminateDuplicates)
                        tempRow = hasher.generateSortedHashKeyWithPostfix(row.getRowArray(),scannedTableName);
                    else
                        tempRow = hasher.generateSortedHashKeyWithPostfix(row.getRowArray(),postfix);

                    Put put = Puts.buildTempTableInsert(tempRow,row.getRowArray(),null,serializer);
                    return Collections.<Mutation>singletonList(put);
                }catch(StandardException se){
                    throw Exceptions.getIOException(se);
                }
            }

            @Override
            public boolean mergeKeys() {
                return false;
            }
        };
    }

	@Override
	public SpliceOperation getLeftOperation() {
		return null;
	}

	@Override
	public RowProvider getMapRowProvider(SpliceOperation top,ExecRow template) throws StandardException {
        try{
            Scan scan = Scans.buildPrefixRangeScan(sequence[0],SpliceUtils.NA_TRANSACTION_ID);
            return new ClientScanProvider(SpliceOperationCoprocessor.TEMP_TABLE,scan,template,null);
        } catch (IOException e) {
            throw Exceptions.parseException(e);
        }
	}

    @Override
    public RowProvider getReduceRowProvider(SpliceOperation top, ExecRow template) throws StandardException {
        return getMapRowProvider(top,template);
    }

    @Override
    protected JobStats doShuffle() throws StandardException {
        Scan scan = buildScan();
        RowProvider provider =  new ClientScanProvider(Bytes.toBytes(tableName),scan,getExecRowDefinition(),null);
        SpliceObserverInstructions soi = SpliceObserverInstructions.create(getActivation(),this);
        return provider.shuffleRows(soi);
    }

    @Override
	public NoPutResultSet executeProbeScan() {
		SpliceLogUtils.trace(LOG, "executeProbeScan");
		try {
			sequence = new DataValueDescriptor[1];
			sequence[0] = activation.getDataValueFactory().getVarcharDataValue(uniqueSequenceID);
			Qualifier[][] probe = (Qualifier[][]) activation.getClass().getField(nextQualifierField).get(activation);
			Scan scan = Scans.newScan(DerbyBytesUtil.generateSortedHashScan(probe,sequence[0]),
																DerbyBytesUtil.generateIncrementedSortedHashScan(probe,sequence[0]), getTransactionID());
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

    public ExecRow getNextSinkRow() throws StandardException {
        return getNextRowCore();
    }

	@Override
	public ExecRow getNextRowCore() throws StandardException {
		  SpliceLogUtils.trace(LOG, "getNextRowCore");
		  List<KeyValue> keyValues = new ArrayList<KeyValue>();	
		  try {
			regionScanner.next(keyValues);
			while (!keyValues.isEmpty()) {
				SpliceLogUtils.trace(LOG, "getNextRowCore retrieved hbase values %s",keyValues);
				  SpliceUtils.populate(keyValues, currentRow.getRowArray(),accessedCols,baseColumnMap);
				  SpliceLogUtils.trace(LOG, "getNextRowCore retrieved derby row %s",currentRow);
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
    public NoPutResultSet executeScan() throws StandardException {
        RowProvider provider = getReduceRowProvider(this,getExecRowDefinition());
        return new SpliceNoPutResultSet(activation,this,provider);
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
