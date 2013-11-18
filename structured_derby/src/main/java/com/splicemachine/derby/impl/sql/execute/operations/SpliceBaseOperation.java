package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.hbase.SpliceObserverInstructions;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.iapi.storage.RowProvider;
import com.splicemachine.derby.impl.sql.execute.ValueRow;
import com.splicemachine.derby.impl.store.access.SpliceTransactionManager;
import com.splicemachine.derby.stats.RegionStats;
import com.splicemachine.derby.utils.StandardSupplier;
import com.splicemachine.derby.utils.marshall.*;
import com.splicemachine.job.JobStats;
import com.splicemachine.job.JobStatsUtils;
import com.splicemachine.utils.IntArrays;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.i18n.MessageService;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.ResultColumnDescriptor;
import org.apache.derby.iapi.sql.ResultDescription;
import org.apache.derby.iapi.sql.ResultSet;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.ExecutionFactory;
import org.apache.derby.iapi.sql.execute.NoPutResultSet;
import org.apache.derby.iapi.store.access.Qualifier;
import org.apache.derby.iapi.store.raw.Transaction;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.Orderable;
import org.apache.derby.iapi.types.RowLocation;
import org.apache.derby.shared.common.reference.SQLState;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.*;
import java.sql.SQLWarning;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

public abstract class SpliceBaseOperation implements SpliceOperation, Externalizable {
    private static final long serialVersionUID = 4l;
	private static Logger LOG = Logger.getLogger(SpliceBaseOperation.class);
	/* Run time statistics variables */
	public int numOpens;
	public int rowsSeen;
	public int rowsFiltered;
	protected long startExecutionTime;
	protected long endExecutionTime;
	public long beginTime;
	public long constructorTime;
	public long openTime;
	public long nextTime;
	public long closeTime;
	protected boolean statisticsTimingOn;
	protected HRegion region;

	protected Activation activation;
    protected String transactionID;

    /**
     * Used to communicate a child transaction ID down to the sink operation in a sub-class.
     * Since some write operations occur in sub-transactions, it is important that all do. This is so that the SI
     * logic can identify the order in which events occurred.
     */
    private String childTransactionID;
	protected boolean isTopResultSet = false;
	protected byte[] uniqueSequenceID;
	protected ExecRow currentRow;
	protected RowLocation currentRowLocation;
	protected List<SpliceOperation> leftOperationStack;

	protected boolean executed = false;
	protected DataValueDescriptor[] sequence;
	protected RegionScanner regionScanner;
	protected long rowsSunk;
	
	protected boolean isOpen = true;
	RegionStats regionStats;

    /*
     * Used to indicate rows which should be excluded from TEMP because their backing operation task
     * failed and was retried for some reasons. will be null for tasks which do not make use of the TEMP
     * table.
     */
    protected List<byte[]> failedTasks = Collections.emptyList();

    protected int resultSetNumber;
    protected OperationInformation operationInformation;

    public SpliceBaseOperation() {
		super();
	}

    public SpliceBaseOperation(OperationInformation information) throws StandardException {
        this.operationInformation = information;
        this.resultSetNumber = operationInformation.getResultSetNumber();
        sequence = new DataValueDescriptor[1];
        sequence[0] = information.getSequenceField(uniqueSequenceID);
    }

	public SpliceBaseOperation(Activation activation,
                               int resultSetNumber,
                               double optimizerEstimatedRowCount,
                               double optimizerEstimatedCost) throws StandardException {
		if (statisticsTimingOn = activation.getLanguageConnectionContext().getStatisticsTiming())
		    beginTime = startExecutionTime = getCurrentTimeMillis();
        this.operationInformation = new DerbyOperationInformation(activation,optimizerEstimatedRowCount,optimizerEstimatedCost,resultSetNumber);
		this.activation = activation;
        this.resultSetNumber = resultSetNumber;
		sequence = new DataValueDescriptor[1];
		SpliceLogUtils.trace(LOG, "dataValueFactor=%s",activation.getDataValueFactory());
		sequence[0] = operationInformation.getSequenceField(uniqueSequenceID);
		if (activation.getLanguageConnectionContext().getStatementContext() == null) {
			SpliceLogUtils.trace(LOG, "Cannot get StatementContext from Activation's lcc");
			return;
		}
	}
	
	public ExecutionFactory getExecutionFactory(){
		return activation.getExecutionFactory();
	}

	@Override
	public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        this.operationInformation = (OperationInformation)in.readObject();
        transactionID = readNullableString(in);
        isTopResultSet = in.readBoolean();
        uniqueSequenceID = new byte[in.readInt()];
        in.readFully(uniqueSequenceID);

		statisticsTimingOn = in.readBoolean();
		constructorTime = in.readLong();
		openTime = in.readLong();
		nextTime = in.readLong();
		closeTime = in.readLong();
		startExecutionTime = in.readLong();
		endExecutionTime = in.readLong();
		rowsSeen = in.readInt();
		rowsFiltered = in.readInt();
        failedTasks = (List<byte[]>)in.readObject();
	}

	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		SpliceLogUtils.trace(LOG, "writeExternal");
        out.writeObject(operationInformation);
        writeNullableString(getTransactionID(), out);
		out.writeBoolean(isTopResultSet);
        out.writeInt(uniqueSequenceID.length);
        out.write(uniqueSequenceID);
		out.writeBoolean(statisticsTimingOn);
		out.writeLong(constructorTime);
		out.writeLong(openTime);
		out.writeLong(nextTime);
		out.writeLong(closeTime);
		out.writeLong(startExecutionTime);
		out.writeLong(endExecutionTime);
		out.writeInt(rowsSeen);
		out.writeInt(rowsFiltered);
        out.writeObject(failedTasks);
	}

	@Override
	public SpliceOperation getLeftOperation() {
		throw new UnsupportedOperationException("class "+this.getClass()+" does not implement getLeftOperation!");
	}

    @Override
	public int modifiedRowCount() {
		return 0;
	}
	
	@Override
	public Activation getActivation() {
		return activation;
	}

	@Override
	public void clearCurrentRow() {
        if(activation!=null){
            activation.clearCurrentRow(operationInformation.getResultSetNumber());
        }
        currentRow=null;
	}

	@Override
	public void close() throws StandardException,IOException {
		if (!isOpen)
			return;

		/* If this is the top ResultSet then we must  close all of the open subqueries for the
		 * entire query.
		 */


        isOpen = false;

	}
	

//	@Override
//	public NoPutResultSet[] getSubqueryTrackingArray(int numSubqueries) {
//		if (subqueryTrackingArray == null)
//			subqueryTrackingArray = new NoPutResultSet[numSubqueries];
//		return subqueryTrackingArray;
//	}
	
//	@Override
	public void addWarning(SQLWarning w) {
        activation.addWarning(w);
	}

//	@Override
	public SQLWarning getWarnings() {
        return activation.getWarnings();
	}

	@Override
	public void markAsTopResultSet() {
		this.isTopResultSet = true;		
	}
	@Override
	public void open() throws StandardException, IOException {
        this.uniqueSequenceID = operationInformation.getUUIDGenerator().nextBytes();
//        init(SpliceOperationContext.newContext(activation));
	}
//	@Override
	public double getEstimatedRowCount() {
		return operationInformation.getEstimatedRowCount();
	}

	@Override
	public int resultSetNumber() {
		return operationInformation.getResultSetNumber();
	}
	@Override
	public void setCurrentRow(ExecRow row) {
        operationInformation.setCurrentRow(row);
		currentRow = row;
	}

	public static void writeNullableString(String value, DataOutput out) throws IOException {
		if (value != null) {
			out.writeBoolean(true);
			out.writeUTF(value);			
		} else {
			out.writeBoolean(false);
		}
	}
	
	public static String readNullableString(DataInput in) throws IOException {
		if (in.readBoolean())
			return in.readUTF();
		return null;
	}

	@Override
	public void init(SpliceOperationContext context) throws StandardException{
		this.activation = context.getActivation();
        this.operationInformation.initialize(context);
        this.resultSetNumber = operationInformation.getResultSetNumber();
		sequence = new DataValueDescriptor[1];
        sequence[0] = operationInformation.getSequenceField(uniqueSequenceID);
		try {
			this.regionScanner = context.getScanner();
			this.region = context.getRegion();
		} catch (IOException e) {
			SpliceLogUtils.logAndThrowRuntime(LOG,"Unable to get Scanner",e);
		}
	}

	@Override
	public String getUniqueSequenceID() {
		return Long.toString(Bytes.toLong(uniqueSequenceID));
	}

		@Override
		public KeyEncoder getKeyEncoder(SpliceRuntimeContext spliceRuntimeContext) throws StandardException {
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
				ExecRow defnRow = getExecRowDefinition();
				return BareKeyHash.encoder(IntArrays.count(defnRow.nColumns()),null);
		}

    /**
     * Called during the executeShuffle() phase, for the execution of parallel operations.
     *
     * If the operation does a transformation (e.g. ProjectRestrict, Normalize, IndexRowToBaseRow), then
     * this should delegate to the operation's source.
     *
     * @param top the top operation to be executed
     * @param decoder the decoder to use
     * @return a MapRowProvider
     * @throws StandardException if something goes wrong
     */
    @Override
	public RowProvider getMapRowProvider(SpliceOperation top,PairDecoder decoder, SpliceRuntimeContext spliceRuntimeContext) throws StandardException {
		throw new UnsupportedOperationException("MapRowProviders not implemented for this node: "+ this.getClass());
	}

    /**
     * Called during the executeScan() phase, for the execution of sequential operations.
     *
     * If the operation does a transformation (e.g. ProjectRestrict, Normalize, IndexRowToBaseRow), then
     * this should delegate to the operation's source.
     *
     * @param top the top operation to be executed
     * @param decoder the decoder to use
     * @return a ReduceRowProvider
     * @throws StandardException if something goes wrong
     */
    @Override
    public RowProvider getReduceRowProvider(SpliceOperation top,PairDecoder decoder, SpliceRuntimeContext spliceRuntimeContext) throws StandardException {
        throw new UnsupportedOperationException("ReduceRowProviders not implemented for this node: "+ this.getClass());
    }

    @Override
    public final void executeShuffle(SpliceRuntimeContext runtimeContext) throws StandardException {
        /*
         * Marked final so that subclasses don't accidentally screw up their error-handling of the
         * TEMP table by forgetting to deal with failedTasks/statistics/whatever else needs to be handled.
         */
        JobStats stats = doShuffle(runtimeContext);
        JobStatsUtils.logStats(stats);
        failedTasks = new ArrayList<byte[]>(stats.getFailedTasks());
	}

    protected JobStats doShuffle(SpliceRuntimeContext spliceRuntimeContext) throws StandardException {
        long start = System.currentTimeMillis();
        final RowProvider rowProvider = getMapRowProvider(this,OperationUtils.getPairDecoder(this,spliceRuntimeContext),spliceRuntimeContext);

        nextTime+= System.currentTimeMillis()-start;
        SpliceObserverInstructions soi = SpliceObserverInstructions.create(getActivation(),this,spliceRuntimeContext);
        return rowProvider.shuffleRows(soi);
    }

    protected ExecRow getFromResultDescription(ResultDescription resultDescription) throws StandardException {
        ExecRow row = new ValueRow(resultDescription.getColumnCount());
        for(int i=1;i<=resultDescription.getColumnCount();i++){
            ResultColumnDescriptor rcd = resultDescription.getColumnDescriptor(i);
            row.setColumn(i, rcd.getType().getNull());
        }
        return row;
    }

	@Override
	public NoPutResultSet executeScan(SpliceRuntimeContext runtimeContext) throws StandardException {
		throw new RuntimeException("Execute Scan Not Implemented for this node " + this.getClass());														
	}

	@Override
	public NoPutResultSet executeProbeScan() {
		throw new RuntimeException("Execute Probe Scan Not Implemented for this node " + this.getClass());														
	}

	@Override
	public ExecRow getExecRowDefinition() throws StandardException {
		throw new RuntimeException("No ExecRow Definition for this node " + this.getClass());														
	}


	@Override
	public void generateLeftOperationStack(List<SpliceOperation> operations) {
//		SpliceLogUtils.trace(LOG, "generateLeftOperationStack");
		OperationUtils.generateLeftOperationStack(this, operations);
	}

	protected List<SpliceOperation> getOperationStack(){
		if(leftOperationStack==null){
			leftOperationStack = new LinkedList<SpliceOperation>();
			generateLeftOperationStack(leftOperationStack);
		}
		return leftOperationStack;
	}
	public void generateRightOperationStack(boolean initial,List<SpliceOperation> operations) {
		SpliceLogUtils.trace(LOG, "generateRightOperationStack");
		SpliceOperation op;
		if (initial) 
			op = getRightOperation();
		else 
			op = getLeftOperation();			
		if(op !=null && !op.getNodeTypes().contains(NodeType.REDUCE)){
			op.generateRightOperationStack(false,operations);
		}else if(op!=null)
			operations.add(op);
		operations.add(this);				
	}

	@Override
	public SpliceOperation getRightOperation() {
		throw new UnsupportedOperationException("class "+this.getClass()+" does not implement getLeftOperation!");
	}
	
//	@Override
//    public int[] getRootAccessedCols(long tableNumber) {
//		throw new UnsupportedOperationException("class "+ this.getClass()+" does not implement getRootAccessedCols");
//	}
//
//    @Override
//    public boolean isReferencingTable(long tableNumber) {
//        throw new UnsupportedOperationException("class "+ this.getClass()+" does not implement isReferencingTable");
//    }

	public long getExecuteTime()
	{
		return getTimeSpent(ResultSet.ENTIRE_RESULTSET_TREE);
	}

	protected final long getCurrentTimeMillis()
	{
		if (statisticsTimingOn)
			return System.currentTimeMillis();
		else
			return 0;
	}
	
	protected final long getElapsedMillis(long beginTime)
	{
		if (statisticsTimingOn)
			return (System.currentTimeMillis() - beginTime);
		else
			return 0;
	}
	
	public RegionStats getRegionStats() {
		return this.regionStats;
	}
	
	public static String printQualifiers(Qualifier[][] qualifiers)
	{
		String idt = "";

		String output = "";
		if (qualifiers == null)
		{
			return idt + MessageService.getTextMessage(SQLState.LANG_NONE);
		}

        for (int term = 0; term < qualifiers.length; term++)
        {
            for (int i = 0; i < qualifiers[term].length; i++)
            {
                Qualifier qual = qualifiers[term][i];

                output = idt + output +
                    MessageService.getTextMessage(
                        SQLState.LANG_COLUMN_ID_ARRAY,
                            String.valueOf(term), String.valueOf(i)) +
                        ": " + qual.getColumnId() + "\n";
                    
                int operator = qual.getOperator();
                String opString;
                switch (operator)
                {
                  case Orderable.ORDER_OP_EQUALS:
                    opString = "=";
                    break;

                  case Orderable.ORDER_OP_LESSOREQUALS:
                    opString = "<=";
                    break;

                  case Orderable.ORDER_OP_LESSTHAN:
                    opString = "<";
                    break;

                  default:
                    // NOTE: This does not have to be internationalized, because
                    // this code should never be reached.
                    opString = "unknown value (" + operator + ")";
                    break;
                }
                output = output +
                    idt + MessageService.getTextMessage(SQLState.LANG_OPERATOR) +
                            ": " + opString + "\n" +
                    idt +
                        MessageService.getTextMessage(
                            SQLState.LANG_ORDERED_NULLS) +
                        ": " + qual.getOrderedNulls() + "\n" +
                    idt +
                        MessageService.getTextMessage(
                            SQLState.LANG_UNKNOWN_RETURN_VALUE) +
                        ": " + qual.getUnknownRV() + "\n" +
                    idt +
                        MessageService.getTextMessage(
                            SQLState.LANG_NEGATE_COMPARISON_RESULT) +
                        ": " + qual.negateCompareResult() + "\n";
            }
        }

		return output;
	}

	protected final void recordConstructorTime()
	{
		if (statisticsTimingOn)
		    constructorTime = getElapsedMillis(beginTime);
	}
	
	public long getTimeSpent(int type)
	{
		return constructorTime + openTime + nextTime + closeTime;
	}

    protected Transaction getTrans() {
        return (activation.getTransactionController() == null) ? null : ((SpliceTransactionManager) activation.getTransactionController()).getRawStoreXact();
    }

    public void clearChildTransactionID() {
        this.childTransactionID = null;
    }

    public String getTransactionID() {
        if (childTransactionID != null) {
            return childTransactionID;
        } else if (activation == null) {
            return transactionID;
        } else {
            return (getTrans() == null) ? null : activation.getTransactionController().getActiveStateTxIdString();
        }
    }

    @Override
    public RowLocation getCurrentRowLocation() {
        return currentRowLocation;
    }

    @Override
    public void setCurrentRowLocation(RowLocation rowLocation) {
        currentRowLocation = rowLocation;
    }

    public int getResultSetNumber() {
        return resultSetNumber;
    }

    public double getEstimatedCost() {
        return operationInformation.getEstimatedCost();
    }
}
