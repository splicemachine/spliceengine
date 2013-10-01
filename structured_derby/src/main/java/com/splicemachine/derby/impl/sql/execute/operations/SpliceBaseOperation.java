package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.derby.hbase.SpliceObserverInstructions;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.iapi.storage.RowProvider;
import com.splicemachine.derby.impl.sql.execute.ValueRow;
import com.splicemachine.derby.impl.store.access.SpliceTransactionManager;
import com.splicemachine.derby.stats.RegionStats;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.derby.utils.marshall.KeyType;
import com.splicemachine.derby.utils.marshall.RowDecoder;
import com.splicemachine.derby.utils.marshall.RowEncoder;
import com.splicemachine.derby.utils.marshall.RowMarshaller;
import com.splicemachine.job.JobStats;
import com.splicemachine.job.JobStatsUtils;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.i18n.MessageService;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.ResultColumnDescriptor;
import org.apache.derby.iapi.sql.ResultDescription;
import org.apache.derby.iapi.sql.ResultSet;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.execute.*;
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
	protected double optimizerEstimatedRowCount;
	protected double optimizerEstimatedCost;
	
	protected Activation activation;
	protected int resultSetNumber;
    private String transactionID;

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
	public NoPutResultSet[]	subqueryTrackingArray;

    /*
     * Used to indicate rows which should be excluded from TEMP because their backing operation task
     * failed and was retried for some reasons. will be null for tasks which do not make use of the TEMP
     * table.
     */
    protected List<byte[]> failedTasks = Collections.emptyList();

	/*
	 * Defines a mapping between any FormattableBitSet's column entries
	 * and a compactRow. This is only used in conjuction with getCompactRow.
	 */
	protected int[] baseColumnMap;

    public SpliceBaseOperation() {
		super();
	}

	public SpliceBaseOperation(Activation activation, int resultSetNumber, double optimizerEstimatedRowCount,double optimizerEstimatedCost) throws StandardException {
		if (statisticsTimingOn = activation.getLanguageConnectionContext().getStatisticsTiming())
		    beginTime = startExecutionTime = getCurrentTimeMillis();		
		this.optimizerEstimatedCost = optimizerEstimatedCost;
		this.optimizerEstimatedRowCount = optimizerEstimatedRowCount;
		this.activation = activation;
		this.resultSetNumber = resultSetNumber;
		sequence = new DataValueDescriptor[1];
		SpliceLogUtils.trace(LOG, "dataValueFactor=%s",activation.getDataValueFactory());
		sequence[0] = activation.getDataValueFactory().getBitDataValue(uniqueSequenceID);
		if (activation.getLanguageConnectionContext().getStatementContext() == null) {
			SpliceLogUtils.trace(LOG, "Cannot get StatementContext from Activation's lcc");
			return;
		}
		//TODO: need to getStatementContext from somewhere
		if (subqueryTrackingArray == null)
			subqueryTrackingArray = activation.getLanguageConnectionContext().getStatementContext().getSubqueryTrackingArray();
	}
	
	public ExecutionFactory getExecutionFactory(){
		return activation.getExecutionFactory();
	}

	@Override
	public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
		optimizerEstimatedCost = in.readDouble();
		optimizerEstimatedRowCount = in.readDouble();
		resultSetNumber = in.readInt();
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
		out.writeDouble(optimizerEstimatedCost);
		out.writeDouble(optimizerEstimatedRowCount);		
		out.writeInt(resultSetNumber);
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
//		out.writeBoolean(operationParams!=null);
//		if(operationParams!=null){
//			out.writeObject(operationParams);
//		}
	}

	@Override
	public SpliceOperation getLeftOperation() {
		throw new UnsupportedOperationException("class "+this.getClass()+" does not implement getLeftOperation!");
	}

    @Override
	public int modifiedRowCount() {
		// TODO Auto-generated method stub
		return 0;
	}
	
	@Override
	public Activation getActivation() {
		return activation;
	}

	@Override
	public void clearCurrentRow() {
        activation.clearCurrentRow(resultSetNumber);
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
        this.uniqueSequenceID = SpliceUtils.getUniqueKey();
        init(SpliceOperationContext.newContext(activation));
	}
//	@Override
	public double getEstimatedRowCount() {
		return this.optimizerEstimatedRowCount;
	}

	@Override
	public int resultSetNumber() {
		return this.resultSetNumber;
	}
	@Override
	public void setCurrentRow(ExecRow row) {
		activation.setCurrentRow(row, resultSetNumber);
		currentRow = row;
	}

    protected ExecRow getCompactRow(LanguageConnectionContext lcc,
                                    ExecRow candidate,
                                    FormatableBitSet accessedCols,
                                    boolean isKeyed) throws StandardException {
        int	numCandidateCols = candidate.nColumns();
		ExecRow compactRow;
		if (accessedCols == null) {
			compactRow =  candidate;
			baseColumnMap = new int[numCandidateCols];
			for (int i = 0; i < baseColumnMap.length; i++)
				baseColumnMap[i] = i;
		}
		else {
			int numCols = accessedCols.getNumBitsSet();
			baseColumnMap = new int[numCandidateCols];

            ExecutionFactory ex = lcc.getLanguageConnectionFactory().getExecutionFactory();
            if (isKeyed) {
                compactRow = ex.getIndexableRow(numCols);
            }
            else {
                compactRow = ex.getValueRow(numCols);
            }
            int position = 0;
			for (int i = accessedCols.anySetBit();i != -1; i = accessedCols.anySetBit(i)) {
				// Stop looking if there are columns beyond the columns
				// in the candidate row. This can happen due to the
				// otherCols bit map.
				if (i >= numCandidateCols)
					break;
				DataValueDescriptor sc = candidate.getColumn(i+1);
				if (sc != null) {
					compactRow.setColumn(position + 1,sc);
				}
				baseColumnMap[i] = position;
				position++;
			}
		}

		return compactRow;
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
		sequence = new DataValueDescriptor[1];
        sequence[0] = activation.getDataValueFactory().getBitDataValue(uniqueSequenceID);
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
    public RowEncoder getRowEncoder(SpliceRuntimeContext spliceRuntimeContext) throws StandardException {
        ExecRow row = getExecRowDefinition();
        return RowEncoder.create(row.nColumns(),
                null,null,null,
                KeyType.BARE, RowMarshaller.packed());
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
	public RowProvider getMapRowProvider(SpliceOperation top,RowDecoder decoder, SpliceRuntimeContext spliceRuntimeContext) throws StandardException {
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
    public RowProvider getReduceRowProvider(SpliceOperation top,RowDecoder decoder, SpliceRuntimeContext spliceRuntimeContext) throws StandardException {
        throw new UnsupportedOperationException("ReduceRowProviders not implemented for this node: "+ this.getClass());
    }

    @Override
    public final void executeShuffle() throws StandardException {
        /*
         * Marked final so that subclasses don't accidentally screw up their error-handling of the
         * TEMP table by forgetting to deal with failedTasks/statistics/whatever else needs to be handled.
         */
        JobStats stats = doShuffle();
        JobStatsUtils.logStats(stats);
        failedTasks = new ArrayList<byte[]>(stats.getFailedTasks());
	}

    protected JobStats doShuffle() throws StandardException {
        long start = System.currentTimeMillis();
        SpliceRuntimeContext spliceRuntimeContext = new SpliceRuntimeContext();
        final RowProvider rowProvider = getMapRowProvider(this, getRowEncoder(spliceRuntimeContext).getDual(getExecRowDefinition()),spliceRuntimeContext);

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
	public NoPutResultSet executeScan() throws StandardException {
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
	
	public double getOptimizerEstimatedRowCount() {
		return this.optimizerEstimatedRowCount;
	}
	
	public double getOptimizerEstimatedCost() {
		return this.optimizerEstimatedCost;
	}
	
	public int getResultSetNumber() {
		return this.resultSetNumber;
	}
	
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
}
