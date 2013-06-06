package com.splicemachine.derby.impl.sql.execute.operations;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.splicemachine.derby.hbase.SpliceObserverInstructions;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.iapi.storage.RowProvider;
import com.splicemachine.derby.impl.sql.execute.ValueRow;
import com.splicemachine.derby.impl.store.access.SpliceTransactionManager;
import com.splicemachine.derby.stats.RegionStats;
import com.splicemachine.derby.stats.TaskStats;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.job.JobStats;
import com.splicemachine.job.JobStatsUtils;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.i18n.MessageService;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.sql.*;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.execute.*;
import org.apache.derby.iapi.sql.execute.xplain.XPLAINVisitor;
import org.apache.derby.iapi.store.access.Qualifier;
import org.apache.derby.iapi.store.raw.Transaction;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.Orderable;
import org.apache.derby.iapi.types.RowLocation;
import org.apache.derby.shared.common.reference.SQLState;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import javax.annotation.Nullable;
import java.io.*;
import java.sql.SQLWarning;
import java.sql.Timestamp;
import java.util.LinkedList;
import java.util.List;

public abstract class SpliceBaseOperation implements SpliceOperation, Externalizable, NoPutResultSet {
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
	protected String uniqueSequenceID;
	protected ExecRow currentRow;
	protected RowLocation currentRowLocation;
	protected List<SpliceOperation> leftOperationStack;
	protected List<SpliceOperation> rightOperationStack;

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
    protected List<byte[]> failedTasks;

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
		sequence[0] = activation.getDataValueFactory().getVarcharDataValue(uniqueSequenceID);		
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
//		SpliceLogUtils.trace(LOG, "readExternal");
		optimizerEstimatedCost = in.readDouble();
		optimizerEstimatedRowCount = in.readDouble();
		resultSetNumber = in.readInt();
        transactionID = readNullableString(in);
        isTopResultSet = in.readBoolean();
		uniqueSequenceID = in.readUTF();
		statisticsTimingOn = in.readBoolean();
		constructorTime = in.readLong();
		openTime = in.readLong();
		nextTime = in.readLong();
		closeTime = in.readLong();
		startExecutionTime = in.readLong();
		endExecutionTime = in.readLong();
		rowsSeen = in.readInt();
		rowsFiltered = in.readInt();
//        if(in.readBoolean()){
//            operationParams = (ParameterValueSet)in.readObject();
//        }
	}

	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		SpliceLogUtils.trace(LOG, "writeExternal");
		out.writeDouble(optimizerEstimatedCost);
		out.writeDouble(optimizerEstimatedRowCount);		
		out.writeInt(resultSetNumber);
        writeNullableString(getTransactionID(), out);
		out.writeBoolean(isTopResultSet);
		out.writeUTF(uniqueSequenceID);
		out.writeBoolean(statisticsTimingOn);
		out.writeLong(constructorTime);
		out.writeLong(openTime);
		out.writeLong(nextTime);
		out.writeLong(closeTime);
		out.writeLong(startExecutionTime);
		out.writeLong(endExecutionTime);
		out.writeInt(rowsSeen);
		out.writeInt(rowsFiltered);
//		out.writeBoolean(operationParams!=null);
//		if(operationParams!=null){
//			out.writeObject(operationParams);
//		}
	}

	@Override
	public boolean needsRowLocation() {
		// TODO Auto-generated method stub
		return false;
	}
	
	@Override
	public SpliceOperation getLeftOperation() {
		throw new UnsupportedOperationException("class "+this.getClass()+" does not implement getLeftOperation!");
	}

	@Override
	public void rowLocation(RowLocation rl) throws StandardException {
		// TODO Auto-generated method stub
	}
	
	@Override
	public boolean returnsRows() {
		// TODO Auto-generated method stub
		return false;
	}
	
	@Override
	public int modifiedRowCount() {
		// TODO Auto-generated method stub
		return 0;
	}
	
	@Override
	public ResultDescription getResultDescription() {
		// TODO Auto-generated method stub
		return null;
	}
	
	@Override
	public Activation getActivation() {
		return activation;
	}
	@Override
	public void open() throws StandardException {
		// TODO Auto-generated method stub
		
	}
	@Override
	public ExecRow getAbsoluteRow(int row) throws StandardException {
		// TODO Auto-generated method stub
		return null;
	}
	@Override
	public ExecRow getRelativeRow(int row) throws StandardException {
		// TODO Auto-generated method stub
		return null;
	}
	@Override
	public ExecRow setBeforeFirstRow() throws StandardException {
		// TODO Auto-generated method stub
		return null;
	}
	@Override
	public ExecRow getFirstRow() throws StandardException {
		// TODO Auto-generated method stub
		return null;
	}
	@Override
	public ExecRow getNextRow() throws StandardException {
		// TODO Auto-generated method stub
		return null;
	}
	@Override
	public ExecRow getPreviousRow() throws StandardException {
		// TODO Auto-generated method stub
		return null;
	}
	@Override
	public ExecRow getLastRow() throws StandardException {
		// TODO Auto-generated method stub
		return null;
	}
	@Override
	public ExecRow setAfterLastRow() throws StandardException {
		// TODO Auto-generated method stub
		return null;
	}
	@Override
	public void clearCurrentRow() {
        activation.clearCurrentRow(resultSetNumber);
        currentRow=null;
	}

	@Override
	public boolean checkRowPosition(int isType) throws StandardException {
		// TODO Auto-generated method stub
		return false;
	}
	@Override
	public int getRowNumber() {
		// TODO Auto-generated method stub
		return 0;
	}
	@Override
	public void close() throws StandardException {
		if (!isOpen)
			return;

		/* If this is the top ResultSet then we must  close all of the open subqueries for the
		 * entire query.
		 */
		if (isTopResultSet)
		{
			/*
			** If run time statistics tracing is turned on, then now is the
			** time to dump out the information.
			*/
			LanguageConnectionContext lcc = activation.getLanguageConnectionContext();
			
                // only if statistics is switched on, collect & derive them
				//TODO: need to get statement context, clearly cannot get from the lcc 
                //if (statisticsTimingOn && !lcc.getStatementContext().getStatementWasInvalidated())
                if (statisticsTimingOn)
				{   
                    endExecutionTime = getCurrentTimeMillis();

                    // get the ResultSetStatisticsFactory, which gathers RuntimeStatistics
                    ExecutionFactory ef = lcc.getLanguageConnectionFactory().getExecutionFactory();
                    ResultSetStatisticsFactory rssf = ef.getResultSetStatisticsFactory();
  
                    // get the RuntimeStatisticsImpl object which is the wrapper for all 
                    // gathered statistics about all the different resultsets
                    RunTimeStatistics rsImpl = rssf.getRunTimeStatistics(activation, this, subqueryTrackingArray); 
                    SpliceLogUtils.trace(LOG, "top resultset, RunTimeStatistics=%s,EndExecutionTimestamp=%s",rsImpl,rsImpl.getEndExecutionTimestamp());
                    // save the RTW (wrapper)object in the lcc
                    lcc.setRunTimeStatisticsObject(rsImpl);
                    
                    // now explain gathered statistics, using an appropriate visitor
                    XPLAINVisitor visitor = ef.getXPLAINFactory().getXPLAINVisitor();
                    visitor.doXPLAIN(rsImpl,activation);
  				}

			int staLength = (subqueryTrackingArray == null) ? 0 : subqueryTrackingArray.length;

			for (int index = 0; index < staLength; index++)
			{
				if (subqueryTrackingArray[index] == null || subqueryTrackingArray[index].isClosed())
					continue;
				subqueryTrackingArray[index].close();
			}
		}

		isOpen = false;

	}
	
	@Override
	public void cleanUp() throws StandardException {
		// TODO Auto-generated method stub
		
	}
	@Override
	public boolean isClosed() {
		return false;
	}
	@Override
	public void finish() throws StandardException {
		// TODO Auto-generated method stub
		
	}
	
	@Override
	public NoPutResultSet[] getSubqueryTrackingArray(int numSubqueries) {
		if (subqueryTrackingArray == null)
			subqueryTrackingArray = new NoPutResultSet[numSubqueries];
		return subqueryTrackingArray;
	}
	
	@Override
	public ResultSet getAutoGeneratedKeysResultset() {
		// TODO Auto-generated method stub
		return null;
	}
	@Override
	public String getCursorName() {
		// TODO Auto-generated method stub
		return null;
	}
	@Override
	public void addWarning(SQLWarning w) {
		// TODO Auto-generated method stub
		
	}
	@Override
	public SQLWarning getWarnings() {
		// TODO Auto-generated method stub
		return null;
	}
	@Override
	public DataValueDescriptor[] getNextRowFromRowSource()
			throws StandardException {
		// TODO Auto-generated method stub
		return null;
	}
	@Override
	public boolean needsToClone() {
		// TODO Auto-generated method stub
		return false;
	}
	@Override
	public FormatableBitSet getValidColumns() {
		// TODO Auto-generated method stub
		return null;
	}
	@Override
	public void closeRowSource() {
		// TODO Auto-generated method stub
		
	}
	@Override
	public void markAsTopResultSet() {
		this.isTopResultSet = true;		
	}
	@Override
	public void openCore() throws StandardException {
        this.uniqueSequenceID = SpliceUtils.getUniqueKeyString();
        init(SpliceOperationContext.newContext(activation));
	}
	@Override
	public void reopenCore() throws StandardException {
		// TODO Auto-generated method stub
		
	}
	@Override
	public int getPointOfAttachment() {
		// TODO Auto-generated method stub
		return 0;
	}
	@Override
	public int getScanIsolationLevel() {
		// TODO Auto-generated method stub
		return 0;
	}
	@Override
	public void setTargetResultSet(TargetResultSet trs) {
		// TODO Auto-generated method stub
		
	}
	@Override
	public void setNeedsRowLocation(boolean needsRowLocation) {
		// TODO Auto-generated method stub
		
	}
	@Override
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
	@Override
	public boolean requiresRelocking() {
		// TODO Auto-generated method stub
		return false;
	}
	@Override
	public boolean isForUpdate() {
		// TODO Auto-generated method stub
		return false;
	}
	@Override
	public void updateRow(ExecRow row, RowChanger rowChanger)
			throws StandardException {
		// TODO Auto-generated method stub
		
	}
	@Override
	public void markRowAsDeleted() throws StandardException {
		// TODO Auto-generated method stub
		
	}
	@Override
	public void positionScanAtRowLocation(RowLocation rLoc)
			throws StandardException {
		// TODO Auto-generated method stub
		
	}

    protected ExecRow getCompactRow(LanguageConnectionContext lcc,
                                    ExecRow candidate,
                                    FormatableBitSet accessedCols,
                                    boolean isKeyed) throws StandardException {
        int	numCandidateCols = candidate.nColumns();
		ExecRow compactRow = null;
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
        sequence[0] = activation.getDataValueFactory().getVarcharDataValue(uniqueSequenceID);
		try {
			this.regionScanner = context.getScanner();
		} catch (IOException e) {
			SpliceLogUtils.logAndThrowRuntime(LOG,"Unable to get Scanner",e);
		}
	}

	@Override
	public String getUniqueSequenceID() {
		return uniqueSequenceID;
	}

    @Override
    public OperationSink.Translator getTranslator() throws IOException {
        throw new UnsupportedOperationException("Sink not implemented for this node: "+ this.getClass());
    }

    /**
     * Called during the executeShuffle() phase, for the execution of parallel operations.
     *
     * If the operation does a transformation (e.g. ProjectRestrict, Normalize, IndexRowToBaseRow), then
     * this should delegate to the operation's source.
     *
     * @param top the top operation to be executed
     * @param template the template rows to be returned
     * @return a MapRowProvider
     * @throws StandardException if something goes wrong
     */
    @Override
	public RowProvider getMapRowProvider(SpliceOperation top,ExecRow template) throws StandardException {
		throw new UnsupportedOperationException("MapRowProviders not implemented for this node: "+ this.getClass());
	}

    /**
     * Called during the executeScan() phase, for the execution of sequential operations.
     *
     * If the operation does a transformation (e.g. ProjectRestrict, Normalize, IndexRowToBaseRow), then
     * this should delegate to the operation's source.
     *
     * @param top the top operation to be executed
     * @param template the template rows to be returned
     * @return a ReduceRowProvider
     * @throws StandardException if something goes wrong
     */
    @Override
    public RowProvider getReduceRowProvider(SpliceOperation top,ExecRow template) throws StandardException {
        throw new UnsupportedOperationException("ReduceRowProviders not implemented for this node: "+ this.getClass());
    }

    @Override
    public void cleanup() {
        throw new RuntimeException("Finish Not Implemented for this node " + this.getClass());
    }

    private static final Function<String,byte[]> taskToBytes = new Function<String, byte[]>() {
        @Override
        public byte[] apply(@Nullable String input) {
            return Bytes.toBytes(input);
        }
    };

    @Override
    public final void executeShuffle() throws StandardException {
        /*
         * Marked final so that subclasses don't accidentally screw up their error-handling of the
         * TEMP table by forgetting to deal with failedTasks/statistics/whatever else needs to be handled.
         */
        JobStats stats = doShuffle();
        JobStatsUtils.logStats(stats,LOG);
        failedTasks = Lists.transform(stats.getFailedTasks(),taskToBytes);
	}

    protected JobStats doShuffle() throws StandardException {
        long start = System.currentTimeMillis();
        final RowProvider rowProvider = getMapRowProvider(this, getExecRowDefinition());

        nextTime+= System.currentTimeMillis()-start;
        SpliceObserverInstructions soi = SpliceObserverInstructions.create(getActivation(),this);
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

	public List<SpliceOperation> getRightOperationStack(){
		if(rightOperationStack==null){
			rightOperationStack = new LinkedList<SpliceOperation>();
			generateRightOperationStack(true,rightOperationStack);
		}
		return rightOperationStack;
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

	/**
	 * Get the Timestamp for the beginning of execution.
	 *
	 * @return Timestamp		The Timestamp for the beginning of execution.
	 */
	public Timestamp getBeginExecutionTimestamp()
	{
		if (startExecutionTime == 0)
			return null;
		else
			return new Timestamp(startExecutionTime);
	}

	/**
	 * Get the Timestamp for the end of execution.
	 *
	 * @return Timestamp		The Timestamp for the end of execution.
	 */
	public Timestamp getEndExecutionTimestamp()
	{
		if (endExecutionTime == 0)
			return null;
		else
			return new Timestamp(endExecutionTime);
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
                String opString = null;
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

    public void setChildTransactionID(String childTransactionID) {
        this.childTransactionID = childTransactionID;
    }

    public void clearChildTransactionID() {
        this.childTransactionID = null;
    }

    protected String getTransactionID() {
        if (childTransactionID != null) {
            return childTransactionID;
        } else if (activation == null) {
            return transactionID;
        } else {
            return (getTrans() == null) ? null : activation.getTransactionController().getActiveStateTxIdString();
        }
    }
}
