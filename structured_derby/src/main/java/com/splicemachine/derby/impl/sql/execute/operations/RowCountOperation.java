package com.splicemachine.derby.impl.sql.execute.operations;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.splicemachine.derby.hbase.SpliceOperationCoprocessor;
import com.splicemachine.derby.iapi.sql.execute.SpliceNoPutResultSet;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.iapi.storage.RowProvider;
import com.splicemachine.derby.impl.sql.execute.Serializer;
import com.splicemachine.derby.impl.store.access.SpliceAccessManager;
import com.splicemachine.derby.stats.TaskStats;
import com.splicemachine.derby.utils.DerbyBytesUtil;
import com.splicemachine.derby.utils.Puts;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.loader.GeneratedMethod;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.conn.StatementContext;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.NoPutResultSet;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.impl.sql.GenericStorablePreparedStatement;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.log4j.Logger;

public class RowCountOperation extends SpliceBaseOperation {
	private static final long serialVersionUID = 11111;
	private static Logger LOG = Logger.getLogger(RowCountOperation.class);
	private static final List<NodeType> nodeTypes;
	
	protected String offsetMethodName;
	protected String fetchFirstMethodName;
	
    protected GeneratedMethod offsetMethod;
    protected GeneratedMethod fetchFirstMethod;
    protected boolean hasJDBClimitClause;
	protected NoPutResultSet source;
	
	protected ExecRow currSortedRow;
	protected int numColumns;
    private long offset;
    private long fetchFirst;
    
    private boolean runTimeStatsOn;

    /**
     * True if we haven't yet fetched any rows from this result set.
     * Will be reset on close so the result set is ready to reuse.
     */
    private boolean firstTime;

    /**
     * Holds the number of rows returned so far in this round of using the
     * result set.  Will be reset on close so the result set is ready to reuse.
     */
    private long rowsFetched;


	static{
		nodeTypes = Arrays.asList(NodeType.SINK);
	}

	public RowCountOperation(){
		SpliceLogUtils.trace(LOG, "instantiated without parameters");
	}

	public RowCountOperation(NoPutResultSet s,
						 Activation a,
						 int resultSetNumber,
						 GeneratedMethod offsetMethod,
						 GeneratedMethod fetchFirstMethod,
						 boolean hasJDBClimitClause,
						 double optimizerEstimatedRowCount,
						 double optimizerEstimatedCost) throws StandardException{
		super(a,resultSetNumber,optimizerEstimatedRowCount,optimizerEstimatedCost);
		SpliceLogUtils.trace(LOG,"instantiated with parameters");
		SpliceLogUtils.trace(LOG,"source="+s);
		this.offsetMethod = offsetMethod;
		this.fetchFirstMethod = fetchFirstMethod;
		this.offsetMethodName = (offsetMethod == null) ? null : offsetMethod.getMethodName();
		this.fetchFirstMethodName = (fetchFirstMethod == null) ? null : fetchFirstMethod.getMethodName();
		this.hasJDBClimitClause = hasJDBClimitClause;
		this.source = s;
		firstTime = true;
		rowsFetched = 0;
		runTimeStatsOn = activation.getLanguageConnectionContext().getRunTimeStatisticsMode();
		recordConstructorTime(); 
	}
	
	@Override
	public void readExternal(ObjectInput in) throws IOException,
			ClassNotFoundException {
		SpliceLogUtils.trace(LOG, "readExternal");
		super.readExternal(in);
		source = (SpliceOperation)in.readObject();
		offsetMethodName = readNullableString(in);
		fetchFirstMethodName = readNullableString(in);
		runTimeStatsOn = in.readBoolean();
	}

	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		SpliceLogUtils.trace(LOG, "writeExternal");
		super.writeExternal(out);
		out.writeObject((SpliceOperation)source);
		writeNullableString(offsetMethodName, out);
		writeNullableString(fetchFirstMethodName, out);
		out.writeBoolean(runTimeStatsOn);
	}

	@Override
	public List<NodeType> getNodeTypes() {
		return nodeTypes;
	}

	@Override
	public List<SpliceOperation> getSubOperations() {
		SpliceLogUtils.trace(LOG,"getSubOperations");
		List<SpliceOperation> ops = new ArrayList<SpliceOperation>();
		ops.add((SpliceOperation)source);
		return ops;
	}

	@Override
	public void init(SpliceOperationContext context) throws StandardException{
		SpliceLogUtils.trace(LOG,"init");
		super.init(context);
		((SpliceOperation)source).init(context);
		try {
            GenericStorablePreparedStatement statement = context.getPreparedStatement();
			if (offsetMethodName != null)
				offsetMethod = statement.getActivationClass().getMethod(offsetMethodName);
			if (fetchFirstMethodName != null)
				fetchFirstMethod = statement.getActivationClass().getMethod(fetchFirstMethodName);
			firstTime = true;
			rowsFetched = 0;
		}

		catch (StandardException e) {
			LOG.error("Operation Init Failed! " + e);
			e.printStackTrace();
		}
	}

	@Override
	public ExecRow getNextRowCore() throws StandardException {
		SpliceLogUtils.trace(LOG,"getNextRowCore, firstTime = " + firstTime);
		ExecRow r = null;

		// the values come from e1 and e2 methods on activation
		if (firstTime) {
			firstTime = false;
			if (offsetMethod != null) {
				SpliceLogUtils.trace(LOG,"offSetMethod triggered");
				DataValueDescriptor offVal
				= (DataValueDescriptor)offsetMethod.invoke(activation);

				if (offVal.isNotNull().getBoolean()) {
					offset = offVal.getLong();
					SpliceLogUtils.trace(LOG,"Offset: " + offset);
				}
				
				if (offset > 0) { // advance over offset
					for (int i = 1; i <= offset; i++) {
						SpliceLogUtils.trace(LOG, "skipping first " + offset + "(" + i + ")");
						r = getNextRowFromSource();
						SpliceLogUtils.trace(LOG,  "Skipping Row: " + r);
						rowsFiltered++;
					}
				}
			}

			if (fetchFirstMethod != null) {
				SpliceLogUtils.trace(LOG,"fetchFirstMethod triggered");
				DataValueDescriptor fetchFirstVal
				= (DataValueDescriptor)fetchFirstMethod.invoke(activation);

				if (fetchFirstVal.isNotNull().getBoolean()) {
					fetchFirst = fetchFirstVal.getLong();
					SpliceLogUtils.trace(LOG, "fetchFirst: " + fetchFirst);
				}
			}
		}
		
		if (fetchFirstMethod != null && rowsFetched >= fetchFirst) {
			SpliceLogUtils.trace(LOG, "Fetching no more rows, rowsFetched = " + rowsFetched + ", fetchFirst = " + fetchFirst);
			r = null;
		}
		else {
			r = getNextRowFromSource();
			if(r !=null){
				rowsFetched++;
				rowsSeen++;
				setCurrentRow(r);
				SpliceLogUtils.trace(LOG,  "Keeping Row: " + r);
			}
		}
		
		if (runTimeStatsOn) {
            if (! isTopResultSet) {
                 // This is simply for RunTimeStats.  We first need to get the
                 // subquery tracking array via the StatementContext
                StatementContext sc = activation.getLanguageConnectionContext().getStatementContext();
                
                if (sc == null) 
                	SpliceLogUtils.trace(LOG, "Cannot get StatementContext from Activation's lcc");
                else
                	subqueryTrackingArray = sc.getSubqueryTrackingArray();
            }
        }
		return r;
	}
	
	private boolean filterRow(ExecRow currRow,ExecRow newRow) throws StandardException{
		SpliceLogUtils.trace(LOG,"filterRow");
		for(int index = 1; index < numColumns; index++){
			DataValueDescriptor currOrderable = currRow.getColumn(index);
			DataValueDescriptor newOrderable = newRow.getColumn(index);
			if (! (currOrderable.compare(DataValueDescriptor.ORDER_OP_EQUALS,newOrderable,true,true)))
				return false;
		}
		return true;
	}
	
	private ExecRow getNextRowFromSource() throws StandardException {
		SpliceLogUtils.trace(LOG,"getNextRowFromSource");
		ExecRow sourceRow = source.getNextRowCore();
		return sourceRow;
	}

	@Override
	public SpliceOperation getLeftOperation() {
		SpliceLogUtils.trace(LOG,"getLeftOperation");
		return (SpliceOperation) this.source;
	}
	
	@Override
	public ExecRow getExecRowDefinition() throws StandardException {
		SpliceLogUtils.trace(LOG,"getExecRowDefinition");
		ExecRow def = ((SpliceOperation)source).getExecRowDefinition();
		source.setCurrentRow(def);
		return def;
	}

	@Override
	public NoPutResultSet executeScan() throws StandardException {
		SpliceLogUtils.trace(LOG, "executeScan");
		final List<SpliceOperation> operationStack = new ArrayList<SpliceOperation>();
		this.generateLeftOperationStack(operationStack);
		SpliceOperation regionOperation = operationStack.get(0);
		SpliceLogUtils.trace(LOG,"regionOperation="+regionOperation);
		RowProvider provider;
		if(regionOperation.getNodeTypes().contains(NodeType.REDUCE) && this != regionOperation){
			provider = regionOperation.getReduceRowProvider(this,getExecRowDefinition());
		}else {
			provider = regionOperation.getMapRowProvider(this,getExecRowDefinition());
		}
		return new SpliceNoPutResultSet(activation,this,provider);
	}

	@Override
	public TaskStats sink() { // gd not sure I want any of this, or at least the sorted part
        TaskStats.SinkAccumulator stats = TaskStats.uniformAccumulator();
        stats.start();
        SpliceLogUtils.trace(LOG, ">>>>statistics starts for sink for RowCountOperation at "+stats.getStartTime());
		SpliceLogUtils.trace(LOG, "sink");
		ExecRow row = null;
		HTableInterface tempTable = null;
		try{
			Put put;
			tempTable = SpliceAccessManager.getFlushableHTable(SpliceOperationCoprocessor.TEMP_TABLE);
            Serializer serializer = new Serializer();

            do{
                long start = System.nanoTime();
                row = getNextRowCore();
                if(row ==null)continue;
                stats.readAccumulator().tick(System.nanoTime()-start);

                start = System.nanoTime();
                SpliceLogUtils.trace(LOG, "row="+row);
                byte[] rowKey = DerbyBytesUtil.generateSortedHashKey(row.getRowArray(),sequence[0],null,null);
                put = Puts.buildTempTableInsert(rowKey, row.getRowArray(), null, serializer);

                tempTable.put(put);

                stats.writeAccumulator().tick(System.nanoTime()-start);
            }while(row!=null);
            tempTable.flushCommits();
			tempTable.close();
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
		TaskStats ss = stats.finish();
		SpliceLogUtils.trace(LOG, ">>>>statistics finishes for sink for RowCountOperation at "+stats.getFinishTime());
        return ss;
	}
	
	@Override
	public void close() throws StandardException {
		SpliceLogUtils.trace(LOG, "close in RowCount");	
		beginTime = getCurrentTimeMillis();
        if ( isOpen ) {

            // we don't want to keep around a pointer to the
            // row ... so it can be thrown away.
            // REVISIT: does this need to be in a finally
            // block, to ensure that it is executed?
            clearCurrentRow();
            source.close();

            super.close();
        } 
        
        firstTime = false;
		rowsFetched = 0;

        closeTime += getElapsedMillis(beginTime);
	}

	public NoPutResultSet getSource() {
		return this.source;
	}
	
	@Override
	public long getTimeSpent(int type) {
        long totTime = constructorTime + openTime + nextTime + closeTime;

        if (type == CURRENT_RESULTSET_ONLY) 
            return  totTime - source.getTimeSpent(ENTIRE_RESULTSET_TREE);
        else 
            return totTime;
    }
}