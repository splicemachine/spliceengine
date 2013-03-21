package com.splicemachine.derby.impl.sql.execute.operations;

<<<<<<< HEAD
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

=======
import com.splicemachine.derby.hbase.SpliceOperationCoprocessor;
import com.splicemachine.derby.iapi.sql.execute.SpliceNoPutResultSet;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.iapi.storage.RowProvider;
import com.splicemachine.derby.impl.sql.execute.Serializer;
import com.splicemachine.derby.impl.store.access.SpliceAccessManager;
import com.splicemachine.derby.stats.SinkStats;
import com.splicemachine.derby.utils.DerbyBytesUtil;
import com.splicemachine.derby.utils.Puts;
import com.splicemachine.utils.SpliceLogUtils;
>>>>>>> data_generator
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

import com.splicemachine.derby.hbase.SpliceOperationCoprocessor;
import com.splicemachine.derby.iapi.sql.execute.SpliceNoPutResultSet;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.iapi.storage.RowProvider;
import com.splicemachine.derby.impl.sql.execute.Serializer;
import com.splicemachine.derby.impl.store.access.SpliceAccessManager;
import com.splicemachine.derby.stats.SinkStats;
import com.splicemachine.derby.utils.DerbyBytesUtil;
import com.splicemachine.derby.utils.Puts;
import com.splicemachine.utils.SpliceLogUtils;

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
	public void init(SpliceOperationContext context){
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
	public ExecRow getExecRowDefinition() {
		SpliceLogUtils.trace(LOG,"getExecRowDefinition");
		ExecRow def = ((SpliceOperation)source).getExecRowDefinition();
		source.setCurrentRow(def);
		return def;
	}

	/*
	@Override
	public void executeShuffle() throws StandardException {
		SpliceLogUtils.trace(LOG,"executeShuffle - SHOULD NOT BE CALLED?");
		final List<SpliceOperation> opStack = new ArrayList<SpliceOperation>();
		generateLeftOperationStack(opStack);
		SpliceLogUtils.trace(LOG,"operationStack="+opStack);
		final SpliceOperation regionOperation = opStack.get(opStack.size()-1);
		SpliceLogUtils.trace(LOG,"regionOperation="+regionOperation);
		SpliceLogUtils.trace(LOG,"regionOperation=this?"+(this==regionOperation));
		final byte[] table;
		final Scan scan;
		if(this == regionOperation){
			table = Bytes.toBytes(regionOperation.getMapTable());
			scan = regionOperation.getMapScan();
		}else {
			table = SpliceOperationCoprocessor.TEMP_TABLE;
			scan = regionOperation.getReduceScan();
		}
		HTableInterface htable = null;
		try{
			htable = SpliceAccessManager.getHTable(table);
			long numberCreated = 0;
			Map<byte[], Long> results = htable.coprocessorExec(SpliceOperationProtocol.class,
																scan.getStartRow(),scan.getStopRow(),
																new Batch.Call<SpliceOperationProtocol,Long>(){
				@Override
				public Long call(
						SpliceOperationProtocol instance)
								throws IOException {
					try{
						return instance.run((GenericStorablePreparedStatement)activation.getPreparedStatement(), scan, regionOperation);
					}catch(StandardException se){
						SpliceLogUtils.logAndThrow(LOG,"Unexpected error executing coprocessor",new IOException(se));
						return null;
					}
				}
			});
			for(Long returnedRow : results.values()){
				numberCreated +=returnedRow;
			}
			SpliceLogUtils.trace(LOG, "sunk "+ numberCreated+" rows");
			executed = true;
		} catch (IOException ioe){
			if(ioe.getCause() instanceof StandardException)
				SpliceLogUtils.logAndThrow(LOG,(StandardException)ioe.getCause());
			else
				SpliceLogUtils.logAndThrow(LOG, StandardException.newException(SQLState.DATA_UNEXPECTED_EXCEPTION,ioe));
		}catch (Throwable e) {
			SpliceLogUtils.logAndThrow(LOG, StandardException.newException(SQLState.DATA_UNEXPECTED_EXCEPTION,e));
		}finally{
			if (htable != null){
				try {
					htable.close();
				}catch(IOException e){
					SpliceLogUtils.logAndThrow(LOG,"Unable to close HBase table",StandardException.newException(SQLState.DATA_UNEXPECTED_EXCEPTION,e));
				}
			}
		}
	}*/

	@Override
	public NoPutResultSet executeScan() {
		SpliceLogUtils.trace(LOG, "executeScan");
		final List<SpliceOperation> operationStack = new ArrayList<SpliceOperation>();
		this.generateLeftOperationStack(operationStack);
		SpliceOperation regionOperation = operationStack.get(0);
		SpliceLogUtils.trace(LOG,"regionOperation="+regionOperation);
//		final String table;
//		final Scan scan;
		RowProvider provider;
		if(regionOperation.getNodeTypes().contains(NodeType.REDUCE) && this != regionOperation){
			provider = regionOperation.getReduceRowProvider(this,getExecRowDefinition());
//			table = SpliceOperationCoprocessor.TEMP_TABLE_STR;
//			scan = regionOperation.getReduceScan();
		}else {
			provider = regionOperation.getMapRowProvider(this,getExecRowDefinition());
//			table = regionOperation.getMapTable();
//			scan = regionOperation.getMapScan();
		}
		return new SpliceNoPutResultSet(activation,this,provider);
	}

	@Override
	public SinkStats sink() { // gd not sure I want any of this, or at least the sorted part
        SinkStats.SinkAccumulator stats = SinkStats.uniformAccumulator();
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
                stats.processAccumulator().tick(System.nanoTime()-start);

                start = System.nanoTime();
                SpliceLogUtils.trace(LOG, "row="+row);
                byte[] rowKey = DerbyBytesUtil.generateSortedHashKey(row.getRowArray(),sequence[0],null,null);
                put = Puts.buildInsert(rowKey,row.getRowArray(),null,serializer);

                tempTable.put(put);

                stats.sinkAccumulator().tick(System.nanoTime()-start);
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
		SinkStats ss = stats.finish();
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