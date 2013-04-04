package com.splicemachine.derby.impl.sql.execute.operations;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.NoPutResultSet;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.log4j.Logger;

import com.splicemachine.derby.hbase.SpliceObserverInstructions;
import com.splicemachine.derby.hbase.SpliceOperationCoprocessor;
import com.splicemachine.derby.iapi.sql.execute.SpliceNoPutResultSet;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.iapi.storage.RowProvider;
import com.splicemachine.derby.impl.sql.execute.Serializer;
import com.splicemachine.derby.impl.storage.ClientScanProvider;
import com.splicemachine.derby.impl.storage.RowProviders;
import com.splicemachine.derby.impl.store.access.SpliceAccessManager;
import com.splicemachine.derby.impl.store.access.hbase.HBaseRowLocation;
import com.splicemachine.derby.stats.RegionStats;
import com.splicemachine.derby.stats.SinkStats;
import com.splicemachine.derby.utils.*;
import com.splicemachine.utils.SpliceLogUtils;

/**
 * In Derby world, UnionResultSet is a pass-through: work/operation is passed to the left and right sub-operations 
 * (ResultSets) and the results from the lower left and right operations are passed to SortResultSet to combine-sort-distinct.
 * 
 * In our current design, UnionOperation could not be a pass-through because of RegionScanner issue, the current temp 
 * workaround implementation is to sink both left and right sub-operations to a temp table, then 
 * a. for Union, pass this temp table resultset (UnionOperation) to an upper operation SortOperation to do sort & distinct.
 * b. for Union All, create a SpliceNoPutResultSet for client to use (no need for SortOperation)
 * 
 * The current implementation is a workaround because the data are written into temp table twice in Union case: first in 
 * UnionOperation to combine the two result sets, then in SortOperation to sort-make unique if needed.
 * 
 * @author jessiezhang
 *
 */

public class UnionOperation extends SpliceBaseOperation {
	private static Logger LOG = Logger.getLogger(UnionOperation.class);
	
    public NoPutResultSet source1;
    public NoPutResultSet source2;
    
    private int whichSource = 1; // 1 or 2, == the source we are currently on.
    
	/* Run time statistics variables */
	public int rowsSeenLeft;
	public int rowsSeenRight;
	public int rowsReturned;
	
	protected Scan reduceScan;
	private boolean isScan = true;
	//protected HTableInterface tempTable = null;
	
	protected static List<NodeType> nodeTypes = Arrays.asList(NodeType.REDUCE,NodeType.SINK,NodeType.SCAN); 
	protected static List<NodeType> sequentialNodeTypes = Arrays.asList(NodeType.SCAN);
	
    public UnionOperation () {
    	super();
    }
    
    public UnionOperation (NoPutResultSet source1,
    		NoPutResultSet source2,
    		Activation activation,
    		int resultSetNumber,
    		double optimizerEstimatedRowCount,
    		double optimizerEstimatedCost) throws StandardException {
    	super(activation,resultSetNumber,optimizerEstimatedRowCount,optimizerEstimatedCost);
    	SpliceLogUtils.trace(LOG, "source1="+source1+", source2="+source2+",whichSource"+whichSource);
    	this.source1 = source1;
    	this.source2 = source2;
    	init(SpliceOperationContext.newContext(activation));

    	SpliceLogUtils.trace(LOG, "statisticsTimingOn="+statisticsTimingOn+",isTopResultSet="+isTopResultSet);
        
    	recordConstructorTime();
    }
    
	@Override
	public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
		SpliceLogUtils.trace(LOG,"readExternal");
		super.readExternal(in);
		source1 = (SpliceOperation)in.readObject();
		source2 = (SpliceOperation)in.readObject();
		whichSource = in.readInt();
	}

	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		SpliceLogUtils.trace(LOG,"writeExternal");
		super.writeExternal(out);
		out.writeObject((SpliceOperation)source1);
		out.writeObject((SpliceOperation)source2);
		out.writeInt(whichSource);
	}
	
	@Override
	public List<NodeType> getNodeTypes() {
		SpliceLogUtils.trace(LOG, "getNodeTypes");
		return isScan? nodeTypes: sequentialNodeTypes;
	}
	
	@Override
	public List<SpliceOperation> getSubOperations() {
		SpliceLogUtils.trace(LOG, "getSubOperations");
		List<SpliceOperation> operations = new ArrayList<SpliceOperation>();
		operations.add((SpliceOperation) source1);
		operations.add((SpliceOperation) source2);
		return operations;
	}

	@Override
	public void init(SpliceOperationContext context){
		SpliceLogUtils.trace(LOG, "UnionOperation init called,whichSource="+whichSource);
		super.init(context);
		
		((SpliceOperation)source1).init(context);
		((SpliceOperation)source2).init(context);
		
		boolean hasScan = false;
		List<SpliceOperation> leftOpStack = getOperations();
		for(SpliceOperation op: leftOpStack){
			if(op instanceof ScanOperation){
				hasScan = true;
				break;
			}
		}
		if(!hasScan){
			leftOpStack.clear();
			((SpliceOperation)source2).generateLeftOperationStack(leftOpStack);
			for(SpliceOperation op: leftOpStack){
				if(op instanceof ScanOperation){
					hasScan = true;
					break;
				}
			}
		}
		isScan = hasScan;
	}
	
	private List<SpliceOperation> getOperations(){
		SpliceLogUtils.trace(LOG, "getOperations");
		List<SpliceOperation> operations = new ArrayList<SpliceOperation>();
		generateLeftOperationStack(operations);
		return operations;
	}	
	
	@Override
	public void executeShuffle() throws StandardException {
		SpliceLogUtils.trace(LOG,"executeShuffle");
		long start = System.currentTimeMillis();
		final List<SpliceOperation> opStack = getOperations();
		final SpliceOperation topOperation = getOperations().get(opStack.size()-1);
		nextTime += System.currentTimeMillis() - start;
		
		whichSource = 1;
		executeShuffle((SpliceOperation) source1, topOperation);
		
		whichSource = 2;
		executeShuffle((SpliceOperation) source2, topOperation);
		
		//whichSource = 1;
	}
	
	protected void executeShuffle(SpliceOperation regionOperation, final SpliceOperation topOperation) throws StandardException {
		SpliceLogUtils.trace(LOG,"regionOperation="+regionOperation);
		long start = System.currentTimeMillis();
		RowProvider provider = regionOperation.getMapRowProvider(topOperation,regionOperation.getExecRowDefinition());
        nextTime += System.currentTimeMillis() - start;

		HTableInterface htable = null;
		try{
			final RegionStats stats = new RegionStats("UnionOperation's "+regionOperation.getClass().getName());
            stats.start();
            
			long numberCreated = 0;
			final SpliceObserverInstructions soi = SpliceObserverInstructions.create(activation,topOperation);
            provider.shuffleRows(soi,stats);
            stats.finish();
            stats.recordStats(LOG);
            nextTime += stats.getTotalTimeTakenMs();
			SpliceLogUtils.trace(LOG,"Retrieved %d records",numberCreated);
			executed = true;
		}finally{
			if (htable != null){
				try {
					htable.close();
				}catch(IOException e){
					SpliceLogUtils.logAndThrow(LOG,"Unable to close HBase table",StandardException.newException(SQLState.DATA_UNEXPECTED_EXCEPTION,e));
				}
			}
		}
	}

	@Override
	public void close() throws StandardException {
		SpliceLogUtils.trace(LOG, "close in Union, whichSource="+whichSource);
		beginTime = getCurrentTimeMillis();
		clearCurrentRow();

		switch (whichSource) {
			case 1 : source1.close();
					break;
			case 2 : source2.close();
					whichSource = 1;
					break;
			default: 
					break;
		}
		super.close();
		closeTime += getElapsedMillis(beginTime);
	}
	
	@Override
	public void finish() throws StandardException {
		SpliceLogUtils.trace(LOG, "finish");
		source1.finish();
		source2.finish();
		super.finish();
	}
	
	@Override
	public SpliceOperation getLeftOperation() {
		SpliceLogUtils.trace(LOG, "getLeftOperation");
		return (SpliceOperation) this.source1;
	}

	@Override
	public void cleanup() {
		SpliceLogUtils.debug(LOG, "cleanup");
		/*try {
			tempTable.close();
		} catch (IOException e) {
			LOG.error("Error closing Temp Table",e);
			e.printStackTrace();
			throw new RuntimeException(e);
		}*/
	}
	
	public boolean shouldContinue() {
		return false;
	}
	
	@Override
	public void	openCore() throws StandardException 
	{
        super.openCore();
        source1.openCore();
        source2.openCore();
//		SpliceLogUtils.trace(LOG, "openCore");
//		switch (whichSource) {
//			case 1: source1.openCore();
//				break;
//			case 2: source2.openCore();
//				if (source1 != null)
//					source1.close();
//				break;
//			default:
//				break;
//		}
	}
	
	@Override
	public ExecRow	getNextRowCore() throws StandardException {
		SpliceLogUtils.trace(LOG, "getNextRowCore, whichSource=%s,regionScanner=%s,isScan=%b",whichSource,regionScanner,isScan);
		return isScan? getNextRowFromScan() : getNextRowFromSources();
	}

	private ExecRow getNextRowFromScan() throws StandardException {
		List<KeyValue> keyValues = new ArrayList<KeyValue>();
		try {
			regionScanner.next(keyValues);
			SpliceLogUtils.trace(LOG,"getNextRowCore, keyvalue="+keyValues);
			Result result = new Result(keyValues);
			if (keyValues.isEmpty()) {
				currentRow = null;
				currentRowLocation = null;
			} else {
				if (currentRow == null) {
					SpliceLogUtils.trace(LOG,"getNextRowCore, currentRow is null");
					currentRow = getExecRowDefinition().getClone();
				}
				SpliceLogUtils.trace(LOG,"getNextRowCore, currentrow="+currentRow+",keyvalue="+keyValues);
				SpliceUtils.populate(result, currentRow.getRowArray());
				currentRowLocation = new HBaseRowLocation(result.getRow());
				SpliceLogUtils.trace(LOG, "getNextRowCore with keyValues "+ keyValues +", and current Row "+ currentRow);
			}
		} catch (Exception e) {
			SpliceLogUtils.logAndThrow(LOG, "Error during getNextRowCore", StandardException.newException(SQLState.DATA_UNEXPECTED_EXCEPTION,e));
		}
		setCurrentRow(currentRow);
		return currentRow;
	}
	
	protected ExecRow	getNextRowFromSources() throws StandardException {
		SpliceLogUtils.trace(LOG, "getNextRowFromSources, whichSource="+whichSource+",source1="+source1+",source2="+source2);
	    ExecRow result = null;
	    //if (isOpen) {
	        switch (whichSource) {
	            case 1 : result = source1.getNextRowCore();
	            		SpliceLogUtils.trace(LOG, "getNextRowFromSources,result from source 1="+result);
	                     if ( result == (ExecRow) null ) {
	                        source1.close();
	                        whichSource = 2;
	                        SpliceLogUtils.trace(LOG, "getNextRowFromSources, open source 2 since result from source 1 is null");
	                        source2.openCore();
	                        SpliceLogUtils.trace(LOG, "getNextRowFromSources, getNextRowCore from source 2");
	                        result = source2.getNextRowCore();
							if (result != null) {
								SpliceLogUtils.trace(LOG, "UnionOperation from source 1 to source 2, getNextRowFromSources="+result);
								rowsSeenRight++;
							}
	                     } else {
	                    	 SpliceLogUtils.trace(LOG, "UnionOperation getNextRowFromSources from source 1="+result);
							 rowsSeenLeft++;
	                     }
	                     break;
	            case 2 : result = source2.getNextRowCore();
						 if (result != null) {
							SpliceLogUtils.trace(LOG, "UnionOperation source=2, getNextRowFromSources="+result);
							rowsSeenRight++;
						 } else {
							 whichSource = 1;
							 SpliceLogUtils.trace(LOG, "UnionOperation source2 has no more getNextRowCore");
						 }
	                     break;
	            default: 
	            	//SpliceLogUtils.logAndThrow(LOG, "Bad source number in union", StandardException);
	                break;
	        //}
	    }

		setCurrentRow(result);
		if (result != null)
			rowsReturned++;

	    return result;
	}
	
	@Override
	public SinkStats sink() {
		SpliceLogUtils.trace(LOG, "sink");
        SinkStats.SinkAccumulator stats = SinkStats.uniformAccumulator();
        stats.start();
        SpliceLogUtils.trace(LOG, ">>>>statistics starts for sink for UnionOperation at "+stats.getStartTime());
		ExecRow row = null;
		HTableInterface tempTable = null;
		Put put = null;
		
		try{
			tempTable = SpliceAccessManager.getFlushableHTable(SpliceOperationCoprocessor.TEMP_TABLE);
//			openCore();
            Serializer serializer= new Serializer();
            do{
                long start = System.nanoTime();
                row = getNextRowFromSources();
                if(row==null) continue;
                stats.processAccumulator().tick(System.nanoTime()-start);

                start = System.nanoTime();
                SpliceLogUtils.trace(LOG, "UnionOperation sink, row="+row);
				/* Need to use non-sorted non-hashed rowkey with the prefix
				put = SpliceUtils.insert(row.getRowArray(),
										 DerbyBytesUtil.generateSortedHashKey(row.getRowArray(), sequence[0], keyColumns, null),
										 null);
										 */
                put = Puts.buildTempTableInsert(DerbyBytesUtil.generatePrefixedRowKey(sequence[0]), row.getRowArray(), null, serializer);
                tempTable.put(put);

                stats.sinkAccumulator().tick(System.nanoTime()-start);
            }while(row!=null);
			tempTable.flushCommits();
		} catch (StandardException se){
			SpliceLogUtils.logAndThrowRuntime(LOG,se);
		} catch (IOException e) {
			SpliceLogUtils.logAndThrowRuntime(LOG, e);
		}
		finally{
			try {
				if(tempTable!=null)
					tempTable.close();
			} catch (IOException e) {
				SpliceLogUtils.error(LOG, "Unexpected error closing TempTable", e);
			}
		}
        //return stats.finish();
		SinkStats ss = stats.finish();
		SpliceLogUtils.trace(LOG, ">>>>statistics finishes for sink for UnionOperation at "+stats.getFinishTime());
        return ss;
	}
	
	@Override
	public NoPutResultSet executeScan() throws StandardException {
		SpliceLogUtils.trace(LOG,"executeScan");
		final List<SpliceOperation> opStack = new ArrayList<SpliceOperation>();
		this.generateLeftOperationStack(opStack);
		SpliceLogUtils.trace(LOG,"operationStack=%s",opStack);
		
		SpliceOperation regionOperation = opStack.get(opStack.size()-1); 
		RowProvider provider;
		if (regionOperation.getNodeTypes().contains(NodeType.REDUCE)){
			provider = regionOperation.getReduceRowProvider(this,getExecRowDefinition());
		}else {
			provider = regionOperation.getMapRowProvider(this,getExecRowDefinition());
		}
		return new SpliceNoPutResultSet(activation,this,provider);
	}
	
	@Override
	public RowProvider getReduceRowProvider(SpliceOperation top, ExecRow template) throws StandardException {
        try {
            reduceScan = Scans.buildPrefixRangeScan(sequence[0],transactionID);
        } catch (IOException e) {
            throw Exceptions.parseException(e);
        }
		SpliceUtils.setInstructions(reduceScan, activation, top);
		return new ClientScanProvider(SpliceOperationCoprocessor.TEMP_TABLE, reduceScan, template,null);
	}

    @Override
    public RowProvider getMapRowProvider(SpliceOperation top, ExecRow template){
        return RowProviders.sourceProvider(top,LOG);
    }

	@Override
	public ExecRow getExecRowDefinition() {
		SpliceLogUtils.trace(LOG, "getExecRowDefinition");

//		currentRow = ((SpliceOperation)source1).getExecRowDefinition();
		switch (whichSource)
		{
	        case 1: 
	        	currentRow = ((SpliceOperation)source1).getExecRowDefinition();
	            break;

	        case 2: 
	        	currentRow = ((SpliceOperation) source2).getExecRowDefinition();
	            break;
        }
		SpliceLogUtils.trace(LOG, "getExecRowDefinition, whichSource="+whichSource+",currentRow="+currentRow);
        setCurrentRow(currentRow);
		return this.currentRow;														
	}

	@Override
	public long getTimeSpent(int type)
	{
		long totTime = constructorTime + openTime + nextTime + closeTime;

		if (type == NoPutResultSet.CURRENT_RESULTSET_ONLY)
			return	totTime - source1.getTimeSpent(ENTIRE_RESULTSET_TREE) 
							- source2.getTimeSpent(ENTIRE_RESULTSET_TREE);
		else
			return totTime;
	}
	
	@Override
	public String toString() {
		return "UnionOp {left="+source1+",right="+source2+"}";
	}
}
