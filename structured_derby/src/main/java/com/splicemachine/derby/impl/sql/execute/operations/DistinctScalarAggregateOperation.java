package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.derby.hbase.SpliceOperationCoprocessor;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.impl.sql.execute.Serializer;
import com.splicemachine.derby.impl.store.access.SpliceAccessManager;
import com.splicemachine.derby.stats.SinkStats;
import com.splicemachine.derby.stats.ThroughputStats;
import com.splicemachine.derby.utils.Puts;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.io.FormatableArrayHolder;
import org.apache.derby.iapi.services.loader.GeneratedMethod;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.execute.ExecIndexRow;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.NoPutResultSet;
import org.apache.derby.iapi.store.access.ColumnOrdering;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;


/**
 * 
 * There are two possible implementations:
 * 
 * option 1: current implementation: shuffle/sink does sort and filter by putting data into temp table;  
 * 			 scan/getNextRowCore does aggregates on the region and generates the final result in memory.
 * Option 2: use two sinks: first to sort/filter by putting data in temp table, second one to aggregate and save the 
 * 			 region aggregate result in temp table; scan/getNextRowCore does aggregates for all the regions (same as
 * 			 ScalarAggregateOperation).
 *
 * option 1 may incur more rpc calls, but the code is cleaner. 
 * option 2 (not implemented) requires two different sink functions in the same operation, which is currently not allowed by design
 * 
 *  @author jessiezhang
 */
public class DistinctScalarAggregateOperation extends ScalarAggregateOperation
{
	private static Logger LOG = Logger.getLogger(DistinctScalarAggregateOperation.class);
	
	private int orderingItem;
	private ColumnOrdering[] order;
	private int maxRowSize;

	protected int[] keyColumns;
	protected String keyColumnsString;
	protected ExecRow currrentSortedRow;

	public DistinctScalarAggregateOperation() {
		super();
	}
    /**
	 * Constructor
	 *
	 * @param	s			input result set
	 * @param	isInSortedOrder	true if the source results are in sorted order
	 * @param	aggregateItem	indicates the number of the
	 *		SavedObject off of the PreparedStatement that holds the
	 *		AggregatorInfoList used by this routine. 
	 * @param	a				activation
	 * @param	ra				generated method to build an empty
	 *	 	output row 
	 * @param	resultSetNumber	The resultSetNumber for this result set
	 *
	 * @exception StandardException Thrown on error
	 */
	public DistinctScalarAggregateOperation(NoPutResultSet s,
					boolean isInSortedOrder,
					int	aggregateItem,
					int	orderingItem,
					Activation a,
					GeneratedMethod ra,
					int maxRowSize,
					int resultSetNumber,
					boolean singleInputRow,
				    double optimizerEstimatedRowCount,
				    double optimizerEstimatedCost) throws StandardException 
	{
		super(s, isInSortedOrder, aggregateItem, a, ra,
			  resultSetNumber, 
			  singleInputRow,
			  optimizerEstimatedRowCount,
			  optimizerEstimatedCost);
		this.orderingItem = orderingItem;
		this.maxRowSize = maxRowSize; 
    }
	
	@Override
	public void init(SpliceOperationContext context){
		SpliceLogUtils.trace(LOG,"init");
		super.init(context);
		order = (ColumnOrdering[])
				((FormatableArrayHolder)(activation.getPreparedStatement().getSavedObject(orderingItem))).getArray(ColumnOrdering.class);

		keyColumns = new int[order.length];
		for (int index = 0; index < order.length; index++) {
			keyColumns[index] = order[index].getColumnId();
		}
	}

	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		SpliceLogUtils.trace(LOG,"writeExternal");
		super.writeExternal(out);
		out.writeInt(orderingItem);
	}

	@Override
	public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
		SpliceLogUtils.trace(LOG,"readExternal");
		super.readExternal(in);
		orderingItem = in.readInt();
	}
	
	@Override
    protected ExecIndexRow aggregate(ExecIndexRow execIndexRow, 
    		ExecIndexRow aggResult, boolean doInitialize,boolean isScan) throws StandardException{
    	if(aggResult==null){
    		aggResult = (ExecIndexRow)execIndexRow.getClone();
    		SpliceLogUtils.trace(LOG, "aggResult = %s aggregate before",aggResult);
    		if(doInitialize){
    			initializeScalarAggregation(aggResult);
    			SpliceLogUtils.trace(LOG, "aggResult = %s aggregate after",aggResult);
    		}
    	}else
    		accumulateScalarAggregation(execIndexRow, aggResult, true,isScan);
    	return aggResult;
    }
	
	@Override
	public SinkStats sink() {
        SinkStats.SinkAccumulator stats = SinkStats.uniformAccumulator();
        stats.start();
		SpliceLogUtils.trace(LOG, "sinking with sort based on column %d",orderingItem);
		ExecRow row = null;
		HTableInterface tempTable = null;
		Put put = null;
		try{
			tempTable = SpliceAccessManager.getFlushableHTable(SpliceOperationCoprocessor.TEMP_TABLE);
			Hasher hasher = new Hasher(((SpliceBaseOperation)source).getExecRowDefinition().getRowArray(),keyColumns,null,sequence[0]);
			byte[] scannedTableName = regionScanner.getRegionInfo().getTableName();
            Serializer serializer = new Serializer();

            do{
                long pTs = System.nanoTime();
                row = source.getNextRowCore();
                if(row==null) continue;

                stats.processAccumulator().tick(System.nanoTime()-pTs);

                pTs = System.nanoTime();
                SpliceLogUtils.trace(LOG, "row="+row);
                byte[] rowKey = hasher.generateSortedHashKeyWithPostfix(row.getRowArray(),scannedTableName);
                put = Puts.buildInsert(rowKey,row.getRowArray(),null,serializer);
                tempTable.put(put);

                stats.sinkAccumulator().tick(System.nanoTime()-pTs);
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
        return stats.finish();
	}
	
//	@Override
//	protected ExecRow doAggregation(boolean useScan) throws StandardException{
//		ExecIndexRow execIndexRow = null;
//		ExecIndexRow aggResult = null;
//		while ((execIndexRow = getNextRowFromScan(false))!=null){
//			SpliceLogUtils.trace(LOG,"userscan, aggResult =%s before",aggResult);
//			aggResult = aggregate(execIndexRow,aggResult,true,true);
//			SpliceLogUtils.trace(LOG,"userscan aggResult =%s after",aggResult);
//		}
//
//		SpliceLogUtils.trace(LOG, "aggResult=%s",aggResult);
//		if (aggResult==null)
//			return null;
//		if(countOfRows==0) {
//			aggResult = finishAggregation(aggResult);
//			setCurrentRow(aggResult);
//			countOfRows++;
//		}
//		return aggResult;
//	}
}
