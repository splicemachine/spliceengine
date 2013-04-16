package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.derby.hbase.SpliceOperationCoprocessor;
import com.splicemachine.derby.impl.sql.execute.Serializer;
import com.splicemachine.derby.impl.store.access.SpliceAccessManager;
import com.splicemachine.derby.stats.TaskStats;
import com.splicemachine.derby.utils.DerbyLogUtils;
import com.splicemachine.derby.utils.Puts;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.loader.GeneratedMethod;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.execute.ExecIndexRow;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.NoPutResultSet;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.shared.common.reference.SQLState;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Sink data into temp table to sort and filter out duplicate, then do grouped aggregates when iterating the rows
 * @author jessiezhang
 */
public class DistinctGroupedAggregateOperation extends GroupedAggregateOperation
{
	private static Logger LOG = Logger.getLogger(DistinctGroupedAggregateOperation.class);
	
	public DistinctGroupedAggregateOperation() {
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
	 * @param	orderingItem	indicates the number of the
	 *		SavedObject off of the PreparedStatement that holds the
	 *		ColumOrdering array used by this routine
	 * @param	a				activation
	 * @param	ra				generated method to build an empty
	 *	 	output row 
	 * @param	maxRowSize		approx row size, passed to sorter
	 * @param	resultSetNumber	The resultSetNumber for this result set
	 *
	 * @exception StandardException Thrown on error
	 */
    public DistinctGroupedAggregateOperation(NoPutResultSet s,
					boolean isInSortedOrder,
					int	aggregateItem,
					int	orderingItem,
					Activation a,
					GeneratedMethod ra,
					int maxRowSize,
					int resultSetNumber,
					double optimizerEstimatedRowCount,
					double optimizerEstimatedCost,
					boolean isRollup) throws StandardException 
	{
		super(s, isInSortedOrder, aggregateItem, orderingItem,
			  a, ra, maxRowSize, resultSetNumber, optimizerEstimatedRowCount, optimizerEstimatedCost, isRollup);
		recordConstructorTime();
    }
    
    @Override
	public TaskStats sink() {
        TaskStats.SinkAccumulator stats = TaskStats.uniformAccumulator();
        stats.start();
        SpliceLogUtils.trace(LOG, ">>>>statistics starts for sink for DistinctGroupedAggregationOperation at "+stats.getStartTime());
		SpliceLogUtils.trace(LOG, "sinking with sort based on column %d",orderingItem);
		ExecRow row = null;
		HTableInterface tempTable = null;
		Put put = null;
        Serializer serializer = new Serializer();
		try{
			tempTable = SpliceAccessManager.getFlushableHTable(SpliceOperationCoprocessor.TEMP_TABLE);
			Hasher hasher = new Hasher(((SpliceBaseOperation)source).getExecRowDefinition().getRowArray(),keyColumns,null,sequence[0]);
			byte[] scannedTableName = regionScanner.getRegionInfo().getTableName();
            do{
                long processTime = System.nanoTime();
                row = source.getNextRowCore();
                if(row==null)continue;
                stats.readAccumulator().tick(System.nanoTime()-processTime);

                processTime = System.nanoTime();
                SpliceLogUtils.trace(LOG, "row="+row);
                byte[] rowKey = hasher.generateSortedHashKeyWithPostfix(row.getRowArray(),scannedTableName);
                put = Puts.buildTempTableInsert(rowKey, row.getRowArray(), null, serializer);
                tempTable.put(put);

                stats.writeAccumulator().tick(System.nanoTime()-processTime);
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
		SpliceLogUtils.trace(LOG, ">>>>statistics finishes for sink for DistinctGroupedAggregationOperation at "+stats.getFinishTime());
        return ss;
	}
    
    @Override
	public ExecRow getExecRowDefinition() {
		SpliceLogUtils.trace(LOG,"getExecRowDefinition");
		ExecRow row = sourceExecIndexRow.getClone();
		try{
			DataValueDescriptor[] descs = new DataValueDescriptor[order.length+aggregates.length];
			for(int i=0;i<order.length;i++){
				descs[i] = row.getColumn(order[i].getColumnId()+1);
			}
			
			for(int i=0;i<aggregates.length;i++){
				descs[i+order.length] = row.getColumn(aggregates[i].getAggregatorInfo().getAggregatorColNum());
			}
			row.setRowArray(descs);
		}catch(StandardException se){
			SpliceLogUtils.logAndThrowRuntime(LOG, se);
		}
		DerbyLogUtils.traceDescriptors(LOG, "getExecRowDefinition row", row.getRowArray());
		return row;
	}
    
    @Override
    protected ExecIndexRow getNextRowFromScan() throws StandardException {
    	SpliceLogUtils.trace(LOG,"getting next row from scan");
    	ExecIndexRow inputRow = null;
    	if(rowProvider!=null){
    		if(rowProvider.hasNext() && (inputRow = (ExecIndexRow)rowProvider.next())!=null){
    			SpliceLogUtils.trace(LOG,"inputRow="+inputRow);
    			initializeVectorAggregation(inputRow);
    			return inputRow;

    		}
    		else return null;
    	}else{
			List<KeyValue> keyValues = new ArrayList<KeyValue>();
			try{
				regionScanner.next(keyValues);
			}catch(IOException ioe){
				SpliceLogUtils.logAndThrow(LOG, StandardException.newException(SQLState.DATA_UNEXPECTED_EXCEPTION,ioe));
			}
			
			Result result = new Result(keyValues);
			if (keyValues.isEmpty())
				return null;
			else{
				inputRow = (ExecIndexRow)sourceExecIndexRow.getClone();
				SpliceUtils.populate(result, null, inputRow.getRowArray());
				SpliceLogUtils.trace(LOG,"inputRow="+inputRow);
				
				if(inputRow!=null)
					initializeVectorAggregation(inputRow);
				
				return inputRow;
			}
		}
	}
}
