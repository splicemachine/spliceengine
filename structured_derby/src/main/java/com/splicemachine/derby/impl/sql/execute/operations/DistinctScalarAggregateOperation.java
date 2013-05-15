package com.splicemachine.derby.impl.sql.execute.operations;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.List;

import com.splicemachine.derby.stats.TaskStats;
import com.splicemachine.derby.utils.Exceptions;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.io.FormatableArrayHolder;
import org.apache.derby.iapi.services.loader.GeneratedMethod;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.execute.ExecIndexRow;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.NoPutResultSet;
import org.apache.derby.iapi.store.access.ColumnOrdering;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.log4j.Logger;

import com.splicemachine.derby.hbase.SpliceOperationCoprocessor;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.impl.sql.execute.Serializer;
import com.splicemachine.derby.impl.store.access.SpliceAccessManager;
import com.splicemachine.derby.utils.Puts;
import com.splicemachine.utils.SpliceLogUtils;

import javax.annotation.Nonnull;


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
		recordConstructorTime();
    }
	
	@Override
	public void init(SpliceOperationContext context) throws StandardException{
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
    public OperationSink.Translator getTranslator() throws IOException {
        final Hasher hasher = new Hasher(getExecRowDefinition().getRowArray(),keyColumns,null,sequence[0]);
        final byte[] scannedTableName = regionScanner.getRegionInfo().getTableName();
        final Serializer serializer = new Serializer();
        return new OperationSink.Translator() {
            @Nonnull
            @Override
            public List<Mutation> translate(@Nonnull ExecRow row) throws IOException {
                try {
                    byte[] rowKey = hasher.generateSortedHashKeyWithPostfix(row.getRowArray(),scannedTableName);
                    Put put = Puts.buildTempTableInsert(rowKey, row.getRowArray(), null, serializer);
                    return Collections.<Mutation>singletonList(put);
                } catch (StandardException e) {
                    throw Exceptions.getIOException(e);
                }
            }
        };
    }

	@Override
	public void close() throws StandardException
    {
		SpliceLogUtils.trace(LOG, "close in DistinctScalarAggregate");
        super.close();
        source.close();
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
