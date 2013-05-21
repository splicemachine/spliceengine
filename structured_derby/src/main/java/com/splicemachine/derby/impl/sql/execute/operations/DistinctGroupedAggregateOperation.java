package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.derby.hbase.SpliceOperationCoprocessor;
import com.splicemachine.derby.impl.sql.execute.Serializer;
import com.splicemachine.derby.impl.store.access.SpliceAccessManager;
import com.splicemachine.derby.stats.TaskStats;
import com.splicemachine.derby.utils.DerbyLogUtils;
import com.splicemachine.derby.utils.Exceptions;
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
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.log4j.Logger;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
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
    public OperationSink.Translator getTranslator() throws IOException {
        final Hasher hasher = new Hasher(getExecRowDefinition().getRowArray(),keyColumns,null,sequence[0]);
        final Serializer serializer = new Serializer();
        final byte[] scannedTableName = regionScanner.getRegionInfo().getTableName();
        return new OperationSink.Translator() {
            @Nonnull
            @Override
            public List<Mutation> translate(@Nonnull ExecRow row) throws IOException {
                try {
                    byte[] rowKey = hasher.generateSortedHashKeyWithPostfix(row.getRowArray(),scannedTableName);
                    Put put = Puts.buildTempTableInsert(rowKey,row.getRowArray(),null,serializer);
                    return Collections.<Mutation>singletonList(put);
                } catch (StandardException e) {
                    throw Exceptions.getIOException(e);
                }
            }
        };
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
