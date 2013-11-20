package com.splicemachine.derby.impl.storage;

import com.splicemachine.derby.hbase.SpliceObserverInstructions;
import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.iapi.storage.RowProvider;
import com.splicemachine.derby.impl.sql.execute.operations.SpliceGenericAggregator;
import com.splicemachine.derby.utils.marshall.PairDecoder;
import com.splicemachine.derby.utils.marshall.RowDecoder;
import com.splicemachine.job.JobResults;
import com.splicemachine.job.JobStats;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.execute.ExecAggregator;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.RowLocation;
import org.apache.hadoop.hbase.client.Scan;

import java.io.IOException;
import java.util.Arrays;

/**
 * When no row is returned from the actual operation, this provides a default value ONCE.
 *
 * @author Scott Fines
 * Created on: 5/21/13
 */
public class ScalarAggregateRowProvider implements RowProvider {
    private boolean defaultReturned = false;

    private final ExecAggregator[] execAggregators;
    private final SpliceGenericAggregator[] genericAggregators;
    private final int[] colPosMap;
		private final RowProvider delegate;
		private ExecRow templateRow;
		private boolean populated = false;

		public ScalarAggregateRowProvider(ExecRow templateRow,
                                      SpliceGenericAggregator[] aggregates,
																			RowProvider delegate) throws StandardException {
				this.delegate = delegate;
				this.templateRow =templateRow;
        this.genericAggregators = aggregates;
        this.execAggregators = new ExecAggregator[genericAggregators.length];
        int []columnMap = new int[execAggregators.length];
        int maxPos = 0;
        for(int i=0;i<genericAggregators.length;i++){
            execAggregators[i] = genericAggregators[i].getAggregatorInstance();
            columnMap[i] = genericAggregators[i].getResultColumnId();
            if(columnMap[i]>maxPos){
                maxPos = columnMap[i];
            }
        }
        this.colPosMap = new int[maxPos+1];
        Arrays.fill(colPosMap,-1);
        for(int i=0;i<columnMap.length;i++){
            colPosMap[columnMap[i]] = i;
        }
    }

		/*delegate methods*/
		@Override public void open() throws StandardException { delegate.open(); }
		@Override public void close() throws StandardException { delegate.close(); }
		@Override public RowLocation getCurrentRowLocation() { return delegate.getCurrentRowLocation(); }
		@Override public byte[] getTableName() { return delegate.getTableName(); }
		@Override public int getModifiedRowCount() { return delegate.getModifiedRowCount(); }

		@Override
		public JobResults shuffleRows(SpliceObserverInstructions instructions) throws StandardException {
				return delegate.shuffleRows(instructions);
		}

		@Override
		public SpliceRuntimeContext getSpliceRuntimeContext() {
				return delegate.getSpliceRuntimeContext();
		}

		@Override
    public ExecRow next() throws StandardException, IOException {
				if(populated&&defaultReturned){
						populated=false;
						return templateRow;
				}

        ExecRow finalRow = null;
        while(hasNext()){
            ExecRow row = delegate.next();
            if(finalRow==null)
                finalRow = row.getClone();
            for(int i=0;i<genericAggregators.length;i++){
                ExecAggregator aggregate = execAggregators[i];
                SpliceGenericAggregator genericAgg = genericAggregators[i];
                DataValueDescriptor column = row.getColumn(colPosMap[genericAgg.getResultColumnId()] + 1);
                /*
                 * For some reason, sometimes we get aggregators that aren't reflected
                 * in the final answer. These should be ignored.
                 */
                if(column!=null)
                    aggregate.add(column);
            }
        }

        if(finalRow!=null){
            for(int i=0;i<genericAggregators.length;i++){
                ExecAggregator aggregate = execAggregators[i];
                SpliceGenericAggregator genericAgg = genericAggregators[i];
                finalRow.setColumn(colPosMap[genericAgg.getResultColumnId()] + 1, aggregate.getResult());
            }
        }

        return finalRow;
    }

    @Override
    public boolean hasNext() throws StandardException, IOException {
        boolean hasNext = delegate.hasNext();
        if(hasNext){
            defaultReturned =true;
            return hasNext;
        }else if(!defaultReturned){
            defaultReturned = true;
            populated = true;
            return true;
        }else return false;
    }
}
