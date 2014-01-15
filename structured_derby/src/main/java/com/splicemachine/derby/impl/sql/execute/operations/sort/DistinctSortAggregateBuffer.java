package com.splicemachine.derby.impl.sql.execute.operations.sort;

import org.apache.derby.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.impl.sql.execute.operations.framework.AbstractAggregateBuffer;
import com.splicemachine.derby.impl.sql.execute.operations.framework.BufferedAggregator;
import com.splicemachine.derby.impl.sql.execute.operations.framework.SpliceGenericAggregator;
import com.splicemachine.derby.utils.StandardSupplier;

/**
 * @author Scott Fines
 * Created on: 11/1/13
 */
public class DistinctSortAggregateBuffer extends AbstractAggregateBuffer {
    protected final StandardSupplier<ExecRow> emptyRowSupplier;
    
    public DistinctSortAggregateBuffer(int maxSize,
                           SpliceGenericAggregator[] aggregators, StandardSupplier<ExecRow> emptyRowSupplier){
    	super(maxSize,aggregators);
    	this.emptyRowSupplier = emptyRowSupplier;
    }
        
	@Override
	public BufferedAggregator createBufferedAggregator() {
		return new DistinctSortBufferedAggregator(emptyRowSupplier);
	}

	@Override
	public void intializeAggregator() {
		values = new DistinctSortBufferedAggregator[bufferSize];
	}
}
