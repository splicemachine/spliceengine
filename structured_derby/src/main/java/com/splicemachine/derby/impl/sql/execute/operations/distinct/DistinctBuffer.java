package com.splicemachine.derby.impl.sql.execute.operations.distinct;

import com.splicemachine.derby.impl.sql.execute.operations.framework.AbstractAggregateBuffer;
import com.splicemachine.derby.impl.sql.execute.operations.framework.BufferedAggregator;
import com.splicemachine.derby.utils.StandardSupplier;
import org.apache.derby.iapi.sql.execute.ExecRow;

/**
 * @author Scott Fines
 * Created on: 11/1/13
 */
public class DistinctBuffer extends AbstractAggregateBuffer {
    private final StandardSupplier<ExecRow> emptyRowSupplier;

    public DistinctBuffer(int maxSize,
                           StandardSupplier<ExecRow> emptyRowSupplier) {
		super(maxSize,null);
		this.emptyRowSupplier = emptyRowSupplier;
    }
        
	@Override
	public BufferedAggregator createBufferedAggregator() {
		return new DistinctBufferedAggregator(emptyRowSupplier);		
	}
	@Override
	public void intializeAggregator() {
        this.values = new DistinctBufferedAggregator[bufferSize];
	}

}
