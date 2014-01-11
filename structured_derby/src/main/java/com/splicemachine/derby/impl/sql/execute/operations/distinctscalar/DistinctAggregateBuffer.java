package com.splicemachine.derby.impl.sql.execute.operations.distinctscalar;

import org.apache.derby.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.impl.sql.execute.operations.WarningCollector;
import com.splicemachine.derby.impl.sql.execute.operations.framework.AbstractAggregateBuffer;
import com.splicemachine.derby.impl.sql.execute.operations.framework.BufferedAggregator;
import com.splicemachine.derby.impl.sql.execute.operations.framework.SpliceGenericAggregator;
import com.splicemachine.derby.utils.StandardSupplier;

/**
 * @author Scott Fines
 * Created on: 11/1/13
 */
public class DistinctAggregateBuffer extends AbstractAggregateBuffer {
    private final StandardSupplier<ExecRow> emptyRowSupplier;
    private final WarningCollector warningCollector;
    private boolean isSorted;
    private boolean alreadyInitialized;
	public static enum STEP {
		ONE, 
		TWO, 
	}
	private STEP step;
    
    public DistinctAggregateBuffer(int maxSize,
                           SpliceGenericAggregator[] aggregators,
                           StandardSupplier<ExecRow> emptyRowSupplier,
                           WarningCollector warningCollector, STEP step){
    	super(maxSize,aggregators);
        this.emptyRowSupplier = emptyRowSupplier;
        this.warningCollector = warningCollector;
        this.step = step;
        switch(step) {
		case ONE:
			this.values = new Step1DistinctScalarBufferedAggregator[bufferSize];
			break;
		case TWO:
			this.values = new Step2DistinctScalarBufferedAggregator[bufferSize];
			break;
		default:
			throw new RuntimeException("createBufferedAggregator called with missing step => " + step);
		}
    }
        
	@Override
	public BufferedAggregator createBufferedAggregator() {
		switch(step) {
		case ONE:
			return new Step1DistinctScalarBufferedAggregator(aggregates,emptyRowSupplier,warningCollector);
		case TWO:
			return new Step2DistinctScalarBufferedAggregator(aggregates,emptyRowSupplier,warningCollector);
		default:
			throw new RuntimeException("createBufferedAggregator called with missing step => " + step);
		}
	}

	@Override
	public void intializeAggregator() {
		
	}
}
