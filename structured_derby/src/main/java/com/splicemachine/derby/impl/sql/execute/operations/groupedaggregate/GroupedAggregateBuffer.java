package com.splicemachine.derby.impl.sql.execute.operations.groupedaggregate;

import com.splicemachine.derby.impl.sql.execute.operations.WarningCollector;
import com.splicemachine.derby.impl.sql.execute.operations.framework.AbstractAggregateBuffer;
import com.splicemachine.derby.impl.sql.execute.operations.framework.BufferedAggregator;
import com.splicemachine.derby.impl.sql.execute.operations.framework.SpliceGenericAggregator;
import com.splicemachine.derby.utils.StandardSupplier;
import org.apache.derby.iapi.sql.execute.ExecRow;

/**
 * @author Scott Fines
 * Created on: 11/1/13
 */
public class GroupedAggregateBuffer extends AbstractAggregateBuffer {
    private final boolean eliminateDuplicates;
    private final boolean shouldMerge;
    private final StandardSupplier<ExecRow> emptyRowSupplier;
    private final WarningCollector warningCollector;

    public GroupedAggregateBuffer(int maxSize,
                           SpliceGenericAggregator[] aggregators,
                           boolean eliminateDuplicates,
                           StandardSupplier<ExecRow> emptyRowSupplier,
                           WarningCollector warningCollector){
        this(maxSize, aggregators, eliminateDuplicates, emptyRowSupplier, warningCollector,false);
    }
		public GroupedAggregateBuffer(int maxSize,
								SpliceGenericAggregator[] aggregators,
								boolean eliminateDuplicates,
							    StandardSupplier<ExecRow> emptyRowSupplier,
								WarningCollector warningCollector,
								boolean shouldMerge) {
		super(maxSize,aggregators);
        this.emptyRowSupplier = emptyRowSupplier;
        this.warningCollector = warningCollector;
        this.shouldMerge = shouldMerge;
        this.eliminateDuplicates = eliminateDuplicates;
		}
        
	@Override
	public BufferedAggregator createBufferedAggregator() {
		return new GroupedAggregateBufferedAggregator(aggregates, eliminateDuplicates, shouldMerge,
                emptyRowSupplier, warningCollector);		
	}
	@Override
	public void intializeAggregator() {
        this.values = new GroupedAggregateBufferedAggregator[bufferSize];
	}

}
