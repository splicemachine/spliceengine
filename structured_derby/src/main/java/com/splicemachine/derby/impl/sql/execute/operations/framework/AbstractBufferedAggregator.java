package com.splicemachine.derby.impl.sql.execute.operations.framework;

import org.apache.derby.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.impl.sql.execute.operations.WarningCollector;
import com.splicemachine.derby.utils.StandardSupplier;
/*
 * Abstract Class for DistinctBufferedAggregator
 * 
 * 
 */
public abstract class AbstractBufferedAggregator implements BufferedAggregator {
    protected final SpliceGenericAggregator[] aggregates;
    protected final StandardSupplier<ExecRow> emptyRowSupplier;
    protected final WarningCollector warningCollector;
    protected ExecRow currentRow;

    protected AbstractBufferedAggregator(SpliceGenericAggregator[] aggregates,
                                 StandardSupplier<ExecRow> emptyRowSupplier,
                                 WarningCollector warningCollector) {
        this.aggregates= aggregates;
        this.emptyRowSupplier = emptyRowSupplier;
        this.warningCollector = warningCollector;
    }
}