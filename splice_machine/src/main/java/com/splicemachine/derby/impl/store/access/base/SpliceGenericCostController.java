package com.splicemachine.derby.impl.store.access.base;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.store.access.StoreCostController;
import com.splicemachine.db.iapi.types.RowLocation;

public abstract class SpliceGenericCostController implements StoreCostController {

    /**
     * Scratch Estimate...
     */
    @Override
    public long getEstimatedRowCount() throws StandardException {
        return 0;
    }

    @Override
    public RowLocation newRowLocationTemplate() throws StandardException {
        return null;
    }
}
