package com.splicemachine.derby.impl.sql.execute.operations.rowcount;

import com.splicemachine.derby.iapi.storage.RowProvider;
import com.splicemachine.derby.impl.storage.RowProviders;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.execute.ExecRow;

import java.io.IOException;

/**
 * Row provider that limits the number of rows returned by the delegate.
 */
class LimitedRowProvider extends RowProviders.DelegatingRowProvider {

    private final long fetchLimit;
    private long currentRowCount;

    public LimitedRowProvider(RowProvider provider, long fetchLimit) {
        super(provider);
        this.fetchLimit = fetchLimit;
    }

    @Override
    public boolean hasNext() throws StandardException, IOException {
        return currentRowCount < fetchLimit && provider.hasNext();
    }

    @Override
    public ExecRow next() throws StandardException, IOException {
        currentRowCount++;
        return provider.next();
    }

}