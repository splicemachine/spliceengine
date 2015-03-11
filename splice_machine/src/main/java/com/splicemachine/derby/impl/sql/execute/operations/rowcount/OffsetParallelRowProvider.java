package com.splicemachine.derby.impl.sql.execute.operations.rowcount;

import com.splicemachine.derby.iapi.storage.RowProvider;
import com.splicemachine.derby.impl.storage.RowProviders;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;

import java.io.IOException;

/**
 * A RowProvider implementation intended for use in the reduce phase of a offset RowCountOperation where the
 * source operation's reduce scan executes on multiple regions concurrently.
 */
class OffsetParallelRowProvider extends RowProviders.DelegatingRowProvider {

    private final long offset;
    private long rowCount;

    OffsetParallelRowProvider(RowProvider provider, long offset) {
        super(provider);
        this.offset = offset;
    }

    @Override
    public ExecRow next() throws StandardException, IOException {
        ExecRow next = null;
        while (provider.hasNext()) {
            next = provider.next();
            rowCount++;
            if (next == null || rowCount > offset) {
                break;
            }
        }
        return rowCount <= offset ? null : next;
    }
}