package com.splicemachine.derby.stream.iapi;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;

import java.util.Iterator;

/**
 * Created by dgomezferro on 5/20/16.
 */
public interface RemoteQueryClient {
    void submit() throws StandardException;

    Iterator<LocatedRow> getIterator();
}
