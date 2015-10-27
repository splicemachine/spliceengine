package com.splicemachine.derby.impl.sql;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;

import java.io.IOException;
import java.util.Iterator;

/**
 * @author Scott Fines
 *         Date: 10/27/15
 */
public interface JoinTable extends AutoCloseable{

    interface Factory{
        JoinTable newTable();
    }

    Iterator<ExecRow> fetchInner(ExecRow outer) throws IOException, StandardException;

    @Override
    void close();
}
