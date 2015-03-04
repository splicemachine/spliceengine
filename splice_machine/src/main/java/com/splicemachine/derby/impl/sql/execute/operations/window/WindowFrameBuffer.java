package com.splicemachine.derby.impl.sql.execute.operations.window;

import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;

import java.io.IOException;

/**
 * Created by jyuan on 9/15/14.
 */
public interface WindowFrameBuffer {
    ExecRow next(SpliceRuntimeContext runtimeContext) throws StandardException, IOException;

    void move() throws StandardException, IOException;
}
