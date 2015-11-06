package com.splicemachine.derby.stream.window;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import java.io.IOException;
import java.util.Iterator;

/**
 * Created by jyuan on 9/15/14.
 */
public interface WindowFrameBuffer extends Iterator<ExecRow> {
    void move() throws StandardException, IOException;
}
