package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.derby.utils.JoinSideExecRow;
import org.apache.derby.iapi.error.StandardException;

import java.io.IOException;

/**
 * @author Scott Fines
 * Created on: 10/29/13
 */
interface MergeScanner {

    void open() throws StandardException,IOException;

    JoinSideExecRow nextRow() throws StandardException,IOException;

    void close() throws StandardException,IOException;
}
