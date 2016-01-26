package com.splicemachine.derby.stream.output;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.si.api.txn.TxnView;

/**
 * @author Scott Fines
 *         Date: 1/8/16
 */
public interface DataSetWriter{

    DataSet<LocatedRow> write() throws StandardException;

    void setTxn(TxnView childTxn);
}
