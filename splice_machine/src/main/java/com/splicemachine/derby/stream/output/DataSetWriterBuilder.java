package com.splicemachine.derby.stream.output;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.stream.iapi.TableWriter;
import com.splicemachine.si.api.txn.TxnView;

/**
 * @author Scott Fines
 *         Date: 1/8/16
 */
public interface DataSetWriterBuilder{

    DataSetWriter build() throws StandardException;

    DataSetWriterBuilder destConglomerate(long heapConglom);

    DataSetWriterBuilder txn(TxnView txn);

    DataSetWriterBuilder operationContext(OperationContext operationContext);

    DataSetWriterBuilder skipIndex(boolean skipIndex);

    TxnView getTxn();

    byte[] getDestinationTable();

    TableWriter buildTableWriter() throws StandardException;
}
