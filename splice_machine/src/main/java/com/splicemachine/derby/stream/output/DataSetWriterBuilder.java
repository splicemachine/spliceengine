package com.splicemachine.derby.stream.output;

import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.stream.output.delete.DeleteTableWriterBuilder;
import com.splicemachine.si.api.txn.TxnView;

/**
 * @author Scott Fines
 *         Date: 1/8/16
 */
public interface DataSetWriterBuilder{

    DataSetWriter build();

    DataSetWriterBuilder destConglomerate(long heapConglom);

    DataSetWriterBuilder txn(TxnView txn);

    DataSetWriterBuilder operationContext(OperationContext operationContext);

    DataSetWriterBuilder skipIndex(boolean skipIndex);
}
