package com.splicemachine.derby.stream.iapi;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.scanner.TableScannerBuilder;

/**
 * Created by jleach on 4/13/15.
 */
public interface DataSetProcessor<Op extends SpliceOperation,K,V> {
    DataSet<V> getTableScanner(Op spliceOperation,TableScannerBuilder siTableBuilder, String tableName) throws StandardException;
    DataSet<V> getEmpty();
    DataSet<V> singleRowDataSet(V value);
    DataSet<V> createDataSet(Iterable<V> value);
    OperationContext<Op> createOperationContext(Op spliceOperation);
    void setJobGroup(String jobName, String jobDescription);


}