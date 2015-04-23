package com.splicemachine.derby.stream;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.impl.sql.execute.operations.SpliceBaseOperation;
import com.splicemachine.derby.impl.sql.execute.operations.scanner.TableScannerBuilder;

import java.io.Externalizable;

/**
 * Created by jleach on 4/13/15.
 */
public interface DataSetProcessor<Op extends SpliceOperation,K,V> {
    DataSet<Op,V> getTableScanner(Op spliceOperation,TableScannerBuilder siTableBuilder, String tableName, SpliceRuntimeContext spliceRuntimContext) throws StandardException;
    DataSet<Op,V> getEmpty();
    DataSet<Op,V> singleRowDataSet(V value);
    DataSet<Op,V> createDataSet(Iterable<V> value);
    OperationContext<Op> createOperationContext(Op spliceOperation, SpliceRuntimeContext spliceRuntimeContext);
    void setJobGroup(String jobName, String jobDescription);

}