package com.splicemachine.derby.stream.iapi;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.TableScanOperation;
import com.splicemachine.derby.impl.sql.execute.operations.scanner.SITableScanner;
import com.splicemachine.derby.impl.sql.execute.operations.scanner.TableScannerBuilder;
import com.splicemachine.derby.stream.iterator.TableScannerIterator;

import java.io.InputStream;

/**
 * Created by jleach on 4/13/15.
 */
public interface DataSetProcessor {
    <Op extends SpliceOperation, V> DataSet<V> getTableScanner(Op spliceOperation,TableScannerBuilder siTableBuilder, String tableName) throws StandardException;
    <V> DataSet<V> getEmpty();
    <V> DataSet<V> singleRowDataSet(V value);
    <V> DataSet<V> createDataSet(Iterable<V> value);

    <K,V> PairDataSet<K, V> singleRowPairDataSet(K key, V value);

    <Op extends SpliceOperation> OperationContext<Op> createOperationContext(Op spliceOperation);
    void setJobGroup(String jobName, String jobDescription);
    PairDataSet<String,InputStream> readTextFile(String path);
    <K,V> PairDataSet<K, V> getEmptyPair();

    TableScannerIterator getTableScannerIterator(TableScanOperation operation) throws StandardException;
}