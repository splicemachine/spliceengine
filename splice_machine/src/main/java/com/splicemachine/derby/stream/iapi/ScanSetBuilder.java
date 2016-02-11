package com.splicemachine.derby.stream.iapi;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.FormatableBitSet;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.impl.sql.execute.operations.scanner.TableScannerBuilder;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.metrics.MetricFactory;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.storage.DataScan;
import com.splicemachine.storage.DataScanner;

import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 1/7/16
 */
public interface ScanSetBuilder<V>{
    ScanSetBuilder<V> metricFactory(MetricFactory metricFactory);

    ScanSetBuilder<V> scanner(DataScanner scanner);

    ScanSetBuilder<V> template(ExecRow template);

    ScanSetBuilder<V> operationContext(OperationContext operationContext);

    ScanSetBuilder<V> scan(DataScan scan);

    ScanSetBuilder<V> execRowTypeFormatIds(int[] execRowTypeFormatIds);

    ScanSetBuilder<V> transaction(TxnView txn);

    ScanSetBuilder<V> rowDecodingMap(int[] rowDecodingMap);

    ScanSetBuilder<V> reuseRowLocation(boolean reuseRowLocation);

    ScanSetBuilder<V> keyColumnEncodingOrder(int[] keyColumnEncodingOrder);

    ScanSetBuilder<V> keyColumnSortOrder(boolean[] keyColumnSortOrder);

    ScanSetBuilder<V> keyColumnTypes(int[] keyColumnTypes);

    ScanSetBuilder<V> keyDecodingMap(int[] keyDecodingMap);

    ScanSetBuilder<V> accessedKeyColumns(FormatableBitSet accessedKeyColumns);

    ScanSetBuilder<V> indexName(String indexName);

    ScanSetBuilder<V> tableVersion(String tableVersion);

    ScanSetBuilder<V> fieldLengths(int[] fieldLengths);

    ScanSetBuilder<V> columnPositionMap(int[] columnPositionMap);

    ScanSetBuilder<V> baseTableConglomId(long baseTableConglomId);

    ScanSetBuilder<V> demarcationPoint(long demarcationPoint);

    DataSet<V> buildDataSet() throws StandardException;

    // TODO (wjkmerge): might be able to do better than Object but for now this is port from master_dataset
    DataSet<V> buildDataSet(Object caller) throws StandardException;

    ScanSetBuilder<V> activation(Activation activation);

    String base64Encode() throws IOException, StandardException;

    DataScan getScan();

    TxnView getTxn();
}
