package com.splicemachine.derby.stream.iapi;

import com.splicemachine.db.iapi.services.io.FormatableBitSet;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.storage.DataScan;
import com.splicemachine.storage.DataScanner;

/**
 * @author Scott Fines
 *         Date: 1/8/16
 */
public interface IndexScanSetBuilder<V> extends ScanSetBuilder<V>{

    IndexScanSetBuilder<V> indexColToMainColPosMap(int[] colPosMap);

    /*
     * Overriding here so that we have the same type returned (IndexScanSetBuilder instead
     * of ScanSetBuilder)
     */
    @Override
    IndexScanSetBuilder<V> demarcationPoint(long demarcationPoint);

    @Override
    IndexScanSetBuilder<V> baseTableConglomId(long baseTableConglomId);

    @Override
    IndexScanSetBuilder<V> columnPositionMap(int[] columnPositionMap);

    @Override
    IndexScanSetBuilder<V> fieldLengths(int[] fieldLengths);

    @Override
    IndexScanSetBuilder<V> tableVersion(String tableVersion);

    @Override
    IndexScanSetBuilder<V> indexName(String indexName);

    @Override
    IndexScanSetBuilder<V> accessedKeyColumns(FormatableBitSet accessedKeyColumns);

    @Override
    IndexScanSetBuilder<V> keyDecodingMap(int[] keyDecodingMap);

    @Override
    IndexScanSetBuilder<V> keyColumnTypes(int[] keyColumnTypes);

    @Override
    IndexScanSetBuilder<V> keyColumnSortOrder(boolean[] keyColumnSortOrder);

    @Override
    IndexScanSetBuilder<V> keyColumnEncodingOrder(int[] keyColumnEncodingOrder);

    @Override
    IndexScanSetBuilder<V> reuseRowLocation(boolean reuseRowLocation);

    @Override
    IndexScanSetBuilder<V> rowDecodingMap(int[] rowDecodingMap);

    @Override
    IndexScanSetBuilder<V> transaction(TxnView txn);

    @Override
    IndexScanSetBuilder<V> execRowTypeFormatIds(int[] execRowTypeFormatIds);

    @Override
    IndexScanSetBuilder<V> scan(DataScan scan);

    @Override
    IndexScanSetBuilder<V> operationContext(OperationContext operationContext);

    @Override
    IndexScanSetBuilder<V> template(ExecRow template);

    @Override
    IndexScanSetBuilder<V> scanner(DataScanner scanner);

    @Override
    IndexScanSetBuilder<V> activation(Activation activation);
}
