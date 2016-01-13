package com.splicemachine.derby.impl.sql.execute.operations.scanner;

import com.splicemachine.db.iapi.services.io.FormatableBitSet;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.stream.iapi.IndexScanSetBuilder;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.si.api.server.TransactionalRegion;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.storage.DataScan;
import com.splicemachine.storage.DataScanner;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * @author Scott Fines
 *         Date: 1/13/16
 */
public abstract class IndexTableScannerBuilder<V> extends TableScannerBuilder<V> implements IndexScanSetBuilder<V>{
    protected int[] indexColToMainColPosMap;

    @Override
    public IndexScanSetBuilder<V> indexColToMainColPosMap(int[] colPosMap){
        this.indexColToMainColPosMap = colPosMap;
        return this;
    }

    @Override
    public IndexScanSetBuilder<V> activation(Activation activation){
        super.activation(activation);
        return this;
    }

    @Override
    public IndexScanSetBuilder<V> scanner(DataScanner scanner){
        super.scanner(scanner);
        return this;
    }

    @Override
    public IndexScanSetBuilder<V> template(ExecRow template){
        super.template(template);
        return this;
    }

    @Override
    public IndexScanSetBuilder<V> operationContext(OperationContext operationContext){
        super.operationContext(operationContext);
        return this;
    }

    @Override
    public IndexScanSetBuilder<V> scan(DataScan scan){
        super.scan(scan);
        return this;
    }

    @Override
    public IndexScanSetBuilder<V> execRowTypeFormatIds(int[] execRowTypeFormatIds){
        super.execRowTypeFormatIds(execRowTypeFormatIds);
        return this;
    }

    @Override
    public IndexScanSetBuilder<V> transaction(TxnView txn){
        super.transaction(txn);
        return this;
    }

    @Override
    public IndexScanSetBuilder<V> rowDecodingMap(int[] rowDecodingMap){
        super.rowDecodingMap(rowDecodingMap);
        return this;
    }

    @Override
    public IndexScanSetBuilder<V> reuseRowLocation(boolean reuseRowLocation){
        super.reuseRowLocation(reuseRowLocation);
        return this;
    }

    @Override
    public IndexScanSetBuilder<V> keyColumnEncodingOrder(int[] keyColumnEncodingOrder){
        super.keyColumnEncodingOrder(keyColumnEncodingOrder);
        return this;
    }

    @Override
    public IndexScanSetBuilder<V> keyColumnSortOrder(boolean[] keyColumnSortOrder){
        super.keyColumnSortOrder(keyColumnSortOrder);
        return this;
    }

    @Override
    public IndexScanSetBuilder<V> keyColumnTypes(int[] keyColumnTypes){
        super.keyColumnTypes(keyColumnTypes);
        return this;
    }

    @Override
    public IndexScanSetBuilder<V> keyDecodingMap(int[] keyDecodingMap){
        super.keyDecodingMap(keyDecodingMap);
        return this;
    }

    @Override
    public IndexScanSetBuilder<V> accessedKeyColumns(FormatableBitSet accessedKeyColumns){
        super.accessedKeyColumns(accessedKeyColumns);
        return this;
    }

    @Override
    public IndexScanSetBuilder<V> indexName(String indexName){
        super.indexName(indexName);
        return this;
    }

    @Override
    public IndexScanSetBuilder<V> tableVersion(String tableVersion){
        super.tableVersion(tableVersion);
        return this;
    }

    @Override
    public IndexScanSetBuilder<V> filterFactory(SIFilterFactory filterFactory){
        super.filterFactory(filterFactory);
        return this;
    }

    @Override
    public IndexScanSetBuilder<V> region(TransactionalRegion region){
        super.region(region);
        return this;
    }

    @Override
    public IndexScanSetBuilder<V> fieldLengths(int[] fieldLengths){
        super.fieldLengths(fieldLengths);
        return this;
    }

    @Override
    public IndexScanSetBuilder<V> columnPositionMap(int[] columnPositionMap){
        super.columnPositionMap(columnPositionMap);
        return this;
    }

    @Override
    public IndexScanSetBuilder<V> baseTableConglomId(long baseTableConglomId){
        super.baseTableConglomId(baseTableConglomId);
        return this;
    }

    @Override
    public IndexScanSetBuilder<V> demarcationPoint(long demarcationPoint){
        super.demarcationPoint(demarcationPoint);
        return this;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException{
        super.writeExternal(out);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException{
        super.readExternal(in);
    }

}
