package com.splicemachine.derby.impl.storage;

import com.google.common.io.Closeables;
import com.splicemachine.derby.utils.marshall.RowDecoder;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.HRegion;

/**
 * Simple and obvious Region-Aware RowProvider implementation.
 *
 * This implementation uses look-aheads and forward skips to distinguish
 * @author Scott Fines
 * Created: 1/17/13 9:35 PM
 */
public class SimpleRegionAwareRowProvider extends  AbstractScanProvider{

    private final  RegionAwareScanner scanner;
    private final byte[] table;

    public SimpleRegionAwareRowProvider(String txnId,HRegion region,Scan scan,byte[] tableName,byte[] columnFamily,
                                        RowDecoder decoder){
        super(decoder);
        this.table = tableName;
        this.scanner = RegionAwareScanner.create(txnId,region,scan,tableName,
                new SingleTypeHashAwareScanBoundary2(columnFamily,decoder));
    }

    @Override
    protected Result getResult() throws StandardException {
        return scanner.getNextResult();
    }

    @Override
    public void open() throws StandardException {
        scanner.open();
    }

    @Override
    public void close() {
        Closeables.closeQuietly(scanner);
    }

    @Override
    public Scan toScan() {
        return scanner.toScan();
    }

    @Override
    public byte[] getTableName() {
        return table;
    }
}

