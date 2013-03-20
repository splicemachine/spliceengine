package com.splicemachine.derby.impl.storage;

import com.google.common.io.Closeables;
import com.splicemachine.derby.iapi.storage.ScanBoundary;
import com.splicemachine.derby.impl.sql.execute.operations.Hasher;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.HRegion;

import java.io.IOException;

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

    public SimpleRegionAwareRowProvider(String transactionId, HRegion region,byte[] table,
                                           byte[] columnFamily,
                                           byte[] start, byte[] finish,
                                           final Hasher hasher,
                                           final ExecRow rowTemplate, FormatableBitSet fbt) {
        super(rowTemplate, fbt);
        this.table = table;
        this.scanner = RegionAwareScanner.create(transactionId, region, new SingleTypeHashAwareScanBoundary(
                           columnFamily,rowTemplate, hasher),table,start,finish);
    }

	public SimpleRegionAwareRowProvider(String transactionId, HRegion region, byte[] table,
																			byte[] start, byte[] finish,
																			final ExecRow rowTemplate, FormatableBitSet fbt,ScanBoundary boundary){
		super(rowTemplate,fbt);
		this.table = table;
		this.scanner = RegionAwareScanner.create(transactionId, region,boundary,table,start,finish);
	}

    @Override
    protected Result getResult() throws IOException {
        return scanner.getNextResult();
    }

    @Override
    public void open() {
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

