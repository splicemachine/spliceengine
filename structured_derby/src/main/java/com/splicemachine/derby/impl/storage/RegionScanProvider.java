package com.splicemachine.derby.impl.storage;

import com.splicemachine.utils.SpliceLogUtils;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.RegionScanner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * RowProvider which uses a RegionScanner to pull in data locally.
 *
 * @author Scott Fines
 *         Date Created: 1/17/13:1:18 PM
 */
public class RegionScanProvider extends AbstractScanProvider {
    private final RegionScanner scanner;
    private final List<KeyValue> keyValues;
    private boolean done = false;
    private byte[] start;
    private byte[] finish;

    public RegionScanProvider(RegionScanner scanner,ExecRow row, FormatableBitSet fbt) {
        super(row,fbt);
        this.scanner = scanner;
        this.keyValues = new ArrayList<KeyValue>();
    }

    @Override
    protected Result getResult() throws IOException {
        if(done) return null;
        done = scanner.next(keyValues);
        Result r = new Result(keyValues);
        if(start==null)start = r.getRow();
        if(done) finish = r.getRow();
        return r;
    }

    @Override public void open() { }

    @Override
    public void close() {
        try {
            scanner.close();
        } catch (IOException e) {
            SpliceLogUtils.logAndThrowRuntime(LOG,e);
        }
    }

    @Override
    public Scan toScan() {
        byte[] scanStart = start==null?scanner.getRegionInfo().getStartKey(): start;
        byte[] scanStop = finish==null?scanner.getRegionInfo().getEndKey(): finish;

        Scan scan = new Scan(scanStart,scanStop);
        return scan;
    }

    @Override
    public byte[] getTableName() {
        return scanner.getRegionInfo().getTableName();
    }
}
