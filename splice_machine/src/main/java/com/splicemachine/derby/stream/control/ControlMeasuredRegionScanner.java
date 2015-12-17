package com.splicemachine.derby.stream.control;

import com.splicemachine.derby.impl.store.access.SpliceAccessManager;
import com.splicemachine.hbase.MeasuredRegionScanner;
import com.splicemachine.metrics.TimeView;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import java.io.IOException;
import java.util.List;

/**
 * Created by jleach on 4/20/15.
 */
public class ControlMeasuredRegionScanner implements MeasuredRegionScanner<Cell> {
    private static final Logger LOG = Logger.getLogger(ControlMeasuredRegionScanner.class);
    protected byte[] tableName;
    protected Scan scan;
    private Table htable;
    private ResultScanner scanner;
    private boolean opened = false;

    public ControlMeasuredRegionScanner(byte[] tableName, Scan scan) {
        this.tableName = tableName;
        this.scan = scan;
    }

    @Override
    public void start() {
        if(htable==null)
            htable = SpliceAccessManager.getHTable(tableName);
    }

    @Override
    public TimeView getReadTime() {
        return null;
    }

    @Override
    public long getBytesOutput() {
        return 0;
    }

    @Override
    public Cell next() throws IOException {
        return null;
    }

    @Override
    public boolean internalNextRaw(List<Cell> results) throws IOException {

        try {
            if (!opened) {
                start();
                scanner = htable.getScanner(scan);
                opened = true;
            }
            Result result = scanner.next();
            if (result!=null) {
                Cell[] rawCells = result.rawCells();
//                results.addAll(result.listCells()); Removed this call which creates extra objects
                for (int i =0; i<rawCells.length;i++) {
                    results.add(rawCells[i]);

                }
            }
            return result!=null;
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    @Override
    public long getBytesVisited() {
        return 0;
    }

    @Override
    public long getRowsOutput() {
        return 0;
    }

    @Override
    public long getRowsFiltered() {
        return 0;
    }

    @Override
    public long getRowsVisited() {
        return 0;
    }

    @Override
    public HRegionInfo getRegionInfo() {
        return null;
    }

    @Override
    public boolean isFilterDone() throws IOException {
        return false;
    }

    @Override
    public boolean reseek(byte[] bytes) throws IOException {
        return false;
    }

    @Override
    public long getMaxResultSize() {
        return 0;
    }

    @Override
    public long getMvccReadPoint() {
        return 0;
    }

    @Override
    public boolean nextRaw(List<Cell> cells) throws IOException {
        return internalNextRaw(cells);
    }

    public boolean nextRaw(List<Cell> cells, int i) throws IOException {
        return internalNextRaw(cells);
    }

    @Override
    public boolean next(List<Cell> cells) throws IOException {
        return internalNextRaw(cells);
    }

    public boolean next(List<Cell> cells, int i) throws IOException {
        return internalNextRaw(cells);
    }

    @Override
    public void close() throws IOException {
        if(scanner!=null)scanner.close();
        if(htable!=null)
            try {
                htable.close();
            } catch (IOException e) {
                SpliceLogUtils.logAndThrowRuntime(LOG,"unable to close htable for "+ Bytes.toString(tableName),e);
            }
    }
}
