package com.splicemachine.derby.impl.sql.execute.operations.rowcount;

import com.google.common.collect.Lists;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.constants.bytes.BytesUtil;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.impl.storage.AbstractScanProvider;
import com.splicemachine.derby.impl.store.access.SpliceAccessManager;
import com.splicemachine.derby.metrics.OperationRuntimeStats;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.derby.utils.marshall.PairDecoder;
import com.splicemachine.hbase.table.SpliceHTableUtil;
import com.splicemachine.metrics.IOStats;
import com.splicemachine.metrics.Metrics;
import com.splicemachine.pipeline.exception.Exceptions;
import com.splicemachine.db.iapi.error.StandardException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;

/**
 * This is the reduce row provider for RowCountOperation in the case where the source operation's reduce scan is serial
 * (not executed on multiple regions concurrently).
 */
class OffsetScanRowProvider extends AbstractScanProvider {

    private final Scan fullScan;
    private final long totalOffset;
    private final byte[] tableName;
    private final SpliceOperation topOperation;
    private final RowCountOperation rowCountOperation;

    private Queue<Scan> offsetScans;
    private ResultScanner currentScan;
    private long rowsSkipped;
    private HTableInterface table;

    OffsetScanRowProvider(RowCountOperation rowCountOperation, SpliceOperation topOperation,
                          PairDecoder rowDecoder, Scan fullScan, long totalOffset, byte[] tableName,
                          SpliceRuntimeContext spliceRuntimeContext) {
        super(rowDecoder, "offsetScan", spliceRuntimeContext);
        this.rowCountOperation = rowCountOperation;
        this.fullScan = fullScan;
        this.totalOffset = totalOffset;
        this.tableName = tableName;
        this.topOperation = topOperation;
    }

    @Override
    public Result getResult() throws StandardException {
        if (currentScan == null) {
            Scan next = offsetScans.poll();

            if (next == null) {
                return null; // we've finished
            }

            // attach the rows to skip
            if (topOperation instanceof RowCountOperation) {
                ((RowCountOperation) topOperation).setRowsSkipped(rowsSkipped);
            } else {
                for (SpliceOperation op : topOperation.getSubOperations()) {
                    if (op instanceof RowCountOperation) {
                        ((RowCountOperation) op).setRowsSkipped(rowsSkipped);
                        break;
                    }
                }
            }
            SpliceUtils.setInstructions(next, topOperation.getActivation(), topOperation,
                    new SpliceRuntimeContext(rowCountOperation.getOperationInformation().getTransaction()));
            // set the offset that this scan needs to roll from
            try {
                currentScan = table.getScanner(next);
            } catch (IOException e) {
                throw Exceptions.parseException(e);
            }
            return getResult();
        }
        try {
            Result next = currentScan.next();
            // should never happen, but it's good to be safe
            if (next == null) {
                return null;
            }

            byte[] value = next.getValue(SpliceConstants.DEFAULT_FAMILY_BYTES, RowCountOperation.OFFSET_RESULTS_COL);
            if (value == null) {
                return next;
            } else {
                // we've exhausted a region without exhausting the offset, so we need
                // to parse out how many we've skipped and adjust our offset accordingly
                rowsSkipped = Bytes.toLong(value);
                currentScan.close();
                currentScan = null;
                return getResult();
            }
        } catch (IOException e) {
            throw Exceptions.parseException(e);
        }
    }

    @Override
    public Scan toScan() {
        return fullScan;
    }

    @Override
    public void open() {
        table = SpliceAccessManager.getHTable(getTableName());
        try {
            splitScansAroundRegionBarriers();
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public byte[] getTableName() {
        return tableName;
    }

    @Override
    public void close() throws StandardException {
        try {
            table.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            super.close();
        }
    }

    private void splitScansAroundRegionBarriers() throws ExecutionException, IOException {
        // get the region set for this table
        List<HRegionInfo> regionInfos;
        final HTable hTable = SpliceHTableUtil.toHTable(table);
        if (hTable != null) {
            regionInfos = Lists.newArrayList(hTable.getRegionLocations().keySet());
        } else {
            throw new ExecutionException(
                    new UnsupportedOperationException(
                            "Unknown Table type, unable to get Region information. Table type is "
                                    + table.getClass()));
        }

        List<Pair<byte[], byte[]>> ranges = Lists.newArrayListWithCapacity(regionInfos.size());
        byte[] scanStart = fullScan.getStartRow();
        byte[] scanStop = fullScan.getStopRow();

        if (Bytes.compareTo(scanStart, HConstants.EMPTY_START_ROW) == 0
                && Bytes.compareTo(scanStop, HConstants.EMPTY_END_ROW) == 0) {
            // we cover everything
            for (HRegionInfo regionInfo : regionInfos) {
                ranges.add(Pair.newPair(regionInfo.getStartKey(), regionInfo.getEndKey()));
            }
        } else {
            for (HRegionInfo regionInfo : regionInfos) {
                Pair<byte[], byte[]> intersect =
                        BytesUtil.intersect(scanStart, scanStop, regionInfo.getStartKey(),
                                regionInfo.getEndKey());
                if (intersect != null) ranges.add(intersect);
            }
        }

        // make sure we're sorted low to high
        Collections.sort(ranges, new Comparator<Pair<byte[], byte[]>>() {
            @Override
            public int compare(Pair<byte[], byte[]> o1, Pair<byte[], byte[]> o2) {
                byte[] left = o1.getFirst();
                byte[] right = o2.getFirst();
                if (Bytes.compareTo(left, HConstants.EMPTY_START_ROW) == 0) {
                    if (Bytes.compareTo(right, HConstants.EMPTY_START_ROW) == 0) return 0;
                    else return -1;
                } else if (Bytes.compareTo(right, HConstants.EMPTY_START_ROW) == 0) return 1;
                else return Bytes.compareTo(left, right);
            }
        });

        offsetScans = new LinkedList<Scan>();
        for (Pair<byte[], byte[]> region : ranges) {
            Scan scan = new Scan();
            scan.setStartRow(region.getFirst());
            scan.setStopRow(region.getSecond());
            scan.setFilter(fullScan.getFilter());
            if (totalOffset < fullScan.getCaching()) {
                scan.setCaching((int) totalOffset);
            }
            offsetScans.add(scan);
        }
    }

    @Override
    public SpliceRuntimeContext getSpliceRuntimeContext() {
        return super.getSpliceRuntimeContext();
    }

    @Override
    public void reportStats(long statementId, long operationId, long taskId, String xplainSchema,
                            String regionName) throws IOException {
        OperationRuntimeStats metrics = rowCountOperation.getMetrics(statementId, operationId, true);
        if(metrics != null) {
            metrics.setHostName(SpliceUtils.getHostName());
            SpliceDriver.driver().getTaskReporter()
                    .report(metrics, rowCountOperation.getOperationInformation().getTransaction());
        }
    }

    @Override
    public IOStats getIOStats() {
        return Metrics.noOpIOStats();
    }
}
