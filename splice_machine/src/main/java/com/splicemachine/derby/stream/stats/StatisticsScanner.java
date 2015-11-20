package com.splicemachine.derby.stream.stats;

import com.clearspring.analytics.util.Lists;
import com.splicemachine.SpliceKryoRegistry;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.FormatableBitSet;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.*;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.impl.sql.catalog.SYSCOLUMNSTATISTICSRowFactory;
import com.splicemachine.derby.impl.sql.execute.operations.scanner.SIFilterFactory;
import com.splicemachine.derby.impl.sql.execute.operations.scanner.SITableScanner;
import com.splicemachine.derby.impl.stats.SimpleOverheadManagedPartitionStatistics;
import com.splicemachine.derby.impl.store.access.hbase.HBaseRowLocation;
import com.splicemachine.derby.utils.marshall.dvd.TimestampV2DescriptorSerializer;
import com.splicemachine.encoding.MultiFieldEncoder;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.hbase.MeasuredRegionScanner;
import com.splicemachine.pipeline.api.CallBuffer;
import com.splicemachine.si.api.TransactionalRegion;
import com.splicemachine.si.api.TxnView;
import com.splicemachine.si.data.api.SDataLib;
import com.splicemachine.stats.ColumnStatistics;
import com.splicemachine.storage.EntryEncoder;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.client.Scan;
import java.io.IOException;
import java.sql.*;
import com.carrotsearch.hppc.BitSet;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class StatisticsScanner<Data> extends SITableScanner<Data> {
    private boolean initialized;
    private List<ExecRow> rows;
    private int[] columnPositionMap;
    private String conglomId;
    private String regionId;
    private TxnView txn;
    private StatisticsCollector collector;
    SimpleOverheadManagedPartitionStatistics statistics;
    private long baseConglomId;

    public StatisticsScanner(final SDataLib dataLib, MeasuredRegionScanner<Data> scanner,
                             final TransactionalRegion region,
                             final ExecRow template,
                             Scan scan,
                             final int[] rowDecodingMap,
                             final TxnView txn,
                             int[] keyColumnEncodingOrder,
                             boolean[] keyColumnSortOrder,
                             int[] keyColumnTypes,
                             int[] keyDecodingMap,
                             FormatableBitSet accessedPks,
                             boolean reuseRowLocation,
                             String indexName,
                             final String tableVersion,
                             SIFilterFactory filterFactory,
                             int[] fieldLengths,
                             int[] columnPositionMap,
                             long baseConglomId) {
        super(dataLib, scanner, region, template, scan, rowDecodingMap, txn, keyColumnEncodingOrder,
                keyColumnSortOrder, keyColumnTypes, keyDecodingMap, accessedPks, reuseRowLocation, indexName,
                tableVersion, filterFactory);
        assert baseConglomId != -1:"Base Conglom ID passed in incorrectly.";
        this.columnPositionMap = columnPositionMap;
        this.txn = txn;
        MeasuredRegionScanner regionScanner = getRegionScanner();
        HRegionInfo r = regionScanner.getRegionInfo();
        conglomId = r.getTable().getQualifierAsString();
        regionId = r.getEncodedName();
        this.baseConglomId = baseConglomId;
        collector = new StatisticsCollector(txn, template, columnPositionMap, fieldLengths, this);
    }

    @Override
    public ExecRow next() throws StandardException, IOException {
        if (!initialized) {
            initialize();
        }
        if(rows.size() == 0)
            return null;
        HBaseRowLocation currentLocation = new HBaseRowLocation(SpliceDriver.driver().getUUIDGenerator().nextUUIDBytes());
        currentRowLocation = currentLocation;
        return rows.remove(0);
    }

    private void initialize() throws StandardException, IOException {
        try {
            ExecRow next = null;
            while ((next = super.next()) != null) {
                collector.collect(next);
            }
            statistics = collector.getStatistics();
            ExecRow row = SYSCOLUMNSTATISTICSRowFactory.makeGenericRow();
            List<ColumnStatistics> columnStatisticsList = statistics.columnStatistics();
            rows = Lists.newArrayList();
            int i = 0;
            for (ColumnStatistics columnStatistics : columnStatisticsList) {
                if (columnStatistics == null)
                    continue;
                row.resetRowArray();
                DataValueDescriptor[] rowArray = row.getRowArray();
                rowArray[0] = new SQLLongint(new Long(conglomId));
                rowArray[1] = new SQLVarchar(regionId);
                rowArray[2] = new SQLInteger(columnPositionMap[i++]);
                rowArray[3] = new UserType(columnStatistics);
                rows.add(row.getClone());
            }
            initialized = true;
        }
        catch (ExecutionException e) {
            throw StandardException.plainWrapException(e);
        }
    }

    @Override
    public void close() throws StandardException, IOException {
        collectTableStatistics();
        super.close();
    }

    protected void collectTableStatistics() throws StandardException{

        try {
            CallBuffer<KVPair> buffer = SpliceDriver.driver().getTableWriter().writeBuffer(Long.toString(baseConglomId).getBytes(), txn);
            byte[] rowKey = getRowKey();
            byte[] row = getRow(this);
            KVPair kvPair = new KVPair(rowKey, row, KVPair.Type.UPSERT);
            buffer.add(kvPair);
            buffer.flushBuffer();
        } catch (Exception e) {
            throw StandardException.newException(e.getMessage());
        }
    }

    private byte[] getRowKey() {

        MultiFieldEncoder keyEncoder = MultiFieldEncoder.create(2);
        keyEncoder = keyEncoder.encodeNext(new Long(conglomId))
                .encodeNext(regionId);
        byte[] rowKey = keyEncoder.build();
        return rowKey;
    }

    private byte[] getRow(SITableScanner scanner) throws ExecutionException {
        BitSet nonNullRowFields = new BitSet();
        nonNullRowFields.set(2,14);
        BitSet scalarFields = new BitSet();
        scalarFields.set(2);
        scalarFields.set(5,14);

        BitSet doubleFields = new BitSet();
        EntryEncoder rowEncoder = EntryEncoder.create(SpliceKryoRegistry.getInstance(), 11, nonNullRowFields, scalarFields, doubleFields, doubleFields);
        byte[] row = null;
        try{
            MultiFieldEncoder rEncoder = rowEncoder.getEntryEncoder();
            Timestamp timestamp = new Timestamp(System.currentTimeMillis());
            rEncoder.encodeNext(TimestampV2DescriptorSerializer.formatLong(timestamp))
                    .encodeNext(false)
                    .encodeNext(false)
                    .encodeNext(statistics.rowCount())
                    .encodeNext(statistics.totalSize())
                    .encodeNext(statistics.avgRowWidth())
                    .encodeNext(statistics.queryCount())
                    .encodeNext(statistics.localReadTime())
                    .encodeNext(statistics.remoteReadTime())
                    .encodeNext(statistics.remoteReadTime()) //TODO -sf- add in write latency
                    .encodeNext((long)statistics.getOpenScannerLatency())
                    .encodeNext((long)statistics.getCloseScannerLatency());

            row = rowEncoder.encode();

        } catch (Exception e) {
            throw new ExecutionException(e);
        }
        return row;
    }

}
