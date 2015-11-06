package com.splicemachine.derby.stream.input;

import com.carrotsearch.hppc.BitSet;
import com.clearspring.analytics.util.Lists;
import com.splicemachine.SpliceKryoRegistry;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.FormatableBitSet;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.db.iapi.types.*;
import com.splicemachine.db.impl.jdbc.EmbedConnection;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.impl.sql.execute.operations.scanner.SIFilterFactory;
import com.splicemachine.derby.impl.sql.execute.operations.scanner.SITableScanner;
import com.splicemachine.derby.impl.stats.SimpleOverheadManagedPartitionStatistics;
import com.splicemachine.derby.impl.store.access.SpliceTransactionManager;
import com.splicemachine.derby.impl.store.access.base.SpliceConglomerate;
import com.splicemachine.derby.impl.store.access.hbase.HBaseRowLocation;
import com.splicemachine.derby.stream.stats.IndexStatisticsCollector;
import com.splicemachine.derby.stream.stats.StatisticsCollector;
import com.splicemachine.derby.utils.marshall.dvd.TimestampV2DescriptorSerializer;
import com.splicemachine.encoding.MultiFieldEncoder;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.hbase.MeasuredRegionScanner;
import com.splicemachine.mrio.api.core.SMSQLUtil;
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
import java.util.List;
import java.util.concurrent.ExecutionException;


public class StatisticsScanner<Data> extends SITableScanner<Data> {
    private boolean initialized;
    private List<ExecRow> rows;
    private int[] columnPositionMap;
    private String conglomId;
    private String regionId;
    private transient long openScannerTimeMicros = -1l;
    private transient long closeScannerTimeMicros = -1l;
    private long tableStatsConglomId;
    private TxnView txn;
    private StatisticsCollector collector;
    SimpleOverheadManagedPartitionStatistics statistics;

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
        this.columnPositionMap = columnPositionMap;
        this.txn = txn;
        MeasuredRegionScanner regionScanner = getRegionScanner();
        HRegionInfo r = regionScanner.getRegionInfo();
        conglomId = r.getTable().toString();
        regionId = r.getEncodedName();
        if (baseConglomId <=0) {
            this.collector = new StatisticsCollector(txn, template, columnPositionMap, fieldLengths, this);
        }
        else
            this.collector = new IndexStatisticsCollector(txn, template, columnPositionMap, fieldLengths, this, baseConglomId);
    }

    @Override
    public ExecRow next() throws StandardException, IOException {
        ExecRow next = null;
        if (!initialized) {
            initialize();
        }
        if(rows.size() == 0)
            return null;
        HBaseRowLocation currentLocation = new HBaseRowLocation(SpliceDriver.driver().getUUIDGenerator().nextUUIDBytes());
        currentRowLocation = currentLocation;
        return rows.remove(0);
    }

    private ExecRow getColumnStatsExecRow(String name) throws StandardException, IOException, ExecutionException{
        EmbedConnection conn = (EmbedConnection)SpliceDriver.driver().getInternalConnection();
        TransactionController transactionExecute = conn.getLanguageConnection().getTransactionExecute();
        long conglomId = getConglomerateId(name);
        SpliceConglomerate conglomerate = (SpliceConglomerate) ((SpliceTransactionManager) transactionExecute)
                .findConglomerate(conglomId);
        int[] formatIds = conglomerate.getFormat_ids();
        return SMSQLUtil.getExecRow(formatIds);

    }
    private void initialize() throws StandardException, IOException {
        try {
            ExecRow next = null;
            tableStatsConglomId = getConglomerateId("SYSTABLESTATS");
            while ((next = super.next()) != null) {
                collector.collect(next);
            }
            statistics = collector.getStatistics();
            ExecRow row = getColumnStatsExecRow("SYSCOLUMNSTATS");
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
            throw StandardException.newException(e.getLocalizedMessage());
        }
    }

    @Override
    public void close() throws StandardException, IOException {
        collectTableStatistics();
        super.close();
    }

    protected void collectTableStatistics() throws StandardException{

        try {
            CallBuffer<KVPair> buffer = SpliceDriver.driver().getTableWriter().writeBuffer(Long.toString(tableStatsConglomId).getBytes(), txn);
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

    private long getConglomerateId(String name) throws ExecutionException {
        try {
            Connection conn = SpliceDriver.driver().getInternalConnection();
            String sql = "select conglomeratenumber from sys.sysschemas s, sys.systables t, sys.sysconglomerates c "
                    + "where s.schemaid = t.schemaid and t.tableid=c.tableid and s.schemaname='SYS' and t.tablename=?";
            PreparedStatement s = conn.prepareStatement(sql);
            s.setString(1, name);
            ResultSet rs = s.executeQuery();
            assert  (rs.next());
            long    id = rs.getLong(1);
            return id;
        } catch (Exception e) {
            throw new ExecutionException(e);
        }
    }
}
