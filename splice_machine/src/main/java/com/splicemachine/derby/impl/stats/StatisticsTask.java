package com.splicemachine.derby.impl.stats;

import com.carrotsearch.hppc.BitSet;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import com.splicemachine.SpliceKryoRegistry;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.ArrayUtil;
import com.splicemachine.db.iapi.services.io.FormatableBitSet;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;
import com.splicemachine.db.iapi.sql.dictionary.SchemaDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.TableDescriptor;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.impl.jdbc.EmbedConnection;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.impl.job.ZkTask;
import com.splicemachine.derby.impl.job.coprocessor.RegionTask;
import com.splicemachine.derby.impl.job.scheduler.SchedulerPriorities;
import com.splicemachine.derby.jdbc.SpliceTransactionResourceImpl;
import com.splicemachine.derby.stats.TaskStats;
import com.splicemachine.derby.utils.marshall.dvd.TimestampV2DescriptorSerializer;
import com.splicemachine.encoding.Encoding;
import com.splicemachine.encoding.MultiFieldEncoder;
import com.splicemachine.hbase.BufferedRegionScanner;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.hbase.MeasuredRegionScanner;
import com.splicemachine.hbase.ReadAheadRegionScanner;
import com.splicemachine.metrics.MetricFactory;
import com.splicemachine.metrics.Metrics;
import com.splicemachine.pipeline.api.CallBuffer;
import com.splicemachine.si.api.TransactionalRegion;
import com.splicemachine.si.api.Txn;
import com.splicemachine.si.api.TxnLifecycleManager;
import com.splicemachine.si.api.TxnView;
import com.splicemachine.si.data.api.SDataLib;
import com.splicemachine.si.impl.HTransactorFactory;
import com.splicemachine.si.impl.TransactionalRegions;
import com.splicemachine.stats.ColumnStatistics;
import com.splicemachine.stats.PartitionStatistics;
import com.splicemachine.storage.EntryEncoder;
import com.splicemachine.utils.SpliceZooKeeperManager;
import com.splicemachine.utils.kryo.KryoObjectOutput;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.RegionScanner;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * Asynchronous task to collect statistics for a region.
 *
 * @author Scott Fines
 *         Date: 2/26/15
 */
public class StatisticsTask extends ZkTask{
    private ExecRow colsToCollect;
    private Scan partitionScan;
    private int[] rowDecodingMap;
    private int[] columnPositionMap; //inverse map from outputPosition to input in the base row
    private int[] keyDecodingMap;
    private int[] keyColumnEncodingOrder;
    private boolean[] keyColumnSortOrder;
    private int[] keyColumnTypes;
    private FormatableBitSet collectedKeyColumns;
    private String tableVersion;
    private int[] fieldLengths;

    //for use on when scanning index tables--set to -1 when scanning the base table
    private long baseTableConglomerateId;

    /**Serialization Constructor. DO NOT USE*/
    @Deprecated
    public StatisticsTask() { }


    public StatisticsTask(String jobId,
                          ExecRow colsToCollect,
                          int[] columnPositionMap,
                          int[] rowDecodingMap,
                          int[] keyDecodingMap,
                          int[] keyColumnEncodingOrder,
                          boolean[] keyColumnSortOrder,
                          int[] keyColumnTypes,
                          int[] fieldLengths,
                          FormatableBitSet collectedKeyColumns,
                          String tableVersion) {
        this(jobId,
                colsToCollect,
                columnPositionMap,
                rowDecodingMap,
                keyDecodingMap,
                keyColumnEncodingOrder,
                keyColumnSortOrder,
                keyColumnTypes,
                fieldLengths,
                collectedKeyColumns,
                -1l,
                tableVersion);
    }

    public StatisticsTask(String jobId,
                          ExecRow colsToCollect,
                          int[] columnPositionMap,
                          int[] rowDecodingMap,
                          int[] keyDecodingMap,
                          int[] keyColumnEncodingOrder,
                          boolean[] keyColumnSortOrder,
                          int[] keyColumnTypes,
                          int[] fieldLengths,
                          FormatableBitSet collectedKeyColumns,
                          long baseTableConglomerateId,
                          String tableVersion) {
        super(jobId,SchedulerPriorities.INSTANCE.getBasePriority(StatisticsTask.class));
        this.colsToCollect = colsToCollect;
        this.rowDecodingMap = rowDecodingMap;
        this.columnPositionMap = columnPositionMap;
        this.keyDecodingMap = keyDecodingMap;
        this.keyColumnEncodingOrder = keyColumnEncodingOrder;
        this.keyColumnSortOrder = keyColumnSortOrder;
        this.keyColumnTypes = keyColumnTypes;
        this.collectedKeyColumns = collectedKeyColumns;
        this.tableVersion = tableVersion;
        this.fieldLengths = fieldLengths;
        this.baseTableConglomerateId = baseTableConglomerateId;
    }

    @Override
    public void prepareTask(byte[] start, byte[] stop, RegionCoprocessorEnvironment rce, SpliceZooKeeperManager zooKeeper) throws ExecutionException {
        partitionScan = new Scan(start,stop);

        super.prepareTask(start, stop, rce, zooKeeper);
    }

    @Override
    protected void doExecute() throws ExecutionException, InterruptedException {
        Txn txn = getTxn();

        try(TransactionalRegion txnRegion = TransactionalRegions.get(region)) {
            StatisticsCollector collector;
            if(baseTableConglomerateId<0) {
                collector = new StatisticsCollector(txn, colsToCollect, partitionScan,
                        rowDecodingMap,
                        keyColumnEncodingOrder,
                        keyColumnSortOrder,
                        keyColumnTypes,
                        keyDecodingMap,
                        columnPositionMap,
                        fieldLengths,
                        collectedKeyColumns,
                        tableVersion,
                        txnRegion,
                        getScanner());
            }else{
                collector = new IndexStatisticsCollector(txn,
                        colsToCollect,
                        partitionScan,
                        rowDecodingMap,
                        keyColumnEncodingOrder,
                        keyColumnSortOrder,
                        keyColumnTypes,
                        keyDecodingMap,
                        columnPositionMap,
                        fieldLengths,
                        collectedKeyColumns,
                        tableVersion,
                        txnRegion,
                        getScanner(),
                        baseTableConglomerateId);

            }

            long start = System.nanoTime();
            PartitionStatistics collected = collector.collect();

            long[] statsTableIds = getStatsConglomerateIds();
            writeTableStats(txnRegion, statsTableIds[0], collected);
            writeColumnStats(txnRegion, statsTableIds[1], collected.columnStatistics());
            long end = System.nanoTime();

            TaskStats ts = new TaskStats(end-start,collected.rowCount(),3l);
            status.setStats(ts);
        }
    }

    @Override
    protected Txn beginChildTransaction(TxnView parentTxn, TxnLifecycleManager tc) throws IOException {
        //we want to read data using a READ_UNCOMMITTED view, so that we are as optimistic as possible
        return tc.beginChildTransaction(parentTxn, Txn.IsolationLevel.READ_UNCOMMITTED, "statistics".getBytes());
    }

    @Override protected String getTaskType() { return "StatisticsCollection"; }
    @Override public boolean invalidateOnClose() { return true; }

    @Override
    public RegionTask getClone() {
        return new StatisticsTask(getJobId(),colsToCollect.getClone(),
                rowDecodingMap,
                columnPositionMap,
                keyDecodingMap,
                keyColumnEncodingOrder,
                keyColumnSortOrder,
                keyColumnTypes,
                fieldLengths,
                collectedKeyColumns,
                baseTableConglomerateId,
                tableVersion);
    }

    @Override
    public int getPriority() {
        return SchedulerPriorities.INSTANCE.getBasePriority(StatisticsTask.class);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        colsToCollect = (ExecRow)in.readObject();
        rowDecodingMap = ArrayUtil.readIntArray(in);
        columnPositionMap = ArrayUtil.readIntArray(in);
        keyDecodingMap = ArrayUtil.readIntArray(in);
        keyColumnEncodingOrder = ArrayUtil.readIntArray(in);
        if(in.readBoolean())
            keyColumnSortOrder = ArrayUtil.readBooleanArray(in);
        else
            keyColumnSortOrder = null;
        keyColumnTypes = ArrayUtil.readIntArray(in);
        fieldLengths = ArrayUtil.readIntArray(in);
        collectedKeyColumns = (FormatableBitSet)in.readObject();
        tableVersion = in.readUTF();
        baseTableConglomerateId = in.readLong();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeObject(colsToCollect);
        ArrayUtil.writeIntArray(out, rowDecodingMap);
        ArrayUtil.writeIntArray(out, columnPositionMap);
        ArrayUtil.writeIntArray(out,keyDecodingMap);
        ArrayUtil.writeIntArray(out, keyColumnEncodingOrder);
        out.writeBoolean(keyColumnSortOrder!=null);
        if(keyColumnSortOrder!=null)
            ArrayUtil.writeBooleanArray(out, keyColumnSortOrder);
        ArrayUtil.writeIntArray(out, keyColumnTypes);
        ArrayUtil.writeIntArray(out,fieldLengths);
        out.writeObject(collectedKeyColumns);
        out.writeUTF(tableVersion);
        out.writeLong(baseTableConglomerateId);
    }

    /* ****************************************************************************************************************/
    /*private helper methods*/
    private MeasuredRegionScanner getScanner() throws ExecutionException {
        partitionScan.setCacheBlocks(false);
        RegionScanner baseScanner;
        try {
            baseScanner = region.getCoprocessorHost().preScannerOpen(partitionScan);
            if (baseScanner == null) {
                baseScanner = region.getScanner(partitionScan);
            }
        } catch (IOException e) {
            throw new ExecutionException(e);
        }
        partitionScan.setCaching(SpliceConstants.DEFAULT_CACHE_SIZE);

        SDataLib dataLib = HTransactorFactory.getTransactor().getDataLib();
        MetricFactory metricFactory = Metrics.basicMetricFactory();
        int caching = partitionScan.getCaching();
        if(SpliceConstants.useReadAheadScanner)
            return new ReadAheadRegionScanner(region, caching, baseScanner, metricFactory, dataLib);
        else
            return new BufferedRegionScanner(region, baseScanner, partitionScan, caching,metricFactory,dataLib);
    }

    private void writeColumnStats(TransactionalRegion txnRegion,long columnStatsConglomerate,List<ColumnStatistics> collected) throws ExecutionException {
        if(collected.size()<=0) return; //nothing to write, so don't bother
        long tableConglomerateId = Long.parseLong(txnRegion.getTableName());
        //get Row Key
        MultiFieldEncoder keyEncoder = MultiFieldEncoder.create(3);
        keyEncoder = keyEncoder.encodeNext(tableConglomerateId)
                .encodeNext(region.getRegionInfo().getEncodedName());
        keyEncoder.mark();

        BitSet nonNullRowFields = new BitSet();
        nonNullRowFields.set(3);
        EntryEncoder rowEncoder = EntryEncoder.create(SpliceKryoRegistry.getInstance(),1,nonNullRowFields,null,null,null);
        int i=0;
        Kryo kryo = SpliceKryoRegistry.getInstance().get();
        try(CallBuffer<KVPair> buffer = SpliceDriver.driver().getTableWriter().writeBuffer(Long.toString(columnStatsConglomerate).getBytes(),getTxn())){
            Output output = new Output(128,-1);
            ObjectOutput byteOutput = new KryoObjectOutput(output,kryo);
            for (ColumnStatistics stats : collected) {
                keyEncoder.reset();
                int colPos = columnPositionMap[i];
                byte[] key = keyEncoder.encodeNext(colPos+1).build();

                output.clear();
                byteOutput.writeObject(stats); //can just write the object since our implementations are Externalizable
                rowEncoder.getEntryEncoder().reset(); //just reset the code, don't reset the index
                rowEncoder.getEntryEncoder().encodeNextUnsorted(output.toBytes());
                byte[] row = rowEncoder.encode();

                KVPair kvPair = new KVPair(key,row, KVPair.Type.UPSERT);
                buffer.add(kvPair);

                i++; //go to the next column
            }
            buffer.flushBuffer();
        } catch (Exception e) {
            throw new ExecutionException(e);
        } finally{
            SpliceKryoRegistry.getInstance().returnInstance(kryo);
        }
    }

    private void writeTableStats(TransactionalRegion txnRegion, long tableStatsConglomerate,PartitionStatistics collected) throws ExecutionException {
        long tableConglomerateId = Long.parseLong(txnRegion.getTableName());
        //get Row Key
        MultiFieldEncoder keyEncoder = MultiFieldEncoder.create(2);
        keyEncoder = keyEncoder.encodeNext(tableConglomerateId)
                .encodeNext(region.getRegionInfo().getEncodedName());
        byte[] rowKey = keyEncoder.build();

        BitSet nonNullRowFields = new BitSet();
        nonNullRowFields.set(2,12);
        BitSet scalarFields = new BitSet();
        scalarFields.set(2);
        scalarFields.set(5,12);

        BitSet floatFields = new BitSet();
        EntryEncoder rowEncoder = EntryEncoder.create(SpliceKryoRegistry.getInstance(),11,nonNullRowFields,scalarFields,floatFields, floatFields);

        try(CallBuffer<KVPair> buffer = SpliceDriver.driver().getTableWriter().writeBuffer(Long.toString(tableStatsConglomerate).getBytes(),getTxn())){
            MultiFieldEncoder rEncoder = rowEncoder.getEntryEncoder();
            Timestamp timestamp = new Timestamp(System.currentTimeMillis());
//            System.out.printf("Recording row %s|%s: %s|%b|%b|%d|%d|%d|%d%n",tableConglomerateId,txnRegion.getRegionName(),
//                    timestamp,false,false,collected.rowCount(),collected.totalSize(),collected.avgRowWidth(),collected.queryCount());
            rEncoder.encodeNext(TimestampV2DescriptorSerializer.formatLong(timestamp))
                    .encodeNext(false)
                    .encodeNext(false)
                    .encodeNext(collected.rowCount())
                    .encodeNext(collected.totalSize())
                    .encodeNext(collected.avgRowWidth())
                    .encodeNext(collected.queryCount())
                    .encodeNext(collected.localReadTime())
                    .encodeNext(collected.remoteReadTime())
                    .encodeNext(collected.remoteReadTime()); //TODO -sf- add in write latency

            byte[] row = rowEncoder.encode();

            KVPair kvPair = new KVPair(rowKey,row, KVPair.Type.UPSERT);
            buffer.add(kvPair);
            buffer.flushBuffer();
        } catch (Exception e) {
            throw new ExecutionException(e);
        }
    }

    private long[] getStatsConglomerateIds() throws ExecutionException {
        try (SpliceTransactionResourceImpl txn = new SpliceTransactionResourceImpl()){
            txn.marshallTransaction(getTxn());

            EmbedConnection dbConn = (EmbedConnection) SpliceDriver.driver().getInternalConnection();
            LanguageConnectionContext lcc = dbConn.getLanguageConnection();
            DataDictionary dd = lcc.getDataDictionary();
            SchemaDescriptor sysSchema = dd.getSystemSchemaDescriptor();

            long[] ids = new long[3];
            TableDescriptor tableColDesc = dd.getTableDescriptor("SYSTABLESTATS",
                    sysSchema, lcc.getTransactionExecute());
            ids[0] = tableColDesc.getHeapConglomerateId();
            TableDescriptor colColDesc = dd.getTableDescriptor("SYSCOLUMNSTATS",
                    sysSchema, lcc.getTransactionExecute());
            ids[1] = colColDesc.getHeapConglomerateId();
            TableDescriptor physColDesc = dd.getTableDescriptor("SYSPHYSICALSTATS",
                    sysSchema, lcc.getTransactionExecute());
            ids[2] = physColDesc.getHeapConglomerateId();
            return ids;
        } catch (StandardException | SQLException e) {
            throw new ExecutionException(e);
        }
    }
}
