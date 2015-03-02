package com.splicemachine.derby.impl.stats;

import com.carrotsearch.hppc.BitSet;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import com.splicemachine.SpliceKryoRegistry;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.impl.job.ZkTask;
import com.splicemachine.derby.impl.job.coprocessor.RegionTask;
import com.splicemachine.derby.impl.job.scheduler.SchedulerPriorities;
import com.splicemachine.derby.impl.sql.execute.operations.DerbyScanInformation;
import com.splicemachine.encoding.Encoder;
import com.splicemachine.encoding.MultiFieldEncoder;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.pipeline.api.CallBuffer;
import com.splicemachine.si.api.TransactionalRegion;
import com.splicemachine.si.api.Txn;
import com.splicemachine.si.api.TxnLifecycleManager;
import com.splicemachine.si.api.TxnView;
import com.splicemachine.si.impl.TransactionalRegions;
import com.splicemachine.si.impl.TxnRegion;
import com.splicemachine.stats.ColumnStatistics;
import com.splicemachine.stats.PartitionStatistics;
import com.splicemachine.storage.EntryEncoder;
import com.splicemachine.storage.index.BitIndex;
import com.splicemachine.utils.SpliceZooKeeperManager;
import com.splicemachine.utils.kryo.KryoObjectOutput;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.io.ArrayUtil;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.SchemaDescriptor;
import org.apache.derby.iapi.sql.dictionary.TableDescriptor;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.impl.jdbc.EmbedConnection;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.concurrent.Callable;
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
    private int[] keyDecodingMap;
    private int[] keyColumnEncodingOrder;
    private boolean[] keyColumnSortOrder;
    private int[] keyColumnTypes;
    private FormatableBitSet collectedKeyColumns;
    private String tableVersion;

    @Override
    public void prepareTask(byte[] start, byte[] stop, RegionCoprocessorEnvironment rce, SpliceZooKeeperManager zooKeeper) throws ExecutionException {
        partitionScan = new Scan(start,stop);

        super.prepareTask(start, stop, rce, zooKeeper);
    }

    @Override
    protected void doExecute() throws ExecutionException, InterruptedException {
        Txn txn = getTxn();

        try(TransactionalRegion txnRegion = TransactionalRegions.get(region)) {
            EmbedConnection connection = (EmbedConnection)SpliceDriver.driver().getInternalConnection();
            tableVersion = DerbyScanInformation.fetchTableVersion(
                            Long.parseLong(txnRegion.getTableName()),connection.getLanguageConnection());
            StatisticsCollector collector = new StatisticsCollector(txn, colsToCollect, partitionScan,
                    rowDecodingMap,
                    keyColumnEncodingOrder,
                    keyColumnSortOrder,
                    keyColumnTypes,
                    keyDecodingMap,
                    collectedKeyColumns,
                    tableVersion,
                    txnRegion);

            PartitionStatistics collected = collector.collect();

            long[] statsTableIds = getStatsConglomerateIds();
            writeTableStats(txnRegion,statsTableIds[0],collected);
            writeColumnStats(txnRegion,statsTableIds[1], collected.columnStatistics());
            writePhysicalStats(statsTableIds[2], collected);
        }
    }

    @Override
    protected Txn beginChildTransaction(TxnView parentTxn, TxnLifecycleManager tc) throws IOException {
        //we want to read data using a READ_UNCOMMITTED view, so that we are as optimistic as possible
        return tc.beginChildTransaction(parentTxn, Txn.IsolationLevel.READ_UNCOMMITTED, null);
    }

    @Override protected String getTaskType() { return "StatisticsCollection"; }
    @Override public boolean invalidateOnClose() { return true; }

    @Override
    public RegionTask getClone() {
        throw new UnsupportedOperationException("IMPLEMENT");
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
        keyDecodingMap = ArrayUtil.readIntArray(in);
        keyColumnEncodingOrder = ArrayUtil.readIntArray(in);
        keyColumnSortOrder = ArrayUtil.readBooleanArray(in);
        keyColumnTypes = ArrayUtil.readIntArray(in);
        collectedKeyColumns = (FormatableBitSet)in.readObject();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeObject(colsToCollect);
        ArrayUtil.writeIntArray(out,rowDecodingMap);
        ArrayUtil.writeIntArray(out,keyDecodingMap);
        ArrayUtil.writeIntArray(out, keyColumnEncodingOrder);
        ArrayUtil.writeBooleanArray(out, keyColumnSortOrder);
        ArrayUtil.writeIntArray(out, keyColumnTypes);
        out.writeObject(collectedKeyColumns);
    }

    /* ****************************************************************************************************************/
    /*private helper methods*/
    private void writeColumnStats(TransactionalRegion txnRegion,long columnStatsConglomerate,List<ColumnStatistics> collected) throws ExecutionException {
        long tableConglomerateId = Long.parseLong(txnRegion.getTableName());
        //get Row Key
        MultiFieldEncoder keyEncoder = MultiFieldEncoder.create(3);
        keyEncoder = keyEncoder.encodeNext(tableConglomerateId)
                .encodeNext(txnRegion.getRegionName());
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
                byte[] key = keyEncoder.encodeNext(i).build();

                output.clear();
                byteOutput.writeObject(stats); //can just write the object since our implementations are Externalizable
                rowEncoder.getEntryEncoder().setRawBytes(output.toBytes());
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

    private void writePhysicalStats(long physStatsConglomerate,PartitionStatistics collected) throws ExecutionException {
        //TODO -sf- update physical statistics here!
    }

    private void writeTableStats(TransactionalRegion txnRegion, long tableStatsConglomerate,PartitionStatistics collected) throws ExecutionException {
        long tableConglomerateId = Long.parseLong(txnRegion.getTableName());
        //get Row Key
        MultiFieldEncoder keyEncoder = MultiFieldEncoder.create(2);
        keyEncoder = keyEncoder.encodeNext(tableConglomerateId)
                .encodeNext(txnRegion.getRegionName());
        byte[] rowKey = keyEncoder.build();

        BitSet nonNullRowFields = new BitSet();
        nonNullRowFields.set(2,8);
        BitSet scalarFields = new BitSet();
        scalarFields.set(2);
        scalarFields.set(5);
        scalarFields.set(6);
        scalarFields.set(8);

        EntryEncoder rowEncoder = EntryEncoder.create(SpliceKryoRegistry.getInstance(),1,nonNullRowFields,null,null,null);
        try(CallBuffer<KVPair> buffer = SpliceDriver.driver().getTableWriter().writeBuffer(Long.toString(tableStatsConglomerate).getBytes(),getTxn())){
            MultiFieldEncoder rEncoder = rowEncoder.getEntryEncoder();
            rEncoder.encodeNext(System.currentTimeMillis())
                    .encodeNext(false)
                    .encodeNext(false)
                    .encodeNext(collected.rowCount())
                    .encodeNext(collected.totalSize())
                    .encodeNext(collected.avgRowWidth())
                    .encodeNext(collected.queryCount());

            byte[] row = rEncoder.build();

            KVPair kvPair = new KVPair(rowKey,row, KVPair.Type.UPSERT);
            buffer.add(kvPair);
            buffer.flushBuffer();
        } catch (Exception e) {
            throw new ExecutionException(e);
        }
    }

    private long[] getStatsConglomerateIds() throws ExecutionException {
        EmbedConnection dbConn = (EmbedConnection) SpliceDriver.driver().getInternalConnection();
        LanguageConnectionContext lcc = dbConn.getLanguageConnection();
        DataDictionary dd = lcc.getDataDictionary();
        try {
            SchemaDescriptor sysSchema = dd.getSystemSchemaDescriptor();

            long[] ids = new long[3];
            TableDescriptor tableColDesc = dd.getTableDescriptor("SYSTABLESTATISTICS",
                    sysSchema, lcc.getTransactionExecute());
            ids[0] = tableColDesc.getHeapConglomerateId();
            TableDescriptor colColDesc = dd.getTableDescriptor("SYSCOLUMNSTATISTICS",
                    sysSchema, lcc.getTransactionExecute());
            ids[1] = colColDesc.getHeapConglomerateId();
            TableDescriptor physColDesc = dd.getTableDescriptor("SYSPHYSICALSTATISTICS",
                    sysSchema, lcc.getTransactionExecute());
            ids[2] = physColDesc.getHeapConglomerateId();
            return ids;
        } catch (StandardException e) {
            throw new ExecutionException(e);
        }
    }
}
