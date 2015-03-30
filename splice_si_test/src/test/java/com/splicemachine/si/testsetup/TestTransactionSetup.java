package com.splicemachine.si.testsetup;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.splicemachine.constants.SIConstants;
import com.splicemachine.si.api.*;
import com.splicemachine.si.data.api.SDataLib;
import com.splicemachine.si.data.api.STableReader;
import com.splicemachine.si.data.api.STableWriter;
import com.splicemachine.si.data.light.LGet;
import com.splicemachine.si.data.light.LTuple;
import com.splicemachine.si.impl.*;
import com.splicemachine.si.impl.readresolve.NoOpReadResolver;
import com.splicemachine.si.impl.store.CompletedTxnCacheSupplier;
import com.splicemachine.si.jmx.ManagedTransactor;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.OperationWithAttributes;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.List;
import java.util.Map;
import java.util.NavigableSet;

/**
 * A Setup class for use in testing code.
 */
@SuppressWarnings("unchecked")
public class TestTransactionSetup {

    byte[] family;
    byte[] ageQualifier;
    byte[] jobQualifier;
    int agePosition = 0;
    int jobPosition = 1;

    TxnOperationFactory txnOperationFactory;
    public Transactor transactor;
    public ManagedTransactor hTransactor;
    public RollForward rollForwardQueue;
    public DataStore dataStore;
    public TimestampSource timestampSource = new SimpleTimestampSource();
    public TransactionReadController readController;

    public ManualKeepAliveScheduler keepAliveScheduler;
    public final TxnStore txnStore;
    public final TxnSupplier txnSupplier;
    public TxnLifecycleManager txnLifecycleManager;
    public ReadResolver readResolver = NoOpReadResolver.INSTANCE; //test read-resolvers through different mechanisms

    private final boolean isInMemory;

    public TestTransactionSetup(StoreSetup storeSetup, boolean simple) {
        isInMemory = !(storeSetup instanceof HStoreSetup);
        final SDataLib dataLib = storeSetup.getDataLib();
        final STableReader reader = storeSetup.getReader();
        final STableWriter writer = storeSetup.getWriter();

        family = dataLib.encode(SIConstants.DEFAULT_FAMILY_BYTES);
        ageQualifier = dataLib.encode(Bytes.toBytes("age"));
        jobQualifier = dataLib.encode(Bytes.toBytes("job"));

        final ManagedTransactor listener = new ManagedTransactor();

        timestampSource = storeSetup.getTimestampSource();
        ClientTxnLifecycleManager lfManager = new ClientTxnLifecycleManager(timestampSource);

        txnStore = storeSetup.getTxnStore();
        if (isInMemory) {
            ((InMemoryTxnStore) txnStore).setLifecycleManager(lfManager);
        }
        txnSupplier = new CompletedTxnCacheSupplier(txnStore, 100, 16);
        lfManager.setStore(txnStore);
        txnLifecycleManager = lfManager;

        txnOperationFactory = new SimpleOperationFactory();

        //noinspection unchecked
        dataStore = new DataStore(dataLib, reader, writer,
                SIConstants.SI_NEEDED,
                SIConstants.SI_DELETE_PUT,
                SIConstants.SNAPSHOT_ISOLATION_COMMIT_TIMESTAMP_COLUMN_BYTES,
                SIConstants.SNAPSHOT_ISOLATION_TOMBSTONE_COLUMN_BYTES,
                SIConstants.EMPTY_BYTE_ARRAY,
                SIConstants.SNAPSHOT_ISOLATION_ANTI_TOMBSTONE_VALUE_BYTES,
                SIConstants.SNAPSHOT_ISOLATION_FAILED_TIMESTAMP,
                SIConstants.DEFAULT_FAMILY_BYTES,
                txnSupplier, txnLifecycleManager
        );


        keepAliveScheduler = new ManualKeepAliveScheduler(txnStore);
        ((ClientTxnLifecycleManager) txnLifecycleManager).setKeepAliveScheduler(keepAliveScheduler);

        readController = new SITransactionReadController(dataStore, dataLib, txnStore);

        SITransactor.Builder builder = new SITransactor.Builder()
                .dataLib(dataLib)
                .dataWriter(writer)
                .dataStore(dataStore)
                .txnStore(txnSupplier) //use the cache for completed transactions
                .operationFactory(txnOperationFactory);

        transactor = builder.build();

        if (!simple) {
            listener.setTransactor(transactor);
            hTransactor = listener;
        }
    }

		/*
         * The following methods are in place to bridge the goofiness gap between real code (i.e. HBase) and
		 * the stupid test code, without requiring odd production-level classes and methods which don't have good
		 * type signatures and don't make sense within the system. Someday, we'll remove the test Operation logic
		 * entirely and replace it with an in-memory HBase installation
		 */

    public OperationWithAttributes convertTestTypePut(Put put) {
        if (isInMemory) {
            OperationWithAttributes owa = new LTuple(put.getRow(), Lists.newArrayList(Iterables.concat(put.getFamilyMap().values())));
            copyAttributes(put, owa);
            return owa;
        } else return put;
    }

    private static void copyAttributes(OperationWithAttributes source, OperationWithAttributes dest) {
        Map<String, byte[]> attributesMap = source.getAttributesMap();
        for (Map.Entry<String, byte[]> attribute : attributesMap.entrySet()) {
            dest.setAttribute(attribute.getKey(), attribute.getValue());
        }
    }

    protected OperationWithAttributes convertTestTypeScan(Scan scan, Long effectiveTimestamp) {
        if (isInMemory) {
            List<List<byte[]>> columns = Lists.newArrayList();
            List<byte[]> families = Lists.newArrayList();
            Map<byte[], NavigableSet<byte[]>> familyMap = scan.getFamilyMap();
            for (byte[] family : families) {
                List<byte[]> columnsForFamily = Lists.newArrayList(familyMap.get(family));
                columns.add(columnsForFamily);
            }
            if (families.size() <= 0)
                families = null;
            if (columns.size() <= 0)
                columns = null;

            OperationWithAttributes owa = new LGet(scan.getStartRow(), scan.getStopRow(),
                    families,
                    columns, effectiveTimestamp, scan.getMaxVersions());
            copyAttributes(scan, owa);
            return owa;
        } else return scan;
    }

    public OperationWithAttributes convertTestTypeGet(Get scan, Long effectiveTimestamp) {
        if (isInMemory) {
            List<List<byte[]>> columns = Lists.newArrayList();
            List<byte[]> families = Lists.newArrayList();
            Map<byte[], NavigableSet<byte[]>> familyMap = scan.getFamilyMap();
            for (byte[] family : familyMap.keySet()) {
                families.add(family);
                List<byte[]> columnsForFamily = Lists.newArrayList(familyMap.get(family));
                columns.add(columnsForFamily);
            }
            if (families.size() <= 0)
                families = null;
            if (columns.size() <= 0)
                columns = null;

            OperationWithAttributes owa = new LGet(scan.getRow(), scan.getRow(),
                    families,
                    columns, effectiveTimestamp, scan.getMaxVersions());
            copyAttributes(scan, owa);
            return owa;
        } else return scan;

    }
}
