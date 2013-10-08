package com.splicemachine.si;

import com.google.common.base.Function;
import com.splicemachine.constants.SIConstants;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.si.api.TransactionStatus;
import com.splicemachine.si.api.Transactor;
import com.splicemachine.si.data.api.SDataLib;
import com.splicemachine.si.data.api.STableReader;
import com.splicemachine.si.data.hbase.HbRegion;
import com.splicemachine.si.data.light.IncrementingClock;
import com.splicemachine.si.data.light.LRowAccumulator;
import com.splicemachine.si.data.light.LStore;
import com.splicemachine.si.data.light.LTuple;
import com.splicemachine.si.impl.FilterState;
import com.splicemachine.si.impl.FilterStatePacked;
import com.splicemachine.si.impl.IFilterState;
import com.splicemachine.si.impl.RollForwardAction;
import com.splicemachine.si.impl.SynchronousRollForwardQueue;
import com.splicemachine.si.impl.Tracer;
import com.splicemachine.si.impl.Transaction;
import com.splicemachine.si.impl.TransactionId;
import com.splicemachine.si.impl.WriteConflict;
import com.splicemachine.storage.EntryDecoder;
import com.splicemachine.storage.EntryEncoder;
import com.splicemachine.storage.EntryPredicateFilter;
import com.splicemachine.storage.Predicate;
import com.splicemachine.utils.kryo.KryoPool;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.OperationStatus;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class SITransactorTest extends SIConstants {
    boolean useSimple = true;
    boolean usePacked = false;

    StoreSetup storeSetup;
    TransactorSetup transactorSetup;
    Transactor transactor;

    void baseSetUp() {
        transactor = transactorSetup.transactor;
        transactorSetup.rollForwardQueue = new SynchronousRollForwardQueue(
                storeSetup.getHasher(),
                new RollForwardAction() {
                    @Override
                    public Boolean rollForward(long transactionId, List rowList) throws IOException {
                        final STableReader reader = storeSetup.getReader();
                        Object testSTable = reader.open(storeSetup.getPersonTableName());
                        transactor.rollForward(testSTable, transactionId, rowList);
                        return true;
                    }
                }, 10, 100, 1000, "test");
    }

    @Before
    public void setUp() {
        SynchronousRollForwardQueue.scheduler = Executors.newScheduledThreadPool(1);
        storeSetup = new LStoreSetup();
        transactorSetup = new TransactorSetup(storeSetup, true);
        baseSetUp();
    }

    @After
    public void tearDown() throws Exception {
    }

    private void insertAge(TransactionId transactionId, String name, Integer age) throws IOException {
        insertAgeDirect(useSimple, usePacked, transactorSetup, storeSetup, transactionId, name, age);
    }

    private void insertAgeBatch(Object[]... args) throws IOException {
        insertAgeDirectBatch(useSimple, usePacked, transactorSetup, storeSetup, args);
    }

    private void insertJob(TransactionId transactionId, String name, String job) throws IOException {
        insertJobDirect(useSimple, usePacked, transactorSetup, storeSetup, transactionId, name, job);
    }

    private void deleteRow(TransactionId transactionId, String name) throws IOException {
        deleteRowDirect(useSimple, usePacked, transactorSetup, storeSetup, transactionId, name);
    }

    private String read(TransactionId transactionId, String name) throws IOException {
        return readAgeDirect(useSimple, usePacked, transactorSetup, storeSetup, transactionId, name);
    }

    private String scan(TransactionId transactionId, String name) throws IOException {
        final SDataLib dataLib = storeSetup.getDataLib();
        final STableReader reader = storeSetup.getReader();

        Object key = dataLib.newRowKey(new Object[]{name});
        Object scan = makeScan(dataLib, key, null, key);
        transactorSetup.clientTransactor.initializeScan(transactionId.getTransactionIdString(), scan, true);
        Object testSTable = reader.open(storeSetup.getPersonTableName());
        try {
            Iterator results = reader.scan(testSTable, scan);
            if (results.hasNext()) {
                Object rawTuple = results.next();
                Assert.assertTrue(!results.hasNext());
                return readRawTuple(useSimple, usePacked, transactorSetup, storeSetup, transactionId, name, dataLib, rawTuple, false, true,
                        true, true);
            } else {
                return "";
            }
        } finally {
            reader.close(testSTable);
        }
    }

    private String scanNoColumns(TransactionId transactionId, String name, boolean deleted) throws IOException {
        return scanNoColumnsDirect(useSimple, usePacked, transactorSetup, storeSetup, transactionId, name, deleted);
    }

    private String scanAll(TransactionId transactionId, String startKey, String stopKey, Integer filterValue,
                           boolean includeSIColumn) throws IOException {
        final SDataLib dataLib = storeSetup.getDataLib();
        final STableReader reader = storeSetup.getReader();

        Object key = dataLib.newRowKey(new Object[]{startKey});
        Object endKey = dataLib.newRowKey(new Object[]{stopKey});
        Object get = makeScan(dataLib, endKey, null, key);
        if (!useSimple && filterValue != null) {
            final Scan scan = (Scan) get;
            SingleColumnValueFilter filter = new SingleColumnValueFilter((byte[]) transactorSetup.family,
                    (byte[]) transactorSetup.ageQualifier,
                    CompareFilter.CompareOp.EQUAL,
                    new BinaryComparator((byte[]) dataLib.encode(filterValue)));
            filter.setFilterIfMissing(true);
            scan.setFilter(filter);
        }
        transactorSetup.clientTransactor.initializeScan(transactionId.getTransactionIdString(), get, includeSIColumn
        );
        Object testSTable = reader.open(storeSetup.getPersonTableName());
        try {
            Iterator results = reader.scan(testSTable, get);

            StringBuilder result = new StringBuilder();
            while (results.hasNext()) {
                final Object value = results.next();
                final String name = (String) dataLib.decode(dataLib.getResultKey(value), String.class);
                final String s = readRawTuple(useSimple, usePacked, transactorSetup, storeSetup, transactionId, name, dataLib, value, false,
                        includeSIColumn, true, true);
                result.append(s);
                if (s.length() > 0) {
                    result.append("\n");
                }
            }
            return result.toString();
        } finally {
            reader.close(testSTable);
        }
    }

    static void insertAgeDirect(boolean useSimple, boolean usePacked, TransactorSetup transactorSetup, StoreSetup storeSetup,
                                TransactionId transactionId, String name, Integer age) throws IOException {
        insertField(useSimple, usePacked, transactorSetup, storeSetup, transactionId, name, transactorSetup.ageQualifier, age);
    }

    static void insertAgeDirectBatch(boolean useSimple, boolean usePacked, TransactorSetup transactorSetup, StoreSetup storeSetup,
                                     Object[] args) throws IOException {
        insertFieldBatch(useSimple, usePacked, transactorSetup, storeSetup, args, transactorSetup.ageQualifier);
    }

    static void insertJobDirect(boolean useSimple, boolean usePacked, TransactorSetup transactorSetup, StoreSetup storeSetup,
                                TransactionId transactionId, String name, String job) throws IOException {
        insertField(useSimple, usePacked, transactorSetup, storeSetup, transactionId, name, transactorSetup.jobQualifier, job);
    }

    private static void insertField(boolean useSimple, boolean usePacked, TransactorSetup transactorSetup, StoreSetup storeSetup,
                                    TransactionId transactionId, String name, Object qualifier, Object fieldValue)
            throws IOException {
        final SDataLib dataLib = storeSetup.getDataLib();
        final STableReader reader = storeSetup.getReader();
        Object put = makePut(useSimple, usePacked, transactorSetup, transactionId, name, qualifier, fieldValue, dataLib);
        processPutDirect(useSimple, usePacked, transactorSetup, storeSetup, reader, put);
    }

    private static Object makePut(boolean useSimple, boolean usePacked, TransactorSetup transactorSetup, TransactionId transactionId, String name, Object qualifier, Object fieldValue, SDataLib dataLib) throws IOException {
        Object key = dataLib.newRowKey(new Object[]{name});
        Object put = dataLib.newPut(key);
        if (fieldValue != null) {
            if (usePacked && !useSimple) {
                int columnIndex;
                if (dataLib.valuesEqual(qualifier, transactorSetup.ageQualifier)) {
                    columnIndex = 0;
                } else if (dataLib.valuesEqual(qualifier, transactorSetup.jobQualifier)) {
                    columnIndex = 1;
                } else {
                    throw new RuntimeException("unknown qualifier");
                }
                final BitSet bitSet = new BitSet();
                bitSet.set(columnIndex);
                final EntryEncoder entryEncoder = EntryEncoder.create(KryoPool.defaultPool(), 2, bitSet, null, null, null);
                try {
                    if (columnIndex == 0) {
                        entryEncoder.getEntryEncoder().encodeNext((Integer) fieldValue);
                    } else {
                        entryEncoder.getEntryEncoder().encodeNext((String) fieldValue);
                    }
                    final byte[] packedRow = entryEncoder.encode();
                    dataLib.addKeyValueToPut(put, transactorSetup.family, dataLib.encode("x"), null, packedRow);
                } finally {
                    entryEncoder.close();
                }
            } else {
                dataLib.addKeyValueToPut(put, transactorSetup.family, qualifier, null, dataLib.encode(fieldValue));
            }
        }
        transactorSetup.clientTransactor.initializePut(transactionId.getTransactionIdString(), put);
        return put;
    }

    private static void insertFieldBatch(boolean useSimple, boolean usePacked, TransactorSetup transactorSetup, StoreSetup storeSetup,
                                         Object[] args, Object qualifier) throws IOException {
        final SDataLib dataLib = storeSetup.getDataLib();
        final STableReader reader = storeSetup.getReader();
        Object[] puts = new Object[args.length];
        int i = 0;
        for (Object subArgs : args) {
            Object[] subArgsArray = (Object[]) subArgs;
            TransactionId transactionId = (TransactionId) subArgsArray[0];
            String name = (String) subArgsArray[1];
            Object fieldValue = subArgsArray[2];
            Object put = makePut(useSimple, usePacked, transactorSetup, transactionId, name, qualifier, fieldValue, dataLib);
            puts[i] = put;
            i++;
        }
        processPutDirectBatch(useSimple, usePacked, transactorSetup, storeSetup, reader, puts);
    }

    static void deleteRowDirect(boolean useSimple, boolean usePacked, TransactorSetup transactorSetup, StoreSetup storeSetup,
                                TransactionId transactionId, String name) throws IOException {
        final SDataLib dataLib = storeSetup.getDataLib();
        final STableReader reader = storeSetup.getReader();

        Object key = dataLib.newRowKey(new Object[]{name});
        Object deletePut = transactorSetup.clientTransactor.createDeletePut(transactionId, key);
        processPutDirect(useSimple, usePacked, transactorSetup, storeSetup, reader, deletePut);
    }

    private static void processPutDirect(boolean useSimple, boolean usePacked,
                                         TransactorSetup transactorSetup, StoreSetup storeSetup, STableReader reader,
                                         Object put) throws IOException {
        Object testSTable = reader.open(storeSetup.getPersonTableName());
        try {
            if (useSimple) {
                if (usePacked) {
                    Assert.assertTrue(transactorSetup.transactor.processPut(testSTable, transactorSetup.rollForwardQueue,
                            ((LTuple) put).pack("V", "p")));
                } else {
                    Assert.assertTrue(transactorSetup.transactor.processPut(testSTable, transactorSetup.rollForwardQueue, put));
                }
            } else {
                storeSetup.getWriter().write(testSTable, put);
            }
        } finally {
            reader.close(testSTable);
        }
    }

    private static void processPutDirectBatch(boolean useSimple, boolean usePacked,
                                              TransactorSetup transactorSetup, StoreSetup storeSetup, STableReader reader,
                                              Object[] puts) throws IOException {
        final Object testSTable = reader.open(storeSetup.getPersonTableName());
        try {
            Object tableToUse = testSTable;
            if (useSimple) {
                if (usePacked) {
                    Object[] packedPuts = new Object[puts.length];
                    for (int i = 0; i < puts.length; i++) {
                        packedPuts[i] = ((LTuple) puts[i]).pack("V", "p");
                    }
                    puts = packedPuts;
                }
            } else {
                tableToUse = new HbRegion(HStoreSetup.regionMap.get(storeSetup.getPersonTableName()));
            }
            final Object[] results = transactorSetup.transactor.processPutBatch(tableToUse, transactorSetup.rollForwardQueue, puts);
            for (Object r : results) {
                Assert.assertEquals(HConstants.OperationStatusCode.SUCCESS, ((OperationStatus) r).getOperationStatusCode());
            }
        } finally {
            reader.close(testSTable);
        }
    }

    static String readAgeDirect(boolean useSimple, boolean usePacked, TransactorSetup transactorSetup, StoreSetup storeSetup,
                                TransactionId transactionId, String name) throws IOException {
        final SDataLib dataLib = storeSetup.getDataLib();
        final STableReader reader = storeSetup.getReader();

        Object key = dataLib.newRowKey(new Object[]{name});
        Object get = makeGet(dataLib, key);
        transactorSetup.clientTransactor.initializeGet(transactionId.getTransactionIdString(), get);
        Object testSTable = reader.open(storeSetup.getPersonTableName());
        try {
            Object rawTuple = reader.get(testSTable, get);
            return readRawTuple(useSimple, usePacked, transactorSetup, storeSetup, transactionId, name, dataLib, rawTuple, true, true, false, true);
        } finally {
            reader.close(testSTable);
        }
    }

    static String scanNoColumnsDirect(boolean useSimple, boolean usePacked, TransactorSetup transactorSetup, StoreSetup storeSetup,
                                      TransactionId transactionId, String name, boolean deleted) throws IOException {
        final SDataLib dataLib = storeSetup.getDataLib();
        final STableReader reader = storeSetup.getReader();

        final Object endKey = dataLib.newRowKey(new Object[]{name});
        final ArrayList families = new ArrayList();
        final Object startKey = endKey;
        Object scan = makeScan(dataLib, endKey, families, startKey);
        transactorSetup.clientTransactor.initializeScan(transactionId.getTransactionIdString(), scan, true);
        transactorSetup.transactor.preProcessScan(scan);
        Object testSTable = reader.open(storeSetup.getPersonTableName());
        try {
            Iterator results = reader.scan(testSTable, scan);
            if (deleted) {
            } else {
                Assert.assertTrue(results.hasNext());
            }
            Object rawTuple = results.next();
            Assert.assertTrue(!results.hasNext());
            return readRawTuple(useSimple, usePacked, transactorSetup, storeSetup, transactionId, name, dataLib, rawTuple, false, true, false, true);
        } finally {
            reader.close(testSTable);
        }
    }

    private static String readRawTuple(boolean useSimple, boolean usePacked, TransactorSetup transactorSetup, StoreSetup storeSetup,
                                       TransactionId transactionId, String name, SDataLib dataLib, Object rawTuple,
                                       boolean singleRowRead, boolean includeSIColumn,
                                       boolean dumpKeyValues, boolean decodeTimestamps)
            throws IOException {
        if (rawTuple != null) {
            Object result = rawTuple;
            if (useSimple) {
                IFilterState filterState;
                try {
                    filterState = transactorSetup.transactor.newFilterState(transactorSetup.rollForwardQueue,
                            transactionId, includeSIColumn);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                if (usePacked) {
                    filterState = new FilterStatePacked(null, storeSetup.getDataLib(), transactorSetup.dataStore, (FilterState) filterState, new LRowAccumulator());
                }
                result = transactorSetup.transactor.filterResult(filterState, rawTuple);
                if (usePacked && result != null) {
                    result = ((LTuple) result).unpack("V");
                }
            } else {
                if (((Result) result).size() == 0) {
                    return name + " absent";
                }
                if (usePacked && result != null) {
                    final Object resultKey = dataLib.getResultKey(result);
                    final List newKeyValues = new ArrayList();
                    for (Object kv : dataLib.listResult(result)) {
                        if (dataLib.valuesEqual(dataLib.getKeyValueFamily(kv), transactorSetup.family) &&
                                dataLib.valuesEqual(dataLib.getKeyValueQualifier(kv), dataLib.encode("x"))) {
                            final byte[] packedColumns = (byte[]) dataLib.getKeyValueValue(kv);
                            final EntryDecoder entryDecoder = new EntryDecoder(KryoPool.defaultPool());
                            entryDecoder.set(packedColumns);
                            final MultiFieldDecoder decoder = entryDecoder.getEntryDecoder();
                            if (entryDecoder.isSet(0)) {
                                final int age = decoder.decodeNextInt();
                                final Object ageKeyValue = dataLib.newKeyValue(resultKey, transactorSetup.family,
                                        transactorSetup.ageQualifier,
                                        dataLib.getKeyValueTimestamp(kv),
                                        dataLib.encode(age));
                                newKeyValues.add(ageKeyValue);
                            }
                            if (entryDecoder.isSet(1)) {
                                final String job = decoder.decodeNextString();
                                final Object jobKeyValue = dataLib.newKeyValue(resultKey, transactorSetup.family,
                                        transactorSetup.jobQualifier,
                                        dataLib.getKeyValueTimestamp(kv),
                                        dataLib.encode(job));
                                newKeyValues.add(jobKeyValue);
                            }
                        } else {
                            newKeyValues.add(kv);
                        }
                    }
                    result = dataLib.newResult(resultKey, newKeyValues);
                }
            }
            if (result != null) {
                String suffix = dumpKeyValues ? "[ " + resultToKeyValueString(transactorSetup, name, dataLib, result, decodeTimestamps) + "]" : "";
                return resultToStringDirect(transactorSetup, name, dataLib, result) + suffix;
            }
        }
        if (singleRowRead) {
            return name + " absent";
        } else {
            return "";
        }
    }

    private String resultToString(String name, Object result) {
        return resultToStringDirect(transactorSetup, name, storeSetup.getDataLib(), result);
    }

    private static String resultToStringDirect(TransactorSetup transactorSetup, String name, SDataLib dataLib, Object result) {
        final Object ageValue = dataLib.getResultValue(result, transactorSetup.family, transactorSetup.ageQualifier);
        Integer age = (Integer) dataLib.decode(ageValue, Integer.class);
        final Object jobValue = dataLib.getResultValue(result, transactorSetup.family, transactorSetup.jobQualifier);
        String job = (String) dataLib.decode(jobValue, String.class);
        return name + " age=" + age + " job=" + job;
    }

    private static String resultToKeyValueString(TransactorSetup transactorSetup, String name, SDataLib dataLib,
                                                 Object result, boolean decodeTimestamps) {
        final Map<Long, String> timestampDecoder = new HashMap<Long, String>();
        final StringBuilder s = new StringBuilder();
        for (Object kv : dataLib.listResult(result)) {
            final String family = (String) dataLib.decode(dataLib.getKeyValueFamily(kv), String.class);
            String qualifier = "?";
            String value = "?";
            if (dataLib.valuesEqual(dataLib.getKeyValueQualifier(kv), transactorSetup.ageQualifier)) {
                value = dataLib.decode(dataLib.getKeyValueValue(kv), Integer.class).toString();
                qualifier = "age";
            } else if (dataLib.valuesEqual(dataLib.getKeyValueQualifier(kv), transactorSetup.jobQualifier)) {
                value = (String) dataLib.decode(dataLib.getKeyValueValue(kv), String.class);
                qualifier = "job";
            } else if (dataLib.valuesEqual(dataLib.getKeyValueQualifier(kv), transactorSetup.tombstoneQualifier)) {
                qualifier = "X";
                value = "";
            } else if (dataLib.valuesEqual(dataLib.getKeyValueQualifier(kv), transactorSetup.commitTimestampQualifier)) {
                qualifier = "TX";
                value = "";
            }
            final long timestamp = dataLib.getKeyValueTimestamp(kv);
            String timestampString;
            if (decodeTimestamps) {
                timestampString = timestampToStableString(timestampDecoder, timestamp);
            } else {
                timestampString = new Long(timestamp).toString();
            }
            String equalString = value.length() > 0 ? "=" : "";
            s.append(family + "." + qualifier + "@" + timestampString + equalString + value + " ");
        }
        return s.toString();
    }

    private static String timestampToStableString(Map<Long, String> timestampDecoder, long timestamp) {
        if (timestampDecoder.containsKey(timestamp)) {
            return timestampDecoder.get(timestamp);
        } else {
            final String timestampString = "~" + (9 - timestampDecoder.size());
            timestampDecoder.put(timestamp, timestampString);
            return timestampString;
        }
    }

    private void dumpStore() {
        dumpStore("");
    }

    private void dumpStore(String label) {
        if (useSimple) {
            System.out.println("store " + label + " =" + storeSetup.getStore());
        }
    }

    @Test
    public void writeRead() throws IOException {
        TransactionId t1 = transactor.beginTransaction();
        Assert.assertEquals("joe9 absent", read(t1, "joe9"));
        insertAge(t1, "joe9", 20);
        Assert.assertEquals("joe9 age=20 job=null", read(t1, "joe9"));
        transactor.commit(t1);

        TransactionId t2 = transactor.beginTransaction();
        Assert.assertEquals("joe9 age=20 job=null", read(t2, "joe9"));
    }

    @Test
    public void writeReadOverlap() throws IOException {
        TransactionId t1 = transactor.beginTransaction();
        Assert.assertEquals("joe8 absent", read(t1, "joe8"));
        insertAge(t1, "joe8", 20);
        Assert.assertEquals("joe8 age=20 job=null", read(t1, "joe8"));

        TransactionId t2 = transactor.beginTransaction();
        Assert.assertEquals("joe8 age=20 job=null", read(t1, "joe8"));
        Assert.assertEquals("joe8 absent", read(t2, "joe8"));
        transactor.commit(t1);
        Assert.assertEquals("joe8 absent", read(t2, "joe8"));
    }

    @Test
    public void writeWrite() throws IOException {
        TransactionId t1 = transactor.beginTransaction();
        insertAge(t1, "joe", 20);
        Assert.assertEquals("joe age=20 job=null", read(t1, "joe"));
        transactor.commit(t1);

        TransactionId t2 = transactor.beginTransaction();
        Assert.assertEquals("joe age=20 job=null", read(t2, "joe"));
        insertAge(t2, "joe", 30);
        Assert.assertEquals("joe age=30 job=null", read(t2, "joe"));
        transactor.commit(t2);
    }

    @Test
    public void writeWriteOverlap() throws IOException {
        TransactionId t1 = transactor.beginTransaction();
        Assert.assertEquals("joe012 absent", read(t1, "joe012"));
        insertAge(t1, "joe012", 20);
        Assert.assertEquals("joe012 age=20 job=null", read(t1, "joe012"));

        TransactionId t2 = transactor.beginTransaction();
        Assert.assertEquals("joe012 age=20 job=null", read(t1, "joe012"));
        Assert.assertEquals("joe012 absent", read(t2, "joe012"));
        try {
            insertAge(t2, "joe012", 30);
            Assert.fail();
        } catch (WriteConflict e) {
        } catch (RetriesExhaustedWithDetailsException e) {
            assertWriteConflict(e);
        } finally {
            transactor.fail(t2);
        }
        Assert.assertEquals("joe012 age=20 job=null", read(t1, "joe012"));
        transactor.commit(t1);
        try {
            transactor.commit(t2);
            Assert.fail();
        } catch (DoNotRetryIOException dnrio) {
            Assert.assertTrue("Incorrect message returned!", dnrio.getMessage().matches("transaction [0-9]* is not ACTIVE.*"));
        }
    }

    @Test
    public void writeWriteOverlapRecovery() throws IOException {
        TransactionId t1 = transactor.beginTransaction();
        Assert.assertEquals("joe142 absent", read(t1, "joe142"));
        insertAge(t1, "joe142", 20);
        Assert.assertEquals("joe142 age=20 job=null", read(t1, "joe142"));

        TransactionId t2 = transactor.beginTransaction();
        Assert.assertEquals("joe142 age=20 job=null", read(t1, "joe142"));
        Assert.assertEquals("joe142 absent", read(t2, "joe142"));
        try {
            insertAge(t2, "joe142", 30);
            Assert.fail();
        } catch (WriteConflict e) {
        } catch (RetriesExhaustedWithDetailsException e) {
            assertWriteConflict(e);
        }
        // can still use a transaction after a write conflict
        insertAge(t2, "bob142", 30);
        Assert.assertEquals("bob142 age=30 job=null", read(t2, "bob142"));
        Assert.assertEquals("joe142 age=20 job=null", read(t1, "joe142"));
        transactor.commit(t1);
        transactor.commit(t2);
    }

    @Test
    public void readAfterCommit() throws IOException {
        TransactionId t1 = transactor.beginTransaction();
        insertAge(t1, "joe3", 20);
        transactor.commit(t1);
        Assert.assertEquals("joe3 age=20 job=null", read(t1, "joe3"));
    }

    @Test
    public void rollbackAfterCommit() throws IOException {
        TransactionId t1 = transactor.beginTransaction();
        insertAge(t1, "joe50", 20);
        transactor.commit(t1);
        transactor.rollback(t1);
        Assert.assertEquals("joe50 age=20 job=null", read(t1, "joe50"));
    }

    @Test
    public void writeScan() throws IOException {
        TransactionId t1 = transactor.beginTransaction();
        Assert.assertEquals("joe4 absent", read(t1, "joe4"));
        insertAge(t1, "joe4", 20);
        Assert.assertEquals("joe4 age=20 job=null", read(t1, "joe4"));
        transactor.commit(t1);

        TransactionId t2 = transactor.beginTransaction();
        Assert.assertEquals("joe4 age=20 job=null[ S.TX@~9 V.age@~9=20 ]", scan(t2, "joe4"));

        Assert.assertEquals("joe4 age=20 job=null", read(t2, "joe4"));
    }

    @Test
    public void writeScanWithDeleteActive() throws IOException {
        TransactionId t1 = transactor.beginTransaction();
        insertAge(t1, "joe128", 20);
        transactor.commit(t1);

        TransactionId t2 = transactor.beginTransaction();

        TransactionId t3 = transactor.beginTransaction();

        deleteRow(t2, "joe128");

        Assert.assertEquals("joe128 age=20 job=null[ S.TX@~9 V.age@~9=20 ]", scan(t3, "joe128"));
        transactor.commit(t2);
    }

    @Test
    public void writeScanNoColumns() throws IOException {
        TransactionId t1 = transactor.beginTransaction();
        insertAge(t1, "joe84", 20);
        transactor.commit(t1);

        TransactionId t2 = transactor.beginTransaction();
        // reading si only (i.e. no data columns) returns the rows but none of the "user" data
        Assert.assertEquals("joe84 age=null job=null", scanNoColumns(t2, "joe84", false));
    }

    @Test
    public void writeDeleteScanNoColumns() throws IOException {
        TransactionId t1 = transactor.beginTransaction();
        insertAge(t1, "joe85", 20);
        transactor.commit(t1);

        TransactionId t2 = transactor.beginTransaction();
        deleteRow(t2, "joe85");
        transactor.commit(t2);


        TransactionId t3 = transactor.beginTransaction();
        // reading si only (i.e. no data columns) returns the rows but none of the "user" data
        Assert.assertEquals("", scanNoColumns(t3, "joe85", true));
    }

    @Test
    public void writeScanMultipleRows() throws IOException {
        TransactionId t1 = transactor.beginTransaction();
        insertAge(t1, "17joe", 20);
        insertAge(t1, "17bob", 30);
        insertAge(t1, "17boe", 40);
        insertAge(t1, "17tom", 50);
        transactor.commit(t1);

        TransactionId t2 = transactor.beginTransaction();
        String expected = "17bob age=30 job=null[ V.age@~9=30 ]\n" +
                "17boe age=40 job=null[ V.age@~9=40 ]\n" +
                "17joe age=20 job=null[ V.age@~9=20 ]\n" +
                "17tom age=50 job=null[ V.age@~9=50 ]\n";
        Assert.assertEquals(expected, scanAll(t2, "17a", "17z", null, false));
    }

    @Test
    public void writeScanWithFilter() throws IOException {
        TransactionId t1 = transactor.beginTransaction();
        insertAge(t1, "91joe", 20);
        insertAge(t1, "91bob", 30);
        insertAge(t1, "91boe", 40);
        insertAge(t1, "91tom", 50);
        transactor.commit(t1);

        TransactionId t2 = transactor.beginTransaction();
        String expected = "91boe age=40 job=null[ V.age@~9=40 ]\n";
        if (!useSimple) {
            Assert.assertEquals(expected, scanAll(t2, "91a", "91z", 40, false));
        }
    }

    @Test
    public void writeScanWithFilterAndPendingWrites() throws IOException {
        TransactionId t1 = transactor.beginTransaction();
        insertAge(t1, "92joe", 20);
        insertAge(t1, "92bob", 30);
        insertAge(t1, "92boe", 40);
        insertAge(t1, "92tom", 50);
        transactor.commit(t1);

        TransactionId t2 = transactor.beginTransaction();
        insertAge(t2, "92boe", 41);

        TransactionId t3 = transactor.beginTransaction();
        String expected = "92boe age=40 job=null[ V.age@~9=40 ]\n";
        if (!useSimple) {
            Assert.assertEquals(expected, scanAll(t3, "92a", "92z", 40, false));
        }
    }

    @Test
    public void writeWriteRead() throws IOException {
        TransactionId t1 = transactor.beginTransaction();
        insertAge(t1, "joe5", 20);
        Assert.assertEquals("joe5 age=20 job=null", read(t1, "joe5"));
        transactor.commit(t1);

        TransactionId t2 = transactor.beginTransaction();
        Assert.assertEquals("joe5 age=20 job=null", read(t2, "joe5"));
        insertJob(t2, "joe5", "baker");
        Assert.assertEquals("joe5 age=20 job=baker", read(t2, "joe5"));
        transactor.commit(t2);

        TransactionId t3 = transactor.beginTransaction();
        Assert.assertEquals("joe5 age=20 job=baker", read(t3, "joe5"));
    }

    @Test
    public void multipleWritesSameTransaction() throws IOException {
        TransactionId t1 = transactor.beginTransaction();
        Assert.assertEquals("joe16 absent", read(t1, "joe16"));
        insertAge(t1, "joe16", 20);
        Assert.assertEquals("joe16 age=20 job=null", read(t1, "joe16"));

        insertAge(t1, "joe16", 21);
        Assert.assertEquals("joe16 age=21 job=null", read(t1, "joe16"));

        insertAge(t1, "joe16", 22);
        Assert.assertEquals("joe16 age=22 job=null", read(t1, "joe16"));
        transactor.commit(t1);

        TransactionId t2 = transactor.beginTransaction();
        Assert.assertEquals("joe16 age=22 job=null", read(t2, "joe16"));
        transactor.commit(t2);
    }

    @Test
    public void manyWritesManyRollbacksRead() throws IOException {
        TransactionId t1 = transactor.beginTransaction();
        insertAge(t1, "joe6", 20);
        transactor.commit(t1);

        TransactionId t2 = transactor.beginTransaction();
        insertJob(t2, "joe6", "baker");
        transactor.commit(t2);

        TransactionId t3 = transactor.beginTransaction();
        insertJob(t3, "joe6", "butcher");
        transactor.commit(t3);

        TransactionId t4 = transactor.beginTransaction();
        insertJob(t4, "joe6", "blacksmith");
        transactor.commit(t4);

        TransactionId t5 = transactor.beginTransaction();
        insertJob(t5, "joe6", "carter");
        transactor.commit(t5);

        TransactionId t6 = transactor.beginTransaction();
        insertJob(t6, "joe6", "farrier");
        transactor.commit(t6);

        TransactionId t7 = transactor.beginTransaction();
        insertAge(t7, "joe6", 27);
        transactor.rollback(t7);

        TransactionId t8 = transactor.beginTransaction();
        insertAge(t8, "joe6", 28);
        transactor.rollback(t8);

        TransactionId t9 = transactor.beginTransaction();
        insertAge(t9, "joe6", 29);
        transactor.rollback(t9);

        TransactionId t10 = transactor.beginTransaction();
        insertAge(t10, "joe6", 30);
        transactor.rollback(t10);

        TransactionId t11 = transactor.beginTransaction();
        Assert.assertEquals("joe6 age=20 job=farrier", read(t11, "joe6"));
    }

    @Test
    public void writeDelete() throws IOException {
        TransactionId t1 = transactor.beginTransaction();
        insertAge(t1, "joe10", 20);
        Assert.assertEquals("joe10 age=20 job=null", read(t1, "joe10"));
        transactor.commit(t1);

        TransactionId t2 = transactor.beginTransaction();
        Assert.assertEquals("joe10 age=20 job=null", read(t2, "joe10"));
        deleteRow(t2, "joe10");
        Assert.assertEquals("joe10 absent", read(t2, "joe10"));
        transactor.commit(t2);
    }

    @Test
    public void writeDeleteRead() throws IOException {
        TransactionId t1 = transactor.beginTransaction();
        insertAge(t1, "joe11", 20);
        transactor.commit(t1);

        TransactionId t2 = transactor.beginTransaction();
        deleteRow(t2, "joe11");
        transactor.commit(t2);

        TransactionId t3 = transactor.beginTransaction();
        Assert.assertEquals("joe11 absent", read(t3, "joe11"));
        transactor.commit(t3);
    }

    @Test
    public void writeDeleteRollbackRead() throws IOException {
        TransactionId t1 = transactor.beginTransaction();
        insertAge(t1, "joe90", 20);
        transactor.commit(t1);

        TransactionId t2 = transactor.beginTransaction();
        deleteRow(t2, "joe90");
        transactor.rollback(t2);

        TransactionId t3 = transactor.beginTransaction();
        Assert.assertEquals("joe90 age=20 job=null", read(t3, "joe90"));
        transactor.commit(t3);
    }

    @Test
    public void writeChildDeleteParentRollbackDelete() throws IOException {
        TransactionId t1 = transactor.beginTransaction();
        insertAge(t1, "joe93", 20);
        transactor.commit(t1);

        TransactionId t2 = transactor.beginTransaction();
        TransactionId t3 = transactor.beginChildTransaction(t2, true);
        deleteRow(t3, "joe93");
        transactor.rollback(t2);

        TransactionId t4 = transactor.beginTransaction();
        deleteRow(t4, "joe93");
        Assert.assertEquals("joe93 absent", read(t4, "joe93"));
        transactor.commit(t4);
    }

    @Test
    public void writeDeleteOverlap() throws IOException {
        TransactionId t1 = transactor.beginTransaction();
        insertAge(t1, "joe2", 20);

        TransactionId t2 = transactor.beginTransaction();
        try {
            deleteRow(t2, "joe2");
            Assert.fail();
        } catch (WriteConflict e) {
        } catch (RetriesExhaustedWithDetailsException e) {
            assertWriteConflict(e);
        } finally {
            transactor.fail(t2);
        }
        Assert.assertEquals("joe2 age=20 job=null", read(t1, "joe2"));
        Assert.assertEquals("joe2 age=20 job=null", read(t1, "joe2"));
        transactor.commit(t1);
        try {
            transactor.commit(t2);
            Assert.fail();
        } catch (DoNotRetryIOException dnrio) {
            String message = dnrio.getMessage();
            Assert.assertTrue("Incorrect message pattern returned!", message.matches("transaction [0-9]* is not ACTIVE.*"));
        }
    }

    private void assertWriteConflict(RetriesExhaustedWithDetailsException e) {
        Assert.assertEquals(1, e.getNumExceptions());
        Assert.assertTrue(e.getMessage().startsWith("Failed 1 action: com.splicemachine.si.impl.WriteConflict: write/write conflict"));
    }

    private void assertWriteFailed(RetriesExhaustedWithDetailsException e) {
        Assert.assertEquals(1, e.getNumExceptions());
        Assert.assertTrue(e.getMessage().startsWith("commit failed"));
    }

    @Test
    public void writeWriteDeleteOverlap() throws IOException {
        TransactionId t0 = transactor.beginTransaction();
        insertAge(t0, "joe013", 20);
        transactor.commit(t0);

        TransactionId t1 = transactor.beginTransaction();
        deleteRow(t1, "joe013");

        TransactionId t2 = transactor.beginTransaction();
        try {
            insertAge(t2, "joe013", 21);
            Assert.fail();
        } catch (WriteConflict e) {
        } catch (RetriesExhaustedWithDetailsException e) {
            assertWriteConflict(e);
        } finally {
            transactor.fail(t2);
        }
        Assert.assertEquals("joe013 absent", read(t1, "joe013"));
        transactor.commit(t1);
        try {
            transactor.commit(t2);
            Assert.fail();
        } catch (DoNotRetryIOException dnrio) {
            Assert.assertTrue("Incorrect message returned!", dnrio.getMessage().matches("transaction [0-9]* is not ACTIVE.*"));
        }

        TransactionId t3 = transactor.beginTransaction();
        Assert.assertEquals("joe013 absent", read(t3, "joe013"));
        transactor.commit(t3);
    }

    @Test
    public void writeWriteDeleteWriteRead() throws IOException {
        TransactionId t0 = transactor.beginTransaction();
        insertAge(t0, "joe14", 20);
        transactor.commit(t0);

        TransactionId t1 = transactor.beginTransaction();
        insertJob(t1, "joe14", "baker");
        transactor.commit(t1);

        TransactionId t2 = transactor.beginTransaction();
        deleteRow(t2, "joe14");
        transactor.commit(t2);

        TransactionId t3 = transactor.beginTransaction();
        insertJob(t3, "joe14", "smith");
        Assert.assertEquals("joe14 age=null job=smith", read(t3, "joe14"));
        transactor.commit(t3);

        TransactionId t4 = transactor.beginTransaction();
        Assert.assertEquals("joe14 age=null job=smith", read(t4, "joe14"));
        transactor.commit(t4);
    }

    @Test
    public void writeWriteDeleteWriteDeleteWriteRead() throws IOException {
        TransactionId t0 = transactor.beginTransaction();
        insertAge(t0, "joe15", 20);
        transactor.commit(t0);

        TransactionId t1 = transactor.beginTransaction();
        insertJob(t1, "joe15", "baker");
        transactor.commit(t1);

        TransactionId t2 = transactor.beginTransaction();
        deleteRow(t2, "joe15");
        transactor.commit(t2);

        TransactionId t3 = transactor.beginTransaction();
        insertJob(t3, "joe15", "smith");
        Assert.assertEquals("joe15 age=null job=smith", read(t3, "joe15"));
        transactor.commit(t3);

        TransactionId t4 = transactor.beginTransaction();
        deleteRow(t4, "joe15");
        transactor.commit(t4);

        TransactionId t5 = transactor.beginTransaction();
        insertAge(t5, "joe15", 21);
        transactor.commit(t5);

        TransactionId t6 = transactor.beginTransaction();
        Assert.assertEquals("joe15 age=21 job=null", read(t6, "joe15"));
        transactor.commit(t6);
    }

    @Test
    public void writeManyDeleteOneGets() throws IOException {
        TransactionId t1 = transactor.beginTransaction();
        insertAge(t1, "joe47", 20);
        insertAge(t1, "toe47", 30);
        insertAge(t1, "boe47", 40);
        insertAge(t1, "moe47", 50);
        insertAge(t1, "zoe47", 60);
        transactor.commit(t1);

        TransactionId t2 = transactor.beginTransaction();
        deleteRow(t2, "moe47");
        transactor.commit(t2);

        TransactionId t3 = transactor.beginTransaction();
        Assert.assertEquals("joe47 age=20 job=null", read(t3, "joe47"));
        Assert.assertEquals("toe47 age=30 job=null", read(t3, "toe47"));
        Assert.assertEquals("boe47 age=40 job=null", read(t3, "boe47"));
        Assert.assertEquals("moe47 absent", read(t3, "moe47"));
        Assert.assertEquals("zoe47 age=60 job=null", read(t3, "zoe47"));
    }

    @Test
    public void writeManyDeleteOneScan() throws IOException {
        TransactionId t1 = transactor.beginTransaction();
        insertAge(t1, "48joe", 20);
        insertAge(t1, "48toe", 30);
        insertAge(t1, "48boe", 40);
        insertAge(t1, "48moe", 50);
        insertAge(t1, "48xoe", 60);
        transactor.commit(t1);

        TransactionId t2 = transactor.beginTransaction();
        deleteRow(t2, "48moe");
        transactor.commit(t2);

        TransactionId t3 = transactor.beginTransaction();
        String expected = "48boe age=40 job=null[ V.age@~9=40 ]\n" +
                "48joe age=20 job=null[ V.age@~9=20 ]\n" +
                "48toe age=30 job=null[ V.age@~9=30 ]\n" +
                "48xoe age=60 job=null[ V.age@~9=60 ]\n";
        Assert.assertEquals(expected, scanAll(t3, "48a", "48z", null, false));
    }

    @Test
    public void writeManyDeleteOneScanWithIncludeSIColumn() throws IOException {
        TransactionId t1 = transactor.beginTransaction();
        insertAge(t1, "110joe", 20);
        insertAge(t1, "110toe", 30);
        insertAge(t1, "110boe", 40);
        insertAge(t1, "110moe", 50);
        insertAge(t1, "110xoe", 60);
        transactor.commit(t1);

        TransactionId t2 = transactor.beginTransaction();
        deleteRow(t2, "110moe");
        transactor.commit(t2);

        TransactionId t3 = transactor.beginTransaction();
        String expected = "110boe age=40 job=null[ S.TX@~9 V.age@~9=40 ]\n" +
                "110joe age=20 job=null[ S.TX@~9 V.age@~9=20 ]\n" +
                "110toe age=30 job=null[ S.TX@~9 V.age@~9=30 ]\n" +
                "110xoe age=60 job=null[ S.TX@~9 V.age@~9=60 ]\n";
        Assert.assertEquals(expected, scanAll(t3, "110a", "110z", null, true));
    }

    @Test
    public void writeDeleteScanWithIncludeSIColumnAfterRollForward() throws IOException, InterruptedException {
        try {
            Tracer.rollForwardDelayOverride = 100;
            final CountDownLatch latch = makeLatch("140moe");

            TransactionId t1 = transactor.beginTransaction();
            insertAge(t1, "140moe", 50);
            deleteRow(t1, "140moe");
            transactor.commit(t1);

            TransactionId t2 = transactor.beginTransaction();
            String expected = "";
            Assert.assertTrue(latch.await(11, TimeUnit.SECONDS));
            Assert.assertEquals(expected, scanAll(t2, "140a", "140z", null, true));
        } finally {
            Tracer.rollForwardDelayOverride = null;
        }
    }

    @Test
    public void writeManyDeleteOneScanWithIncludeSIColumnSameTransaction() throws IOException {
        TransactionId t1 = transactor.beginTransaction();
        insertAge(t1, "143joe", 20);
        insertAge(t1, "143toe", 30);
        insertAge(t1, "143boe", 40);
        insertAge(t1, "143moe", 50);
        insertAge(t1, "143xoe", 60);
        transactor.commit(t1);

        TransactionId t2 = transactor.beginTransaction();
        deleteRow(t2, "143moe");
        String expected = "143boe age=40 job=null[ S.TX@~9 V.age@~9=40 ]\n" +
                "143joe age=20 job=null[ S.TX@~9 V.age@~9=20 ]\n" +
                "143toe age=30 job=null[ S.TX@~9 V.age@~9=30 ]\n" +
                "143xoe age=60 job=null[ S.TX@~9 V.age@~9=60 ]\n";
        Assert.assertEquals(expected, scanAll(t2, "143a", "143z", null, true));
    }

    @Test
    public void writeManyDeleteOneSameTransactionScanWithIncludeSIColumn() throws IOException {
        TransactionId t1 = transactor.beginTransaction();
        insertAge(t1, "135joe", 20);
        insertAge(t1, "135toe", 30);
        insertAge(t1, "135boe", 40);
        insertAge(t1, "135xoe", 60);
        transactor.commit(t1);

        TransactionId t2 = transactor.beginTransaction();
        insertAge(t1, "135moe", 50);
        deleteRow(t2, "135moe");
        transactor.commit(t2);

        TransactionId t3 = transactor.beginTransaction();
        String expected = "135boe age=40 job=null[ S.TX@~9 V.age@~9=40 ]\n" +
                "135joe age=20 job=null[ S.TX@~9 V.age@~9=20 ]\n" +
                "135toe age=30 job=null[ S.TX@~9 V.age@~9=30 ]\n" +
                "135xoe age=60 job=null[ S.TX@~9 V.age@~9=60 ]\n";
        Assert.assertEquals(expected, scanAll(t3, "135a", "135z", null, true));
    }

    @Test
    public void writeManyDeleteOneAllNullsScanWithIncludeSIColumn() throws IOException {
        TransactionId t1 = transactor.beginTransaction();
        insertAge(t1, "137joe", 20);
        insertAge(t1, "137toe", 30);
        insertAge(t1, "137boe", 40);
        insertAge(t1, "137moe", null);
        insertAge(t1, "137xoe", 60);
        transactor.commit(t1);

        TransactionId t2 = transactor.beginTransaction();
        deleteRow(t2, "137moe");
        transactor.commit(t2);

        TransactionId t3 = transactor.beginTransaction();
        String expected = "137boe age=40 job=null[ S.TX@~9 V.age@~9=40 ]\n" +
                "137joe age=20 job=null[ S.TX@~9 V.age@~9=20 ]\n" +
                "137toe age=30 job=null[ S.TX@~9 V.age@~9=30 ]\n" +
                "137xoe age=60 job=null[ S.TX@~9 V.age@~9=60 ]\n";
        Assert.assertEquals(expected, scanAll(t3, "137a", "137z", null, true));
    }

    @Test
    public void writeManyDeleteOneAllNullsSameTransactionScanWithIncludeSIColumn() throws IOException {
        TransactionId t1 = transactor.beginTransaction();
        insertAge(t1, "138joe", 20);
        insertAge(t1, "138toe", 30);
        insertAge(t1, "138boe", 40);
        insertAge(t1, "138xoe", 60);
        transactor.commit(t1);

        TransactionId t2 = transactor.beginTransaction();
        insertAge(t2, "138moe", null);
        deleteRow(t2, "138moe");
        transactor.commit(t2);

        TransactionId t3 = transactor.beginTransaction();
        String expected = "138boe age=40 job=null[ S.TX@~9 V.age@~9=40 ]\n" +
                "138joe age=20 job=null[ S.TX@~9 V.age@~9=20 ]\n" +
                "138toe age=30 job=null[ S.TX@~9 V.age@~9=30 ]\n" +
                "138xoe age=60 job=null[ S.TX@~9 V.age@~9=60 ]\n";
        Assert.assertEquals(expected, scanAll(t3, "138a", "138z", null, true));
    }

    @Test
    public void writeManyDeleteOneBeforeWriteSameTransactionAsWriteScanWithIncludeSIColumn() throws IOException {
        TransactionId t1 = transactor.beginTransaction();
        insertAge(t1, "136joe", 20);
        insertAge(t1, "136toe", 30);
        insertAge(t1, "136boe", 40);
        insertAge(t1, "136moe", 50);
        insertAge(t1, "136xoe", 60);
        transactor.commit(t1);

        TransactionId t2 = transactor.beginTransaction();
        deleteRow(t2, "136moe");
        insertAge(t2, "136moe", 51);
        transactor.commit(t2);

        TransactionId t3 = transactor.beginTransaction();
        String expected = "136boe age=40 job=null[ S.TX@~9 V.age@~9=40 ]\n" +
                "136joe age=20 job=null[ S.TX@~9 V.age@~9=20 ]\n" +
                "136moe age=51 job=null[ S.TX@~9 V.age@~9=51 ]\n" +
                "136toe age=30 job=null[ S.TX@~9 V.age@~9=30 ]\n" +
                "136xoe age=60 job=null[ S.TX@~9 V.age@~9=60 ]\n";
        Assert.assertEquals(expected, scanAll(t3, "136a", "136z", null, true));
    }

    @Test
    public void writeManyDeleteOneBeforeWriteAllNullsSameTransactionAsWriteScanWithIncludeSIColumn() throws IOException {
        TransactionId t1 = transactor.beginTransaction();
        insertAge(t1, "139joe", 20);
        insertAge(t1, "139toe", 30);
        insertAge(t1, "139boe", 40);
        insertAge(t1, "139moe", 50);
        insertAge(t1, "139xoe", 60);
        transactor.commit(t1);

        TransactionId t2 = transactor.beginTransaction();
        deleteRow(t2, "139moe");
        insertAge(t2, "139moe", null);
        transactor.commit(t2);

        TransactionId t3 = transactor.beginTransaction();
        String expected = "139boe age=40 job=null[ S.TX@~9 V.age@~9=40 ]\n" +
                "139joe age=20 job=null[ S.TX@~9 V.age@~9=20 ]\n" +
                "139moe age=-1 job=null[ S.TX@~9 V.age@~9=-1 ]\n" +
                "139toe age=30 job=null[ S.TX@~9 V.age@~9=30 ]\n" +
                "139xoe age=60 job=null[ S.TX@~9 V.age@~9=60 ]\n";

        final String actual = scanAll(t3, "139a", "139z", null, true);
        if (usePacked) {
            expected = "139boe age=40 job=null[ S.TX@~9 V.age@~9=40 ]\n" +
                    "139joe age=20 job=null[ S.TX@~9 V.age@~9=20 ]\n" +
                    "139moe age=null job=null[ S.TX@~9 ]\n" +
                    "139toe age=30 job=null[ S.TX@~9 V.age@~9=30 ]\n" +
                    "139xoe age=60 job=null[ S.TX@~9 V.age@~9=60 ]\n";
        }
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void writeManyWithOneAllNullsDeleteOneScan() throws IOException {
        TransactionId t1 = transactor.beginTransaction();
        insertAge(t1, "112joe", 20);
        insertAge(t1, "112toe", 30);
        insertAge(t1, "112boe", null);
        insertAge(t1, "112moe", 50);
        insertAge(t1, "112xoe", 60);
        transactor.commit(t1);

        TransactionId t2 = transactor.beginTransaction();
        deleteRow(t2, "112moe");
        transactor.commit(t2);

        TransactionId t3 = transactor.beginTransaction();
        String expected = "112joe age=20 job=null[ V.age@~9=20 ]\n" +
                "112toe age=30 job=null[ V.age@~9=30 ]\n" +
                "112xoe age=60 job=null[ V.age@~9=60 ]\n";
        Assert.assertEquals(expected, scanAll(t3, "112a", "112z", null, false));
    }

    @Test
    public void writeManyWithOneAllNullsDeleteOneScanWithIncludeSIColumn() throws IOException {
        TransactionId t1 = transactor.beginTransaction();
        insertAge(t1, "111joe", 20);
        insertAge(t1, "111toe", 30);
        insertAge(t1, "111boe", null);
        insertAge(t1, "111moe", 50);
        insertAge(t1, "111xoe", 60);
        transactor.commit(t1);

        TransactionId t2 = transactor.beginTransaction();
        deleteRow(t2, "111moe");
        transactor.commit(t2);

        TransactionId t3 = transactor.beginTransaction();
        String expected = "111boe age=null job=null[ S.TX@~9 ]\n" +
                "111joe age=20 job=null[ S.TX@~9 V.age@~9=20 ]\n" +
                "111toe age=30 job=null[ S.TX@~9 V.age@~9=30 ]\n" +
                "111xoe age=60 job=null[ S.TX@~9 V.age@~9=60 ]\n";
        Assert.assertEquals(expected, scanAll(t3, "111a", "111z", null, true));
    }

    @Test
    public void writeDeleteSameTransaction() throws IOException {
        TransactionId t0 = transactor.beginTransaction();
        insertAge(t0, "joe81", 19);
        transactor.commit(t0);

        TransactionId t1 = transactor.beginTransaction();
        insertAge(t1, "joe81", 20);
        deleteRow(t1, "joe81");
        Assert.assertEquals("joe81 absent", read(t1, "joe81"));
        transactor.commit(t1);

        TransactionId t2 = transactor.beginTransaction();
        Assert.assertEquals("joe81 absent", read(t2, "joe81"));
        transactor.commit(t2);
    }

    @Test
    public void deleteWriteSameTransaction() throws IOException {
        TransactionId t0 = transactor.beginTransaction();
        insertAge(t0, "joe82", 19);
        transactor.commit(t0);

        TransactionId t1 = transactor.beginTransaction();
        deleteRow(t1, "joe82");
        insertAge(t1, "joe82", 20);
        Assert.assertEquals("joe82 age=20 job=null", read(t1, "joe82"));
        transactor.commit(t1);

        TransactionId t2 = transactor.beginTransaction();
        Assert.assertEquals("joe82 age=20 job=null", read(t2, "joe82"));
        transactor.commit(t2);
    }

    @Test
    public void fourTransactions() throws Exception {
        TransactionId t1 = transactor.beginTransaction();
        insertAge(t1, "joe7", 20);
        transactor.commit(t1);

        TransactionId t2 = transactor.beginTransaction();
        Assert.assertEquals("joe7 age=20 job=null", read(t2, "joe7"));
        insertAge(t2, "joe7", 30);
        Assert.assertEquals("joe7 age=30 job=null", read(t2, "joe7"));

        TransactionId t3 = transactor.beginTransaction();
        Assert.assertEquals("joe7 age=20 job=null", read(t3, "joe7"));

        transactor.commit(t2);

        TransactionId t4 = transactor.beginTransaction();
        Assert.assertEquals("joe7 age=30 job=null", read(t4, "joe7"));
    }

    @Test
    public void writeReadOnly() throws IOException {
        TransactionId t1 = transactor.beginTransaction();
        insertAge(t1, "joe18", 20);
        transactor.commit(t1);

        TransactionId t2 = transactor.beginTransaction(false);
        Assert.assertEquals("joe18 age=20 job=null", read(t2, "joe18"));
        try {
            insertAge(t2, "joe18", 21);
            Assert.fail("expected exception performing a write on a read-only transaction");
        } catch (IOException e) {
        }
    }

    @Test
    public void writeReadCommitted() throws IOException {
        TransactionId t1 = transactor.beginTransaction();
        insertAge(t1, "joe19", 20);
        transactor.commit(t1);

        TransactionId t2 = transactor.beginTransaction(false, false, true);
        Assert.assertEquals("joe19 age=20 job=null", read(t2, "joe19"));
    }

    @Test
    public void writeReadCommittedOverlap() throws IOException {
        TransactionId t1 = transactor.beginTransaction();
        insertAge(t1, "joe20", 20);

        TransactionId t2 = transactor.beginTransaction(false, false, true);

        Assert.assertEquals("joe20 absent", read(t2, "joe20"));
        transactor.commit(t1);
        Assert.assertEquals("joe20 age=20 job=null", read(t2, "joe20"));
    }

    @Test
    public void writeReadDirty() throws IOException {
        TransactionId t1 = transactor.beginTransaction();
        insertAge(t1, "joe22", 20);
        transactor.commit(t1);

        TransactionId t2 = transactor.beginTransaction(false, true, true);
        Assert.assertEquals("joe22 age=20 job=null", read(t2, "joe22"));
    }

    @Test
    public void writeReadDirtyOverlap() throws IOException {
        TransactionId t1 = transactor.beginTransaction();
        insertAge(t1, "joe21", 20);

        TransactionId t2 = transactor.beginTransaction(false, true, true);

        Assert.assertEquals("joe21 age=20 job=null", read(t2, "joe21"));
        transactor.commit(t1);
        Assert.assertEquals("joe21 age=20 job=null", read(t2, "joe21"));
    }

    @Test
    public void writeRollbackWriteReadDirtyOverlap() throws IOException {
        TransactionId t1 = transactor.beginTransaction();
        insertAge(t1, "joe23", 20);
        transactor.commit(t1);

        TransactionId t2 = transactor.beginTransaction();
        insertAge(t2, "joe23", 21);

        TransactionId t3 = transactor.beginTransaction(false, true, true);
        Assert.assertEquals("joe23 age=21 job=null", read(t3, "joe23"));

        transactor.rollback(t2);
        Assert.assertEquals("joe23 age=20 job=null", read(t3, "joe23"));

        TransactionId t4 = transactor.beginTransaction();
        insertAge(t4, "joe23", 22);
        Assert.assertEquals("joe23 age=22 job=null", read(t3, "joe23"));
    }

    @Test
    public void childDependentTransactionWriteRollbackRead() throws IOException {
        TransactionId t0 = transactor.beginTransaction();
        insertAge(t0, "joe24", 19);
        transactor.commit(t0);
        TransactionId t1 = transactor.beginTransaction();
        insertAge(t1, "joe24", 20);
        TransactionId t2 = transactor.beginChildTransaction(t1, true);
        insertAge(t2, "moe24", 21);
        Assert.assertEquals("joe24 age=20 job=null", read(t1, "joe24"));
        Assert.assertEquals("moe24 absent", read(t1, "moe24"));
        transactor.rollback(t2);
        Assert.assertEquals("joe24 age=20 job=null", read(t1, "joe24"));
        Assert.assertEquals("moe24 absent", read(t1, "moe24"));
        transactor.commit(t1);

        TransactionId t3 = transactor.beginTransaction(false);
        Assert.assertEquals("joe24 age=20 job=null", read(t3, "joe24"));
        Assert.assertEquals("moe24 absent", read(t3, "moe24"));
    }

    @Test
    public void childDependentTransactionWriteCommitRollbackRead() throws IOException {
        TransactionId t1 = transactor.beginTransaction();
        TransactionId t2 = transactor.beginChildTransaction(t1, true);
        insertAge(t2, "joe51", 21);
        transactor.commit(t2);
        transactor.rollback(t2);
        Assert.assertEquals("joe51 age=21 job=null", read(t1, "joe51"));
        transactor.commit(t1);
    }

    @Test
    public void childDependentSeesParentWrites() throws IOException {
        TransactionId t1 = transactor.beginTransaction();
        insertAge(t1, "joe40", 20);
        TransactionId t2 = transactor.beginChildTransaction(t1, true);
        Assert.assertEquals("joe40 age=20 job=null", read(t2, "joe40"));
    }

    @Test
    public void childDependentTransactionWriteRead() throws IOException {
        TransactionId t1 = transactor.beginTransaction();
        insertAge(t1, "joe25", 20);
        TransactionId t2 = transactor.beginChildTransaction(t1, true);
        insertAge(t2, "moe25", 21);
        Assert.assertEquals("joe25 age=20 job=null", read(t1, "joe25"));
        Assert.assertEquals("moe25 absent", read(t1, "moe25"));
        transactor.commit(t2);

        TransactionId t3 = transactor.beginTransaction(false);
        Assert.assertEquals("joe25 absent", read(t3, "joe25"));
        Assert.assertEquals("moe25 absent", read(t3, "moe25"));

        Assert.assertEquals("joe25 age=20 job=null", read(t1, "joe25"));
        Assert.assertEquals("moe25 age=21 job=null", read(t1, "moe25"));
        transactor.commit(t1);

        TransactionId t4 = transactor.beginTransaction(false);
        Assert.assertEquals("joe25 age=20 job=null", read(t4, "joe25"));
        Assert.assertEquals("moe25 age=21 job=null", read(t4, "moe25"));
    }

    @Test
    public void childDependentTransactionWithOtherCommitBetweenParentAndChild() throws IOException {
        TransactionId t0 = transactor.beginTransaction();
        insertAge(t0, "joe37", 20);
        transactor.commit(t0);

        TransactionId t1 = transactor.beginTransaction();

        TransactionId otherTransaction = transactor.beginTransaction();
        insertAge(otherTransaction, "joe37", 30);
        transactor.commit(otherTransaction);

        TransactionId t2 = transactor.beginChildTransaction(t1, true);
        Assert.assertEquals("joe37 age=20 job=null", read(t2, "joe37"));
        transactor.commit(t2);
        transactor.commit(t1);
    }

    @Test
    public void multipleChildDependentTransactionWriteRead() throws IOException {
        TransactionId t1 = transactor.beginTransaction();
        insertAge(t1, "joe26", 20);
        TransactionId t2 = transactor.beginChildTransaction(t1, true);
        TransactionId t3 = transactor.beginChildTransaction(t1, true);
        insertAge(t2, "moe26", 21);
        insertJob(t3, "boe26", "baker");
        Assert.assertEquals("joe26 age=20 job=null", read(t1, "joe26"));
        Assert.assertEquals("moe26 absent", read(t1, "moe26"));
        Assert.assertEquals("boe26 absent", read(t1, "boe26"));
        transactor.commit(t2);

        TransactionId t4 = transactor.beginTransaction(false);
        Assert.assertEquals("joe26 absent", read(t4, "joe26"));
        Assert.assertEquals("moe26 absent", read(t4, "moe26"));
        Assert.assertEquals("boe26 absent", read(t4, "boe26"));

        Assert.assertEquals("joe26 age=20 job=null", read(t1, "joe26"));
        Assert.assertEquals("moe26 age=21 job=null", read(t1, "moe26"));
        Assert.assertEquals("boe26 absent", read(t1, "boe26"));
        transactor.commit(t3);

        TransactionId t5 = transactor.beginTransaction(false);
        Assert.assertEquals("joe26 absent", read(t5, "joe26"));
        Assert.assertEquals("moe26 absent", read(t5, "moe26"));
        Assert.assertEquals("boe26 absent", read(t5, "boe26"));

        Assert.assertEquals("joe26 age=20 job=null", read(t1, "joe26"));
        Assert.assertEquals("moe26 age=21 job=null", read(t1, "moe26"));
        Assert.assertEquals("boe26 age=null job=baker", read(t1, "boe26"));
        transactor.commit(t1);

        TransactionId t6 = transactor.beginTransaction(false);
        Assert.assertEquals("joe26 age=20 job=null", read(t6, "joe26"));
        Assert.assertEquals("moe26 age=21 job=null", read(t6, "moe26"));
        Assert.assertEquals("boe26 age=null job=baker", read(t6, "boe26"));
    }

    @Test
    public void multipleChildDependentTransactionsRollbackThenWrite() throws IOException {
        TransactionId t1 = transactor.beginTransaction();
        insertAge(t1, "joe45", 20);
        insertAge(t1, "boe45", 19);
        TransactionId t2 = transactor.beginChildTransaction(t1, true, true);
        insertAge(t2, "joe45", 21);
        TransactionId t3 = transactor.beginChildTransaction(t1, true, true);
        insertJob(t3, "boe45", "baker");
        Assert.assertEquals("joe45 age=20 job=null", read(t1, "joe45"));
        Assert.assertEquals("joe45 age=21 job=null", read(t2, "joe45"));
        Assert.assertEquals("joe45 age=20 job=null", read(t3, "joe45"));
        transactor.rollback(t2);
        Assert.assertEquals("joe45 age=20 job=null", read(t1, "joe45"));
        Assert.assertEquals("joe45 age=20 job=null", read(t3, "joe45"));
        Assert.assertEquals("boe45 age=19 job=null", read(t1, "boe45"));
        Assert.assertEquals("boe45 age=19 job=baker", read(t3, "boe45"));
        transactor.rollback(t3);
        Assert.assertEquals("joe45 age=20 job=null", read(t1, "joe45"));
        TransactionId t4 = transactor.beginChildTransaction(t1, true, true);
        insertAge(t4, "joe45", 24);
        Assert.assertEquals("joe45 age=20 job=null", read(t1, "joe45"));
        Assert.assertEquals("joe45 age=24 job=null", read(t4, "joe45"));
        transactor.commit(t4);
        Assert.assertEquals("joe45 age=24 job=null", read(t1, "joe45"));
        Assert.assertEquals("joe45 age=24 job=null", read(t4, "joe45"));

        TransactionId t5 = transactor.beginTransaction(false, false, true);
        Assert.assertEquals("joe45 absent", read(t5, "joe45"));
        Assert.assertEquals("boe45 absent", read(t5, "boe45"));
        transactor.commit(t1);
        Assert.assertEquals("joe45 age=24 job=null", read(t5, "joe45"));
        Assert.assertEquals("boe45 age=19 job=null", read(t5, "boe45"));
    }

    @Test
    public void multipleChildCommitParentRollback() throws IOException {
        TransactionId t1 = transactor.beginTransaction();
        insertAge(t1, "joe46", 20);
        TransactionId t2 = transactor.beginChildTransaction(t1, true);
        insertJob(t2, "moe46", "baker");
        transactor.commit(t2);
        transactor.rollback(t1);
        // @TODO: it should not be possible to start a child on a rolled back transaction
        TransactionId t3 = transactor.beginChildTransaction(t1, true);
        Assert.assertEquals("joe46 absent", read(t3, "joe46"));
        Assert.assertEquals("moe46 age=null job=baker", read(t3, "moe46"));
    }

    @Test
    public void childDependentTransactionWriteRollbackParentRead() throws IOException {
        TransactionId t1 = transactor.beginTransaction();
        insertAge(t1, "joe27", 20);
        TransactionId t2 = transactor.beginChildTransaction(t1, true);
        insertAge(t2, "moe27", 21);
        transactor.commit(t2);
        transactor.rollback(t1);

        TransactionId t4 = transactor.beginTransaction(false);
        Assert.assertEquals("joe27 absent", read(t4, "joe27"));
        Assert.assertEquals("moe27 absent", read(t4, "moe27"));
    }

    @Test
    public void commitParentOfCommittedDependent() throws IOException {
        TransactionId t1 = transactor.beginTransaction();
        insertAge(t1, "joe32", 20);
        TransactionId t2 = transactor.beginChildTransaction(t1, true);
        insertAge(t2, "moe32", 21);
        transactor.commit(t2);
        final Transaction transactionStatusA = transactorSetup.transactionStore.getTransaction(t2);
        Assert.assertEquals("committing a dependent child sets a local commit timestamp", 2L, (long) transactionStatusA.getCommitTimestampDirect());
        Assert.assertNull(transactionStatusA.getEffectiveCommitTimestamp());
        transactor.commit(t1);
        final Transaction transactionStatusB = transactorSetup.transactionStore.getTransaction(t2);
        Assert.assertEquals("committing parent of dependent transaction should not change the commit time of the child", 2L, (long) transactionStatusB.getCommitTimestampDirect());
        Assert.assertNotNull(transactionStatusB.getEffectiveCommitTimestamp());
    }

    @Test
    public void dependentWriteFollowedByReadCommittedWriter() throws IOException {
        TransactionId parent = transactor.beginTransaction();

        TransactionId child = transactor.beginChildTransaction(parent, true);
        insertAge(child, "joe34", 22);
        transactor.commit(child);

        TransactionId other = transactor.beginTransaction(true, false, true);
        try {
            insertAge(other, "joe34", 21);
            Assert.fail();
        } catch (WriteConflict e) {
        } catch (RetriesExhaustedWithDetailsException e) {
            assertWriteConflict(e);
        } finally {
            transactor.fail(other);
        }
    }

    @Test
    public void dependentWriteCommitParentFollowedByReadCommittedWriter() throws IOException {
        TransactionId parent = transactor.beginTransaction();

        TransactionId child = transactor.beginChildTransaction(parent, true);
        insertAge(child, "joe94", 22);
        transactor.commit(child);
        transactor.commit(parent);

        TransactionId other = transactor.beginTransaction(true, false, true);
        insertAge(other, "joe94", 21);
        transactor.commit(other);
    }

    @Test
    public void dependentWriteOverlapWithReadCommittedWriter() throws IOException {
        TransactionId parent = transactor.beginTransaction();

        TransactionId other = transactor.beginTransaction(true, false, true);

        TransactionId child = transactor.beginChildTransaction(parent, true);
        insertAge(child, "joe36", 22);
        transactor.commit(child);

        try {
            insertAge(other, "joe36", 21);
            Assert.fail();
        } catch (WriteConflict e) {
        } catch (RetriesExhaustedWithDetailsException e) {
            assertWriteConflict(e);
        } finally {
            transactor.fail(other);
        }
    }

    @Test
    public void rollbackUpdate() throws IOException {
        TransactionId t1 = transactor.beginTransaction();
        insertAge(t1, "joe43", 20);
        transactor.commit(t1);

        TransactionId t2 = transactor.beginTransaction();
        insertAge(t2, "joe43", 21);
        transactor.rollback(t2);

        TransactionId t3 = transactor.beginTransaction();
        Assert.assertEquals("joe43 age=20 job=null", read(t3, "joe43"));
    }

    @Test
    public void rollbackInsert() throws IOException {
        TransactionId t1 = transactor.beginTransaction();
        insertAge(t1, "joe44", 20);
        transactor.rollback(t1);

        TransactionId t2 = transactor.beginTransaction();
        Assert.assertEquals("joe44 absent", read(t2, "joe44"));
    }

    @Test
    public void childrenOfChildrenCommitCommitCommit() throws IOException {
        TransactionId t1 = transactor.beginTransaction();
        TransactionId t2 = transactor.beginChildTransaction(t1, true);
        TransactionId t3 = transactor.beginChildTransaction(t2, true);
        insertAge(t3, "joe53", 20);
        Assert.assertEquals("joe53 age=20 job=null", read(t3, "joe53"));
        transactor.commit(t3);
        Assert.assertEquals("joe53 age=20 job=null", read(t3, "joe53"));
        Assert.assertEquals("joe53 age=20 job=null", read(t2, "joe53"));
        insertAge(t2, "boe53", 21);
        Assert.assertEquals("boe53 age=21 job=null", read(t2, "boe53"));
        transactor.commit(t2);
        Assert.assertEquals("joe53 age=20 job=null", read(t1, "joe53"));
        Assert.assertEquals("boe53 age=21 job=null", read(t1, "boe53"));
        transactor.commit(t1);
        TransactionId t4 = transactor.beginTransaction(false);
        Assert.assertEquals("joe53 age=20 job=null", read(t4, "joe53"));
        Assert.assertEquals("boe53 age=21 job=null", read(t4, "boe53"));
    }

    @Test
    public void childrenOfChildrenCommitCommitCommitParentWriteFirst() throws IOException {
        TransactionId t1 = transactor.beginTransaction();
        TransactionId t2 = transactor.beginChildTransaction(t1, true, true);
        TransactionId t3 = transactor.beginChildTransaction(t2, false, true);
        insertAge(t1, "joe57", 18);
        insertAge(t1, "boe57", 19);
        insertAge(t2, "boe57", 21);
        insertAge(t3, "joe57", 20);
        Assert.assertEquals("joe57 age=20 job=null", read(t3, "joe57"));
        transactor.commit(t3);
        Assert.assertEquals("joe57 age=20 job=null", read(t3, "joe57"));
        Assert.assertEquals("joe57 age=20 job=null", read(t2, "joe57"));
        Assert.assertEquals("boe57 age=21 job=null", read(t2, "boe57"));
        transactor.commit(t2);
        Assert.assertEquals("joe57 age=20 job=null", read(t1, "joe57"));
        Assert.assertEquals("boe57 age=21 job=null", read(t1, "boe57"));
        transactor.commit(t1);
        TransactionId t4 = transactor.beginTransaction(false);
        Assert.assertEquals("joe57 age=20 job=null", read(t4, "joe57"));
        Assert.assertEquals("boe57 age=21 job=null", read(t4, "boe57"));
    }

    @Test
    public void childrenOfChildrenCommitCommitRollback() throws IOException {
        TransactionId t1 = transactor.beginTransaction();
        TransactionId t2 = transactor.beginChildTransaction(t1, true);
        TransactionId t3 = transactor.beginChildTransaction(t2, true);
        insertAge(t3, "joe54", 20);
        Assert.assertEquals("joe54 age=20 job=null", read(t3, "joe54"));
        transactor.rollback(t3);
        Assert.assertEquals("joe54 absent", read(t2, "joe54"));
        insertAge(t2, "boe54", 21);
        Assert.assertEquals("boe54 age=21 job=null", read(t2, "boe54"));
        transactor.commit(t2);
        Assert.assertEquals("joe54 absent", read(t1, "joe54"));
        Assert.assertEquals("boe54 age=21 job=null", read(t1, "boe54"));
        transactor.commit(t1);
        TransactionId t4 = transactor.beginTransaction(false);
        Assert.assertEquals("joe54 absent", read(t4, "joe54"));
        Assert.assertEquals("boe54 age=21 job=null", read(t4, "boe54"));
    }

    @Test
    public void childrenOfChildrenWritesDoNotConflict() throws IOException {
        TransactionId t1 = transactor.beginTransaction();
        TransactionId t2 = transactor.beginChildTransaction(t1, true);
        TransactionId t3 = transactor.beginChildTransaction(t2, true);
        insertAge(t1, "joe95", 18);
        insertAge(t3, "joe95", 20);
        Assert.assertEquals("joe95 age=18 job=null", read(t1, "joe95"));
        Assert.assertEquals("joe95 age=18 job=null", read(t2, "joe95"));
        Assert.assertEquals("joe95 age=20 job=null", read(t3, "joe95"));
    }

    @Test
    public void parentWritesDoNotConflictWithPriorChildWrites() throws IOException {
        TransactionId t1 = transactor.beginTransaction();
        TransactionId t2 = transactor.beginChildTransaction(t1, true);
        insertAge(t2, "joe101", 20);
        transactor.commit(t2);
        insertAge(t1, "joe101", 21);
        Assert.assertEquals("joe101 age=21 job=null", read(t1, "joe101"));
    }

    @Test
    public void parentWritesDoNotConflictWithPriorChildDelete() throws IOException {
        TransactionId t1 = transactor.beginTransaction();
        TransactionId t2 = transactor.beginChildTransaction(t1, true);
        deleteRow(t2, "joe105");
        transactor.commit(t2);
        insertAge(t1, "joe105", 21);
        Assert.assertEquals("joe105 age=21 job=null", read(t1, "joe105"));
    }

    @Test
    public void parentWritesDoNotConflictWithPriorChildDelete2() throws IOException {
        TransactionId t0 = transactor.beginTransaction();
        insertAge(t0, "joe141", 20);
        transactor.commit(t0);
        TransactionId t1 = transactor.beginTransaction();
        TransactionId t2 = transactor.beginChildTransaction(t1, true);
        deleteRow(t2, "joe141");
        transactor.commit(t2);
        insertAge(t1, "joe141", 21);
        Assert.assertEquals("joe141 age=21 job=null", read(t1, "joe141"));
    }

    @Test
    public void parentDeleteDoesNotConflictWithPriorChildDelete() throws IOException {
        TransactionId t1 = transactor.beginTransaction();
        TransactionId t2 = transactor.beginChildTransaction(t1, true);
        deleteRow(t2, "joe109");
        transactor.commit(t2);
        deleteRow(t1, "joe109");
        Assert.assertEquals("joe109 absent", read(t1, "joe109"));
        insertAge(t1, "joe109", 21);
        Assert.assertEquals("joe109 age=21 job=null", read(t1, "joe109"));
    }

    @Test
    public void parentWritesDoNotConflictWithPriorActiveChildWrites() throws IOException {
        TransactionId t1 = transactor.beginTransaction();
        TransactionId t2 = transactor.beginChildTransaction(t1, true);
        insertAge(t2, "joe102", 20);
        insertAge(t1, "joe102", 21);
        Assert.assertEquals("joe102 age=21 job=null", read(t1, "joe102"));
        transactor.commit(t2);
        Assert.assertEquals("joe102 age=21 job=null", read(t1, "joe102"));
    }

    @Test
    public void parentWritesDoNotConflictWithPriorActiveChildDelete() throws IOException {
        TransactionId t1 = transactor.beginTransaction();
        TransactionId t2 = transactor.beginChildTransaction(t1, true);
        deleteRow(t2, "joe106");
        insertAge(t1, "joe106", 21);
        Assert.assertEquals("joe106 age=21 job=null", read(t1, "joe106"));
        transactor.commit(t2);
        Assert.assertEquals("joe106 age=21 job=null", read(t1, "joe106"));
    }

    @Test
    public void parentWritesDoNotConflictWithPriorIndependentChildWrites() throws IOException {
        TransactionId t1 = transactor.beginTransaction();
        TransactionId t2 = transactor.beginChildTransaction(t1, false, true);
        insertAge(t2, "joe103", 20);
        transactor.commit(t2);
        insertAge(t1, "joe103", 21);
        Assert.assertEquals("joe103 age=21 job=null", read(t1, "joe103"));
    }

    @Test
    public void parentWritesDoNotConflictWithPriorIndependentChildDelete() throws IOException {
        TransactionId t1 = transactor.beginTransaction();
        TransactionId t2 = transactor.beginChildTransaction(t1, false, true);
        deleteRow(t2, "joe107");
        transactor.commit(t2);
        insertAge(t1, "joe107", 21);
        Assert.assertEquals("joe107 age=21 job=null", read(t1, "joe107"));
    }

    @Test
    public void parentWritesDoNotConflictWithPriorActiveIndependentChildWrites() throws IOException {
        TransactionId t1 = transactor.beginTransaction();
        TransactionId t2 = transactor.beginChildTransaction(t1, false, true);
        insertAge(t2, "joe104", 20);
        insertAge(t1, "joe104", 21);
        Assert.assertEquals("joe104 age=21 job=null", read(t1, "joe104"));
        transactor.commit(t2);
        Assert.assertEquals("joe104 age=21 job=null", read(t1, "joe104"));
    }

    @Test
    public void parentWritesDoNotConflictWithPriorActiveIndependentChildDelete() throws IOException {
        TransactionId t1 = transactor.beginTransaction();
        TransactionId t2 = transactor.beginChildTransaction(t1, false, true);
        deleteRow(t2, "joe108");
        insertAge(t1, "joe108", 21);
        Assert.assertEquals("joe108 age=21 job=null", read(t1, "joe108"));
        transactor.commit(t2);
        Assert.assertEquals("joe108 age=21 job=null", read(t1, "joe108"));
    }

    @Test
    public void childrenOfChildrenCommitCommitRollbackParentWriteFirst() throws IOException {
        TransactionId t1 = transactor.beginTransaction();
        TransactionId t2 = transactor.beginChildTransaction(t1, true, true);
        TransactionId t3 = transactor.beginChildTransaction(t2, false, true);
        insertAge(t1, "joe58", 18);
        insertAge(t1, "boe58", 19);
        insertAge(t2, "boe58", 21);
        insertAge(t3, "joe58", 20);
        Assert.assertEquals("joe58 age=20 job=null", read(t3, "joe58"));
        transactor.rollback(t3);
        Assert.assertEquals("joe58 age=18 job=null", read(t2, "joe58"));
        Assert.assertEquals("boe58 age=21 job=null", read(t2, "boe58"));
        transactor.commit(t2);
        Assert.assertEquals("joe58 age=18 job=null", read(t1, "joe58"));
        Assert.assertEquals("boe58 age=21 job=null", read(t1, "boe58"));
        transactor.commit(t1);
        TransactionId t4 = transactor.beginTransaction(false);
        Assert.assertEquals("joe58 age=18 job=null", read(t4, "joe58"));
        Assert.assertEquals("boe58 age=21 job=null", read(t4, "boe58"));
    }

    @Test
    public void childrenOfChildrenCommitRollbackCommit() throws IOException {
        TransactionId t1 = transactor.beginTransaction();
        TransactionId t2 = transactor.beginChildTransaction(t1, true);
        TransactionId t3 = transactor.beginChildTransaction(t2, true);
        insertAge(t3, "joe55", 20);
        Assert.assertEquals("joe55 age=20 job=null", read(t3, "joe55"));
        transactor.commit(t3);
        Assert.assertEquals("joe55 age=20 job=null", read(t3, "joe55"));
        Assert.assertEquals("joe55 age=20 job=null", read(t2, "joe55"));
        insertAge(t2, "boe55", 21);
        Assert.assertEquals("boe55 age=21 job=null", read(t2, "boe55"));
        transactor.rollback(t2);
        Assert.assertEquals("joe55 absent", read(t1, "joe55"));
        Assert.assertEquals("boe55 absent", read(t1, "boe55"));
        transactor.commit(t1);
        TransactionId t4 = transactor.beginTransaction(false);
        Assert.assertEquals("joe55 absent", read(t4, "joe55"));
        Assert.assertEquals("boe55 absent", read(t4, "boe55"));
    }

    @Test
    public void childrenOfChildrenCommitRollbackCommitParentWriteFirst() throws IOException {
        TransactionId t1 = transactor.beginTransaction();
        TransactionId t2 = transactor.beginChildTransaction(t1, true);
        TransactionId t3 = transactor.beginChildTransaction(t2, true);
        insertAge(t1, "joe59", 18);
        insertAge(t1, "boe59", 19);
        insertAge(t2, "boe59", 21);
        insertAge(t3, "joe59", 20);
        Assert.assertEquals("joe59 age=20 job=null", read(t3, "joe59"));
        transactor.commit(t3);
        Assert.assertEquals("joe59 age=20 job=null", read(t2, "joe59"));
        Assert.assertEquals("boe59 age=21 job=null", read(t2, "boe59"));
        transactor.rollback(t2);
        Assert.assertEquals("joe59 age=18 job=null", read(t1, "joe59"));
        Assert.assertEquals("boe59 age=19 job=null", read(t1, "boe59"));
        transactor.commit(t1);
        TransactionId t4 = transactor.beginTransaction(false);
        Assert.assertEquals("joe59 age=18 job=null", read(t4, "joe59"));
        Assert.assertEquals("boe59 age=19 job=null", read(t4, "boe59"));
    }

    @Test
    public void childrenOfChildrenRollbackCommitCommitParentWriteFirst() throws IOException {
        TransactionId t1 = transactor.beginTransaction();
        TransactionId t2 = transactor.beginChildTransaction(t1, true);
        TransactionId t3 = transactor.beginChildTransaction(t2, true);
        insertAge(t1, "joe60", 18);
        insertAge(t1, "boe60", 19);
        insertAge(t2, "doe60", 21);
        insertAge(t3, "moe60", 30);
        Assert.assertEquals("moe60 age=30 job=null", read(t3, "moe60"));
        transactor.commit(t3);
        Assert.assertEquals("moe60 age=30 job=null", read(t3, "moe60"));
        Assert.assertEquals("doe60 age=21 job=null", read(t2, "doe60"));
        insertAge(t2, "doe60", 22);
        Assert.assertEquals("doe60 age=22 job=null", read(t2, "doe60"));
        transactor.commit(t2);
        Assert.assertEquals("joe60 age=18 job=null", read(t1, "joe60"));
        Assert.assertEquals("boe60 age=19 job=null", read(t1, "boe60"));
        Assert.assertEquals("moe60 age=30 job=null", read(t1, "moe60"));
        Assert.assertEquals("doe60 age=22 job=null", read(t1, "doe60"));
        transactor.rollback(t1);
        TransactionId t4 = transactor.beginTransaction(false);
        Assert.assertEquals("joe60 absent", read(t4, "joe60"));
        Assert.assertEquals("boe60 absent", read(t4, "boe60"));
    }

    @Test
    public void childrenOfChildrenRollbackCommitCommit() throws IOException {
        TransactionId t1 = transactor.beginTransaction();
        TransactionId t2 = transactor.beginChildTransaction(t1, true);
        TransactionId t3 = transactor.beginChildTransaction(t2, true);
        insertAge(t3, "joe56", 20);
        Assert.assertEquals("joe56 age=20 job=null", read(t3, "joe56"));
        transactor.commit(t3);
        Assert.assertEquals("joe56 age=20 job=null", read(t3, "joe56"));
        Assert.assertEquals("joe56 age=20 job=null", read(t2, "joe56"));
        insertAge(t2, "boe56", 21);
        Assert.assertEquals("boe56 age=21 job=null", read(t2, "boe56"));
        transactor.commit(t2);
        Assert.assertEquals("joe56 age=20 job=null", read(t1, "joe56"));
        Assert.assertEquals("boe56 age=21 job=null", read(t1, "boe56"));
        transactor.rollback(t1);
        TransactionId t4 = transactor.beginTransaction(false);
        Assert.assertEquals("joe56 absent", read(t4, "joe56"));
        Assert.assertEquals("boe56 absent", read(t4, "boe56"));
    }

    @Ignore
    @Test
    public void readWriteMechanics() throws Exception {
        final SDataLib dataLib = storeSetup.getDataLib();
        final STableReader reader = storeSetup.getReader();

        final Object testKey = dataLib.newRowKey(new Object[]{"jim"});
        Object put = dataLib.newPut(testKey);
        Object family = dataLib.encode(DEFAULT_FAMILY);
        Object ageQualifier = dataLib.encode("age");
        dataLib.addKeyValueToPut(put, family, ageQualifier, null, dataLib.encode(25));
        TransactionId t = transactor.beginTransaction();
        transactorSetup.clientTransactor.initializePut(t.getTransactionIdString(), put);
        Object put2 = dataLib.newPut(testKey);
        dataLib.addKeyValueToPut(put2, family, ageQualifier, null, dataLib.encode(27));
        transactorSetup.clientTransactor.initializePut(
                transactorSetup.clientTransactor.transactionIdFromPut(put).getTransactionIdString(),
                put2);
        Assert.assertTrue(dataLib.valuesEqual(dataLib.encode((short) 0), dataLib.getAttribute(put2, SIConstants.SI_NEEDED)));
        Object testSTable = reader.open(storeSetup.getPersonTableName());
        try {
            Assert.assertTrue(transactor.processPut(testSTable, transactorSetup.rollForwardQueue, put));
            Assert.assertTrue(transactor.processPut(testSTable, transactorSetup.rollForwardQueue, put2));
            Object get1 = makeGet(dataLib, testKey);
            transactorSetup.clientTransactor.initializeGet(t.getTransactionIdString(), get1);
            Object result = reader.get(testSTable, get1);
            if (useSimple) {
                result = transactor.filterResult(transactor.newFilterState(transactorSetup.rollForwardQueue, t, false), result);
            }
            final int ageRead = (Integer) dataLib.decode(dataLib.getResultValue(result, family, ageQualifier), Integer.class);
            Assert.assertEquals(27, ageRead);
        } finally {
            reader.close(testSTable);
        }

        TransactionId t2 = transactor.beginTransaction();
        Object get = makeGet(dataLib, testKey);
        testSTable = reader.open(storeSetup.getPersonTableName());
        try {
            final Object resultTuple = reader.get(testSTable, get);
            final IFilterState filterState = transactor.newFilterState(transactorSetup.rollForwardQueue, t2, false);
            if (useSimple) {
                transactor.filterResult(filterState, resultTuple);
            }
        } finally {
            reader.close(testSTable);
        }

        transactor.commit(t);
        t = transactor.beginTransaction();

        dataLib.addKeyValueToPut(put, family, ageQualifier, null, dataLib.encode(35));
        transactorSetup.clientTransactor.initializePut(t.getTransactionIdString(), put);
        testSTable = reader.open(storeSetup.getPersonTableName());
        try {
            Assert.assertTrue(transactor.processPut(testSTable, transactorSetup.rollForwardQueue, put));
        } finally {
            reader.close(testSTable);
        }
    }

    @Test
    public void asynchRollForward() throws IOException, InterruptedException {
        checkAsynchRollForward(61, "commit", false, null, false, new Function<Object[], Object>() {
            @Override
            public Object apply(@Nullable Object[] input) {
                TransactionId t = (TransactionId) input[0];
                Object cell = input[1];
                final SDataLib dataLib = storeSetup.getDataLib();
                final long timestamp = (Long) dataLib.decode(dataLib.getKeyValueValue(cell), Long.class);
                Assert.assertEquals(t.getId() + 1, timestamp);
                return null;
            }
        });
    }

    @Test
    public void asynchRollForwardRolledBackTransaction() throws IOException, InterruptedException {
        checkAsynchRollForward(71, "rollback", false, null, false, new Function<Object[], Object>() {
            @Override
            public Object apply(@Nullable Object[] input) {
                TransactionId t = (TransactionId) input[0];
                Object cell = input[1];
                final SDataLib dataLib = storeSetup.getDataLib();
                final long timestamp = (Integer) dataLib.decode(dataLib.getKeyValueValue(cell), Integer.class);
                Assert.assertEquals(-2, timestamp);
                return null;
            }
        });
    }

    @Test
    public void asynchRollForwardFailedTransaction() throws IOException, InterruptedException {
        checkAsynchRollForward(71, "fail", false, null, false, new Function<Object[], Object>() {
            @Override
            public Object apply(@Nullable Object[] input) {
                TransactionId t = (TransactionId) input[0];
                Object cell = input[1];
                final SDataLib dataLib = storeSetup.getDataLib();
                final long timestamp = (Integer) dataLib.decode(dataLib.getKeyValueValue(cell), Integer.class);
                Assert.assertEquals(-2, timestamp);
                return null;
            }
        });
    }

    @Test
    public void asynchRollForwardNestedCommitFail() throws IOException, InterruptedException {
        checkAsynchRollForward(131, "commit", true, "fail", false, new Function<Object[], Object>() {
            @Override
            public Object apply(@Nullable Object[] input) {
                TransactionId t = (TransactionId) input[0];
                Object cell = input[1];
                final SDataLib dataLib = storeSetup.getDataLib();
                final long timestamp = (Integer) dataLib.decode(dataLib.getKeyValueValue(cell), Integer.class);
                Assert.assertEquals(-2, timestamp);
                return null;
            }
        });
    }

    @Test
    public void asynchRollForwardNestedFailCommitTransaction() throws IOException, InterruptedException {
        checkAsynchRollForward(132, "fail", true, "commit", false, new Function<Object[], Object>() {
            @Override
            public Object apply(@Nullable Object[] input) {
                TransactionId t = (TransactionId) input[0];
                Object cell = input[1];
                final SDataLib dataLib = storeSetup.getDataLib();
                final long timestamp = (Integer) dataLib.decode(dataLib.getKeyValueValue(cell), Integer.class);
                Assert.assertEquals(-2, timestamp);
                return null;
            }
        });
    }

    @Test
    public void asynchRollForwardNestedCommitCommit() throws IOException, InterruptedException {
        checkAsynchRollForward(133, "commit", true, "commit", false, new Function<Object[], Object>() {
            @Override
            public Object apply(@Nullable Object[] input) {
                TransactionId t = (TransactionId) input[0];
                Object cell = input[1];
                final SDataLib dataLib = storeSetup.getDataLib();
                final long timestamp = (Long) dataLib.decode(dataLib.getKeyValueValue(cell), Long.class);
                Assert.assertEquals(t.getId() + 1, timestamp);
                return null;
            }
        });
    }

    @Test
    public void asynchRollForwardNestedFailFail() throws IOException, InterruptedException {
        checkAsynchRollForward(134, "fail", true, "fail", false, new Function<Object[], Object>() {
            @Override
            public Object apply(@Nullable Object[] input) {
                TransactionId t = (TransactionId) input[0];
                Object cell = input[1];
                final SDataLib dataLib = storeSetup.getDataLib();
                final long timestamp = (Integer) dataLib.decode(dataLib.getKeyValueValue(cell), Integer.class);
                Assert.assertEquals(-2, timestamp);
                return null;
            }
        });
    }

    @Test
    public void asynchRollForwardFollowedByWriteConflict() throws IOException, InterruptedException {
        checkAsynchRollForward(83, "commit", false, null, true, new Function<Object[], Object>() {
            @Override
            public Object apply(@Nullable Object[] input) {
                TransactionId t = (TransactionId) input[0];
                Object cell = input[1];
                final SDataLib dataLib = storeSetup.getDataLib();
                final long timestamp = (Long) dataLib.decode(dataLib.getKeyValueValue(cell), Long.class);
                Assert.assertEquals(t.getId() + 2, timestamp);
                return null;  //To change body of implemented methods use File | Settings | File Templates.
            }
        });
    }

    private void checkAsynchRollForward(int testIndex, String commitRollBackOrFail, boolean nested, String parentCommitRollBackOrFail,
                                        boolean conflictingWrite, Function<Object[], Object> timestampDecoder)
            throws IOException, InterruptedException {
        try {
            Tracer.rollForwardDelayOverride = 100;
            TransactionId t0 = null;
            if (nested) {
                t0 = transactor.beginTransaction();
            }
            TransactionId t1;
            if (nested) {
                t1 = transactor.beginChildTransaction(t0, true);
            } else {
                t1 = transactor.beginTransaction();
            }
            TransactionId t1b = null;
            if (conflictingWrite) {
                t1b = transactor.beginTransaction();
            }
            final String testRow = "joe" + testIndex;
            insertAge(t1, testRow, 20);
            final CountDownLatch latch = makeLatch(testRow);
            if (commitRollBackOrFail.equals("commit")) {
                transactor.commit(t1);
            } else if (commitRollBackOrFail.equals("rollback")) {
                transactor.rollback(t1);
            } else if (commitRollBackOrFail.equals("fail")) {
                transactor.fail(t1);
            } else {
                throw new RuntimeException("unknown value");
            }
            if (nested) {
                if (parentCommitRollBackOrFail.equals("commit")) {
                    transactor.commit(t0);
                } else if (parentCommitRollBackOrFail.equals("rollback")) {
                    transactor.rollback(t0);
                } else if (parentCommitRollBackOrFail.equals("fail")) {
                    transactor.fail(t0);
                } else {
                    throw new RuntimeException("unknown value");
                }
            }
            Object result = readRaw(testRow);
            final SDataLib dataLib = storeSetup.getDataLib();
            final List commitTimestamps = dataLib.getResultColumn(result, dataLib.encode(SNAPSHOT_ISOLATION_FAMILY),
                    dataLib.encode(SNAPSHOT_ISOLATION_COMMIT_TIMESTAMP_COLUMN_STRING));
            for (Object c : commitTimestamps) {
                final int timestamp = (Integer) dataLib.decode(dataLib.getKeyValueValue(c), Integer.class);
                Assert.assertEquals(-1, timestamp);
                Assert.assertEquals(t1.getId(), dataLib.getKeyValueTimestamp(c));
            }
            Assert.assertTrue("Latch timed out", latch.await(11, TimeUnit.SECONDS));

            Object result2 = readRaw(testRow);
            final List commitTimestamps2 = dataLib.getResultColumn(result2, dataLib.encode(SNAPSHOT_ISOLATION_FAMILY),
                    dataLib.encode(SNAPSHOT_ISOLATION_COMMIT_TIMESTAMP_COLUMN_STRING));
            for (Object c2 : commitTimestamps2) {
                timestampDecoder.apply(new Object[]{t1, c2});
                Assert.assertEquals(t1.getId(), dataLib.getKeyValueTimestamp(c2));
            }
            TransactionId t2 = transactor.beginTransaction(false);
            if (commitRollBackOrFail.equals("commit") && (!nested || parentCommitRollBackOrFail.equals("commit"))) {
                Assert.assertEquals(testRow + " age=20 job=null", read(t2, testRow));
            } else {
                Assert.assertEquals(testRow + " absent", read(t2, testRow));
            }
            if (conflictingWrite) {
                try {
                    insertAge(t1b, testRow, 21);
                    Assert.fail();
                } catch (WriteConflict e) {
                } catch (RetriesExhaustedWithDetailsException e) {
                    assertWriteConflict(e);
                } finally {
                    transactor.fail(t1b);
                }
            }
        } finally {
            Tracer.rollForwardDelayOverride = null;
        }
    }

    private CountDownLatch makeLatch(final String targetKey) {
        final SDataLib dataLib = storeSetup.getDataLib();
        final CountDownLatch latch = new CountDownLatch(1);
        Tracer.registerRowRollForward(new Function<Object, Object>() {
            @Override
            public Object apply(@Nullable Object input) {
                String key = (String) dataLib.decode(input, String.class);
                if (key.equals(targetKey)) {
                    latch.countDown();
                }
                return null;
            }
        });
        return latch;
    }

    private CountDownLatch makeTransactionLatch(final TransactionId targetTransactionId) {
        final CountDownLatch latch = new CountDownLatch(1);
        Tracer.registerTransactionRollForward(new Function<Long, Object>() {
            @Override
            public Object apply(@Nullable Long input) {
                if (input.equals(targetTransactionId.getId())) {
                    latch.countDown();
                }
                return null;
            }
        });
        return latch;
    }

    @Test
    public void asynchRollForwardViaScan() throws IOException, InterruptedException {
        checkAsynchRollForwardViaScan(62, true, new Function<Object[], Object>() {
            @Override
            public Object apply(@Nullable Object[] input) {
                TransactionId t = (TransactionId) input[0];
                Object cell = input[1];
                final SDataLib dataLib = storeSetup.getDataLib();
                final long timestamp = (Long) dataLib.decode(dataLib.getKeyValueValue(cell), Long.class);
                Assert.assertEquals(t.getId() + 1, timestamp);
                return null;
            }
        });
    }

    @Test
    public void asynchRollForwardViaScanRollback() throws IOException, InterruptedException {
        checkAsynchRollForwardViaScan(72, false, new Function<Object[], Object>() {
            @Override
            public Object apply(@Nullable Object[] input) {
                TransactionId t = (TransactionId) input[0];
                Object cell = input[1];
                final SDataLib dataLib = storeSetup.getDataLib();
                final Object keyValueValue = dataLib.getKeyValueValue(cell);
                final int timestamp = (Integer) dataLib.decode(keyValueValue, Integer.class);
                Assert.assertEquals(-2, timestamp);
                return null;
            }
        });
    }

    private void checkAsynchRollForwardViaScan(int testIndex, boolean commit, Function<Object[], Object> timestampProcessor) throws IOException, InterruptedException {
        try {
            final String testRow = "joe" + testIndex;
            Tracer.rollForwardDelayOverride = 100;
            TransactionId t1 = transactor.beginTransaction();
            final CountDownLatch transactionlatch = makeTransactionLatch(t1);
            insertAge(t1, testRow, 20);
            Assert.assertTrue(transactionlatch.await(11, TimeUnit.SECONDS));
            if (commit) {
                transactor.commit(t1);
            } else {
                transactor.rollback(t1);
            }
            Object result = readRaw(testRow);
            final SDataLib dataLib = storeSetup.getDataLib();
            final List commitTimestamps = dataLib.getResultColumn(result, dataLib.encode(SNAPSHOT_ISOLATION_FAMILY),
                    dataLib.encode(SNAPSHOT_ISOLATION_COMMIT_TIMESTAMP_COLUMN_STRING));
            for (Object c : commitTimestamps) {
                final int timestamp = (Integer) dataLib.decode(dataLib.getKeyValueValue(c), Integer.class);
                Assert.assertEquals(-1, timestamp);
                Assert.assertEquals(t1.getId(), dataLib.getKeyValueTimestamp(c));
            }

            final CountDownLatch latch = makeLatch(testRow);
            TransactionId t2 = transactor.beginTransaction(false);
            if (commit) {
                Assert.assertEquals(testRow + " age=20 job=null", read(t2, testRow));
            } else {
                Assert.assertEquals(testRow + " absent", read(t2, testRow));
            }
            Assert.assertTrue(latch.await(11, TimeUnit.SECONDS));

            Object result2 = readRaw(testRow);

            final List commitTimestamps2 = dataLib.getResultColumn(result2, dataLib.encode(SNAPSHOT_ISOLATION_FAMILY),
                    dataLib.encode(SNAPSHOT_ISOLATION_COMMIT_TIMESTAMP_COLUMN_STRING));
            for (Object c2 : commitTimestamps2) {
                timestampProcessor.apply(new Object[]{t1, c2});
                Assert.assertEquals(t1.getId(), dataLib.getKeyValueTimestamp(c2));
            }
        } finally {
            Tracer.rollForwardDelayOverride = null;
        }
    }

    private Object readRaw(String name) throws IOException {
        return readAgeRawDirect(storeSetup, name, false);
    }

    private Object readRaw(String name, boolean allVersions) throws IOException {
        return readAgeRawDirect(storeSetup, name, allVersions);
    }

    static Object readAgeRawDirect(StoreSetup storeSetup, String name, boolean allversions) throws IOException {
        final SDataLib dataLib = storeSetup.getDataLib();
        final STableReader reader = storeSetup.getReader();

        Object key = dataLib.newRowKey(new Object[]{name});
        Object get = makeGet(dataLib, key);
        if (allversions) {
            dataLib.setGetMaxVersions(get);
        }
        Object testSTable = reader.open(storeSetup.getPersonTableName());
        try {
            return reader.get(testSTable, get);
        } finally {
            reader.close(testSTable);
        }
    }

    private static Object makeGet(SDataLib dataLib, Object key) throws IOException {
        final Object get = dataLib.newGet(key, null, null, null);
        addPredicateFilter(dataLib, get);
        return get;
    }

    private static Object makeScan(SDataLib dataLib, Object endKey, ArrayList families, Object startKey) throws IOException {
        final Object scan = dataLib.newScan(startKey, endKey, families, null, null);
        addPredicateFilter(dataLib, scan);
        return scan;
    }

    private static void addPredicateFilter(SDataLib dataLib, Object operation) throws IOException {
        final BitSet bitSet = new BitSet(2);
        bitSet.set(0);
        bitSet.set(1);
        EntryPredicateFilter filter = new EntryPredicateFilter(bitSet, Collections.<Predicate>emptyList(), true);
        dataLib.addAttribute(operation, SpliceConstants.ENTRY_PREDICATE_LABEL, filter.toBytes());
    }

    @Test
    public void transactionTimeout() throws IOException, InterruptedException {
        TransactionId t1 = transactor.beginTransaction();
        insertAge(t1, "joe63", 20);
        sleep();
        TransactionId t2 = transactor.beginTransaction();
        insertAge(t2, "joe63", 21);
        transactor.commit(t2);
    }

    @Test
    public void transactionNoTimeoutWithKeepAlive() throws IOException, InterruptedException {
        TransactionId t1 = transactor.beginTransaction();
        insertAge(t1, "joe64", 20);
        sleep();
        transactor.keepAlive(t1);
        TransactionId t2 = transactor.beginTransaction();
        try {
            insertAge(t2, "joe64", 21);
            Assert.fail();
        } catch (WriteConflict e) {
        } catch (RetriesExhaustedWithDetailsException e) {
            assertWriteConflict(e);
        } finally {
            transactor.fail(t2);
        }
    }

    @Test
    public void transactionTimeoutAfterKeepAlive() throws IOException, InterruptedException {
        TransactionId t1 = transactor.beginTransaction();
        insertAge(t1, "joe65", 20);
        sleep();
        transactor.keepAlive(t1);
        sleep();
        TransactionId t2 = transactor.beginTransaction();
        insertAge(t2, "joe65", 21);
        transactor.commit(t2);
    }

    private void sleep() throws InterruptedException {
        if (useSimple) {
            final IncrementingClock clock = (IncrementingClock) storeSetup.getClock();
            clock.delay(2000);
        } else {
            Thread.sleep(2000);
        }
    }

    @Test
    public void transactionStatusAlreadyFailed() throws IOException, InterruptedException {
        final TransactionId t1 = transactor.beginTransaction();
        insertAge(t1, "joe66", 20);
        final TransactionId t2 = transactor.beginTransaction();
        try {
            insertAge(t2, "joe66", 22);
        } catch (WriteConflict e) {
        } catch (RetriesExhaustedWithDetailsException e) {
            assertWriteConflict(e);
        } finally {
            transactor.fail(t2);
        }
        try {
            insertAge(t2, "joe66", 23);
        } catch (WriteConflict e) {
        } catch (RetriesExhaustedWithDetailsException e) {
            assertWriteConflict(e);
        } finally {
            transactor.fail(t2);
        }
    }

    @Test
    public void transactionCommitFailRaceFailWins() throws Exception {
        final TransactionId t1 = transactor.beginTransaction();
        insertAge(t1, "joe67", 20);
        final TransactionId t2 = transactor.beginTransaction();

        final Exception[] exception = new Exception[1];
        final CountDownLatch latch = new CountDownLatch(1);
        final CountDownLatch latch2 = new CountDownLatch(1);
        Tracer.registerStatus(new Function<Object[], Object>() {
            @Override
            public Object apply(@Nullable Object[] input) {
                final Long transactionId = (Long) input[0];
                final TransactionStatus status = (TransactionStatus) input[1];
                final Boolean beforeChange = (Boolean) input[2];
                if (transactionId == t2.getId()) {
                    if (status.equals(TransactionStatus.COMMITTING)) {
                        if (beforeChange) {
                            try {
                                latch.await(2, TimeUnit.SECONDS);
                            } catch (InterruptedException e) {
                                throw new RuntimeException(e);
                            }
                        }
                    } else if (status.equals(TransactionStatus.ERROR)) {
                        if (!beforeChange) {
                            latch.countDown();
                        }
                    }
                }
                return null;
            }
        });

        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    transactor.commit(t2);
                } catch (IOException e) {
                    exception[0] = e;
                    latch2.countDown();
                }
            }
        }).start();
        try {
            insertAge(t2, "joe67", 22);
        } catch (WriteConflict e) {
        } catch (RetriesExhaustedWithDetailsException e) {
            assertWriteConflict(e);
        } finally {
            transactor.fail(t2);
        }

        latch2.await(2, TimeUnit.SECONDS);
        Assert.assertEquals("committing failed", exception[0].getMessage());
        Assert.assertEquals(IOException.class, exception[0].getClass());
    }

    @Test
    public void transactionCommitFailRaceCommitWins() throws Exception {
        final TransactionId t1 = transactor.beginTransaction();
        insertAge(t1, "joe68", 20);
        final TransactionId t2 = transactor.beginTransaction();

        final Exception[] exception = new Exception[1];
        final CountDownLatch latch = new CountDownLatch(1);
        final CountDownLatch latch2 = new CountDownLatch(1);
        Tracer.registerStatus(new Function<Object[], Object>() {
            @Override
            public Object apply(@Nullable Object[] input) {
                final Long transactionId = (Long) input[0];
                final TransactionStatus status = (TransactionStatus) input[1];
                final Boolean beforeChange = (Boolean) input[2];
                if (transactionId == t2.getId()) {
                    if (status.equals(TransactionStatus.ERROR)) {
                        if (beforeChange) {
                            try {
                                latch.await(2, TimeUnit.SECONDS);
                            } catch (InterruptedException e) {
                                throw new RuntimeException(e);
                            }
                        }
                    } else if (status.equals(TransactionStatus.COMMITTED)) {
                        if (!beforeChange) {
                            latch.countDown();
                        }
                    }
                }
                return null;
            }
        });

        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    transactor.commit(t2);
                    latch2.countDown();
                } catch (IOException e) {
                    exception[0] = e;
                }
            }
        }).start();
        try {
            insertAge(t2, "joe68", 22);
            // it would be better if this threw an exception indicating that the transaction has already been committed
        } catch (WriteConflict e) {
        } catch (RetriesExhaustedWithDetailsException e) {
            assertWriteConflict(e);
        } finally {
            transactor.fail(t2);
        }

        latch2.await(2, TimeUnit.SECONDS);
        Assert.assertNull(exception[0]);
    }

    @Test
    public void noCompaction() throws IOException, InterruptedException {
        checkNoCompaction(69, true, new Function<Object[], Object>() {
            @Override
            public Object apply(@Nullable Object[] input) {
                TransactionId t = (TransactionId) input[0];
                Object cell = input[1];
                final SDataLib dataLib = storeSetup.getDataLib();
                final int timestamp = (Integer) dataLib.decode(dataLib.getKeyValueValue(cell), Integer.class);
                Assert.assertEquals(-1, timestamp);
                return null;
            }
        });
    }

    @Test
    public void noCompactionRollback() throws IOException, InterruptedException {
        checkNoCompaction(79, false, new Function<Object[], Object>() {
            @Override
            public Object apply(@Nullable Object[] input) {
                TransactionId t = (TransactionId) input[0];
                Object cell = input[1];
                final SDataLib dataLib = storeSetup.getDataLib();
                final int timestamp = (Integer) dataLib.decode(dataLib.getKeyValueValue(cell), Integer.class);
                Assert.assertEquals(-1, timestamp);
                return null;
            }
        });
    }

    private void checkNoCompaction(int testIndex, boolean commit, Function<Object[], Object> timestampProcessor) throws IOException {
        TransactionId t0 = null;
        String testKey = "joe" + testIndex;
        for (int i = 0; i < 10; i++) {
            TransactionId tx = transactor.beginTransaction();
            if (i == 0) {
                t0 = tx;
            }
            insertAge(tx, testKey + "-" + i, i);
            if (commit) {
                transactor.commit(tx);
            } else {
                transactor.rollback(tx);
            }
        }
        Object result = readRaw(testKey + "-0");
        final SDataLib dataLib = storeSetup.getDataLib();
        final List commitTimestamps = dataLib.getResultColumn(result, dataLib.encode(SNAPSHOT_ISOLATION_FAMILY),
                dataLib.encode(SNAPSHOT_ISOLATION_COMMIT_TIMESTAMP_COLUMN));
        for (Object c : commitTimestamps) {
            timestampProcessor.apply(new Object[]{t0, c});
            Assert.assertEquals(t0.getId(), dataLib.getKeyValueTimestamp(c));
        }
    }

    @Test
    public void compaction() throws IOException, InterruptedException {
        checkCompaction(70, true, new Function<Object[], Object>() {
            @Override
            public Object apply(@Nullable Object[] input) {
                TransactionId t = (TransactionId) input[0];
                Object cell = input[1];
                final SDataLib dataLib = storeSetup.getDataLib();
                final long timestamp = (Long) dataLib.decode(dataLib.getKeyValueValue(cell), Long.class);
                Assert.assertEquals(t.getId() + 1, timestamp);
                return null;
            }
        });
    }

    @Test
    public void compactionRollback() throws IOException, InterruptedException {
        checkCompaction(80, false, new Function<Object[], Object>() {
            @Override
            public Object apply(@Nullable Object[] input) {
                TransactionId t = (TransactionId) input[0];
                Object cell = input[1];
                final SDataLib dataLib = storeSetup.getDataLib();
                final int timestamp = (Integer) dataLib.decode(dataLib.getKeyValueValue(cell), Integer.class);
                Assert.assertEquals(-2, timestamp);
                return null;
            }
        });
    }

    private void checkCompaction(int testIndex, boolean commit, Function<Object[], Object> timestampProcessor) throws IOException, InterruptedException {
        final String testRow = "joe" + testIndex;
        final CountDownLatch latch = new CountDownLatch(2);
        Tracer.registerCompact(new Runnable() {
            @Override
            public void run() {
                latch.countDown();
            }
        });

        final HBaseTestingUtility testCluster = storeSetup.getTestCluster();
        final HBaseAdmin admin = useSimple ? null : testCluster.getHBaseAdmin();
        TransactionId t0 = null;
        for (int i = 0; i < 10; i++) {
            TransactionId tx = transactor.beginTransaction();
            if (i == 0) {
                t0 = tx;
            }
            insertAge(tx, testRow + "-" + i, i);
            if (!useSimple) {
                admin.flush(storeSetup.getPersonTableName());
            }
            if (commit) {
                transactor.commit(tx);
            } else {
                transactor.rollback(tx);
            }
        }

        if (useSimple) {
            final LStore store = (LStore) storeSetup.getStore();
            store.compact(transactor, storeSetup.getPersonTableName());
        } else {
            admin.majorCompact(storeSetup.getPersonTableName());
            Assert.assertTrue(latch.await(2, TimeUnit.SECONDS));
        }
        Object result = readRaw(testRow + "-0");
        final SDataLib dataLib = storeSetup.getDataLib();
        final List commitTimestamps = dataLib.getResultColumn(result, dataLib.encode(SNAPSHOT_ISOLATION_FAMILY),
                dataLib.encode(SNAPSHOT_ISOLATION_COMMIT_TIMESTAMP_COLUMN_STRING));
        for (Object c : commitTimestamps) {
            timestampProcessor.apply(new Object[]{t0, c});
            Assert.assertEquals(t0.getId(), dataLib.getKeyValueTimestamp(c));
        }
    }

    private void compactRegions() throws IOException {
        final HBaseTestingUtility testCluster = storeSetup.getTestCluster();
        final HRegionServer regionServer = testCluster.getRSForFirstRegionInTable((byte[]) storeSetup.getDataLib().encode(storeSetup.getPersonTableName()));
        final List<HRegionInfo> regions = regionServer.getOnlineRegions();
        for (HRegionInfo region : regions) {
            regionServer.compactRegion(region, false);
        }
    }

    @Test
    public void committingRace() throws IOException {
        try {
            final CountDownLatch latch = new CountDownLatch(1);
            final CountDownLatch latch2 = new CountDownLatch(1);
            final CountDownLatch latch3 = new CountDownLatch(1);
            final Boolean[] success = new Boolean[]{false};
            final Boolean[] waits = new Boolean[]{false, false};
            Tracer.registerCommitting(new Function<Long, Object>() {
                @Override
                public Object apply(@Nullable Long aLong) {
                    latch.countDown();
                    try {
                        waits[1] = latch2.await(2, TimeUnit.SECONDS);
                        Assert.assertTrue(waits[1]);
                    } catch (InterruptedException e) {
                        Assert.fail();
                    }
                    return null;
                }
            });
            Tracer.registerWaiting(new Function<Long, Object>() {
                @Override
                public Object apply(@Nullable Long aLong) {
                    latch2.countDown();
                    return null;
                }
            });
            TransactionId t1 = transactor.beginTransaction();
            insertAge(t1, "joe86", 20);

            new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        waits[0] = latch.await(2, TimeUnit.SECONDS);
                        Assert.assertTrue(waits[0]);
                        TransactionId t2 = transactor.beginTransaction();
                        try {
                            insertAge(t2, "joe86", 21);
                            Assert.fail();
                        } catch (WriteConflict e) {
                        } catch (RetriesExhaustedWithDetailsException e) {
                            assertWriteConflict(e);
                        } finally {
                            transactor.fail(t2);
                        }
                        success[0] = true;
                        latch3.countDown();
                    } catch (InterruptedException e) {
                        Assert.fail();
                    } catch (IOException e) {
                        Assert.fail(e.getMessage());
                    }
                }
            }).start();

            transactor.commit(t1);
            Assert.assertTrue(latch3.await(2, TimeUnit.SECONDS));
            Assert.assertTrue(waits[0]);
            Assert.assertTrue(waits[1]);
            Assert.assertTrue(success[0]);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            Tracer.registerCommitting(null);
            Tracer.registerWaiting(null);
        }
    }

    @Test
    public void committingRaceNested() throws IOException {
        try {
            final CountDownLatch latch = new CountDownLatch(1);
            final CountDownLatch latch2 = new CountDownLatch(1);
            final CountDownLatch latch3 = new CountDownLatch(1);
            final Boolean[] success = new Boolean[]{false};
            final Boolean[] waits = new Boolean[]{false, false};
            final TransactionId t1 = transactor.beginTransaction();
            final TransactionId t1a = transactor.beginChildTransaction(t1, true);
            Tracer.registerCommitting(new Function<Long, Object>() {
                @Override
                public Object apply(@Nullable Long timestamp) {
                    if (timestamp.equals(t1a.getId())) {
                        latch.countDown();
                        try {
                            waits[1] = latch2.await(2, TimeUnit.SECONDS);
                            Assert.assertTrue(waits[1]);
                        } catch (InterruptedException e) {
                            Assert.fail();
                        }
                    }
                    return null;
                }
            });
            insertAge(t1a, "joe88", 20);
            Tracer.registerWaiting(new Function<Long, Object>() {
                @Override
                public Object apply(@Nullable Long timestamp) {
                    if (timestamp.equals(t1a.getId())) {
                        latch2.countDown();
                    }
                    return null;
                }
            });

            new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        waits[0] = latch.await(2, TimeUnit.SECONDS);
                        Assert.assertTrue(waits[0]);
                        TransactionId t2 = transactor.beginTransaction();
                        try {
                            insertAge(t2, "joe88", 21);
                            Assert.fail();
                        } catch (WriteConflict e) {
                        } catch (RetriesExhaustedWithDetailsException e) {
                            assertWriteConflict(e);
                        } finally {
                            transactor.fail(t2);
                        }
                        success[0] = true;
                        latch3.countDown();
                    } catch (InterruptedException e) {
                        Assert.fail();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            }).start();

            transactor.commit(t1a);
            transactor.commit(t1);
            Assert.assertTrue(latch3.await(2, TimeUnit.SECONDS));
            Assert.assertTrue(waits[0]);
            Assert.assertTrue(waits[1]);
            Assert.assertTrue(success[0]);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            Tracer.registerCommitting(null);
            Tracer.registerWaiting(null);
        }
    }

    @Test
    public void committingRaceCommitFails() throws Exception {
        try {
            final CountDownLatch latch = new CountDownLatch(1);
            final CountDownLatch latch2 = new CountDownLatch(1);
            final CountDownLatch latch3 = new CountDownLatch(1);
            final Boolean[] success = new Boolean[]{false};
            final Exception[] exception = new Exception[]{null};
            Tracer.registerCommitting(new Function<Long, Object>() {
                @Override
                public Object apply(@Nullable Long aLong) {
                    latch.countDown();
                    try {
                        Assert.assertTrue(latch2.await(5, TimeUnit.SECONDS));
                    } catch (InterruptedException e) {
                        Assert.fail();
                    }
                    return null;
                }
            });
            final TransactionId t1 = transactor.beginTransaction();
            Tracer.registerWaiting(new Function<Long, Object>() {
                @Override
                public Object apply(@Nullable Long aLong) {
                    try {
                        Assert.assertTrue(transactorSetup.transactionStore.recordTransactionStatusChange(t1.getId(),
                                TransactionStatus.COMMITTING, TransactionStatus.ERROR));
                    } catch (IOException e) {
                        exception[0] = e;
                        throw new RuntimeException(e);
                    }
                    latch2.countDown();
                    return null;
                }
            });
            insertAge(t1, "joe87", 20);

            new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        Assert.assertTrue(latch.await(5, TimeUnit.SECONDS));
                        TransactionId t2 = transactor.beginTransaction();
                        insertAge(t2, "joe87", 21);
                        success[0] = true;
                        latch3.countDown();
                    } catch (InterruptedException e) {
                        Assert.fail();
                    } catch (IOException e) {
                        Assert.fail(e.getMessage());
                    }
                }
            }).start();

            try {
                transactor.commit(t1);
                Assert.fail();
            } catch (DoNotRetryIOException e) {
            } catch (RetriesExhaustedWithDetailsException e) {
                assertWriteFailed(e);
            }
            Assert.assertTrue(latch3.await(5, TimeUnit.SECONDS));
            if (exception[0] != null) {
                throw exception[0];
            }
            Assert.assertTrue(success[0]);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            Tracer.registerCommitting(null);
            Tracer.registerWaiting(null);
        }
    }

    @Test
    public void writeReadViaFilterResult() throws IOException {
        TransactionId t1 = transactor.beginTransaction();
        insertAge(t1, "joe89", 20);
        transactor.commit(t1);

        TransactionId t2 = transactor.beginTransaction();
        insertAge(t2, "joe89", 21);

        TransactionId t3 = transactor.beginTransaction();
        Object result = readRaw("joe89", true);
        final IFilterState filterState = transactorSetup.transactor.newFilterState(t3);
        result = transactor.filterResult(filterState, result);
        Assert.assertEquals("joe89 age=20 job=null", resultToString("joe89", result));
    }

    @Test
    public void childIndependentTransactionWriteCommitRollbackRead() throws IOException {
        TransactionId t1 = transactor.beginTransaction();
        TransactionId t2 = transactor.beginChildTransaction(t1, true, true);
        insertAge(t2, "joe52", 21);
        transactor.commit(t2);
        transactor.rollback(t2);
        Assert.assertEquals("joe52 age=21 job=null", read(t1, "joe52"));
        transactor.commit(t1);
    }

    @Test
    public void childIndependentSeesParentWrites() throws IOException {
        TransactionId t1 = transactor.beginTransaction();
        insertAge(t1, "joe41", 20);
        TransactionId t2 = transactor.beginChildTransaction(t1, false, true);
        Assert.assertEquals("joe41 age=20 job=null", read(t2, "joe41"));
    }

    @Test
    public void childIndependentReadOnlySeesParentWrites() throws IOException {
        TransactionId t1 = transactor.beginTransaction();
        insertAge(t1, "joe96", 20);
        final TransactionId t2 = transactor.beginChildTransaction(t1, false, false);
        Assert.assertEquals("joe96 age=20 job=null", read(t2, "joe96"));
    }

    @Test
    public void childIndependentReadUncommittedDoesSeeParentWrites() throws IOException {
        TransactionId t1 = transactor.beginTransaction();
        insertAge(t1, "joe99", 20);
        TransactionId t2 = transactor.beginChildTransaction(t1, false, true, true, true);
        Assert.assertEquals("joe99 age=20 job=null", read(t2, "joe99"));
    }

    @Test
    public void childIndependentReadOnlyUncommittedDoesSeeParentWrites() throws IOException {
        TransactionId t1 = transactor.beginTransaction();
        insertAge(t1, "joe100", 20);
        final TransactionId t2 = transactor.beginChildTransaction(t1, false, false, true, true);
        Assert.assertEquals("joe100 age=20 job=null", read(t2, "joe100"));
    }

    @Test
    public void childIndependentTransactionWithOtherCommitBetweenParentAndChild() throws IOException {
        TransactionId t0 = transactor.beginTransaction();
        insertAge(t0, "joe38", 20);
        transactor.commit(t0);

        TransactionId t1 = transactor.beginTransaction();

        TransactionId otherTransaction = transactor.beginTransaction();
        insertAge(otherTransaction, "joe38", 30);
        transactor.commit(otherTransaction);

        TransactionId t2 = transactor.beginChildTransaction(t1, false, true, null, true);
        Assert.assertEquals("joe38 age=30 job=null", read(t2, "joe38"));
        transactor.commit(t2);
        transactor.commit(t1);
    }

    @Test
    public void childIndependentReadOnlyTransactionWithOtherCommitBetweenParentAndChild() throws IOException {
        TransactionId t0 = transactor.beginTransaction();
        insertAge(t0, "joe97", 20);
        transactor.commit(t0);

        TransactionId t1 = transactor.beginTransaction();

        TransactionId otherTransaction = transactor.beginTransaction();
        insertAge(otherTransaction, "joe97", 30);
        transactor.commit(otherTransaction);

        TransactionId t2 = transactor.beginChildTransaction(t1, false, false, null, true);
        Assert.assertEquals("joe97 age=30 job=null", read(t2, "joe97"));
        transactor.commit(t2);
        transactor.commit(t1);
    }

    @Test
    public void childIndependentTransactionWithReadCommittedOffWithOtherCommitBetweenParentAndChild() throws IOException {
        TransactionId t0 = transactor.beginTransaction();
        insertAge(t0, "joe39", 20);
        transactor.commit(t0);

        TransactionId t1 = transactor.beginTransaction();

        TransactionId otherTransaction = transactor.beginTransaction();
        insertAge(otherTransaction, "joe39", 30);
        transactor.commit(otherTransaction);

        TransactionId t2 = transactor.beginChildTransaction(t1, false, true);
        Assert.assertEquals("joe39 age=20 job=null", read(t2, "joe39"));
        transactor.commit(t2);
        transactor.commit(t1);
    }

    @Test
    public void childIndependentReadOnlyTransactionWithReadCommittedOffWithOtherCommitBetweenParentAndChild() throws IOException {
        TransactionId t0 = transactor.beginTransaction();
        insertAge(t0, "joe98", 20);
        transactor.commit(t0);

        TransactionId t1 = transactor.beginTransaction();

        TransactionId otherTransaction = transactor.beginTransaction();
        insertAge(otherTransaction, "joe98", 30);
        transactor.commit(otherTransaction);

        TransactionId t2 = transactor.beginChildTransaction(t1, false, false);
        Assert.assertEquals("joe98 age=20 job=null", read(t2, "joe98"));
        transactor.commit(t2);
        transactor.commit(t1);
    }

    @Test
    public void childIndependentTransactionWriteRollbackRead() throws IOException {
        TransactionId t1 = transactor.beginTransaction();
        insertAge(t1, "joe28", 20);
        TransactionId t2 = transactor.beginChildTransaction(t1, false, true);
        insertAge(t2, "moe28", 21);
        Assert.assertEquals("joe28 age=20 job=null", read(t1, "joe28"));
        Assert.assertEquals("moe28 absent", read(t1, "moe28"));
        transactor.rollback(t2);
        Assert.assertEquals("joe28 age=20 job=null", read(t1, "joe28"));
        Assert.assertEquals("moe28 absent", read(t1, "moe28"));
        transactor.commit(t1);

        TransactionId t3 = transactor.beginTransaction(false);
        Assert.assertEquals("joe28 age=20 job=null", read(t3, "joe28"));
        Assert.assertEquals("moe28 absent", read(t3, "moe28"));
    }

    @Test
    public void multipleChildIndependentTransactionWriteRead() throws IOException {
        TransactionId t1 = transactor.beginTransaction();
        insertAge(t1, "joe31", 20);
        TransactionId t2 = transactor.beginChildTransaction(t1, false, true);
        TransactionId t3 = transactor.beginChildTransaction(t1, false, true);
        insertAge(t2, "moe31", 21);
        insertJob(t3, "boe31", "baker");
        Assert.assertEquals("joe31 age=20 job=null", read(t1, "joe31"));
        Assert.assertEquals("moe31 absent", read(t1, "moe31"));
        Assert.assertEquals("boe31 absent", read(t1, "boe31"));
        transactor.commit(t2);

        TransactionId t4 = transactor.beginTransaction(false);
        Assert.assertEquals("moe31 age=21 job=null", read(t4, "moe31"));

        transactor.commit(t3);

        TransactionId t5 = transactor.beginTransaction(false);
        Assert.assertEquals("joe31 absent", read(t5, "joe31"));
        Assert.assertEquals("moe31 age=21 job=null", read(t5, "moe31"));
        Assert.assertEquals("boe31 age=null job=baker", read(t5, "boe31"));

        Assert.assertEquals("joe31 age=20 job=null", read(t1, "joe31"));
        Assert.assertEquals("moe31 age=21 job=null", read(t1, "moe31"));
        Assert.assertEquals("boe31 age=null job=baker", read(t1, "boe31"));
        transactor.commit(t1);

        TransactionId t6 = transactor.beginTransaction(false);
        Assert.assertEquals("joe31 age=20 job=null", read(t6, "joe31"));
        Assert.assertEquals("moe31 age=21 job=null", read(t6, "moe31"));
        Assert.assertEquals("boe31 age=null job=baker", read(t6, "boe31"));
    }

    @Test
    public void multipleChildIndependentConflict() throws IOException {
        TransactionId t1 = transactor.beginTransaction();
        TransactionId t2 = transactor.beginChildTransaction(t1, false, true);
        TransactionId t3 = transactor.beginChildTransaction(t1, false, true);
        insertAge(t2, "moe31", 21);
        try {
            insertJob(t3, "moe31", "baker");
            Assert.fail();
        } catch (WriteConflict e) {
        } catch (RetriesExhaustedWithDetailsException e) {
            assertWriteConflict(e);
        } finally {
            transactor.fail(t3);
        }
    }

    @Test
    public void childIndependentTransactionWriteRead() throws IOException {
        TransactionId t1 = transactor.beginTransaction();
        insertAge(t1, "joe29", 20);
        TransactionId t2 = transactor.beginChildTransaction(t1, false, true);
        insertAge(t2, "moe29", 21);
        Assert.assertEquals("moe29 absent", read(t1, "moe29"));
        transactor.commit(t2);
        Assert.assertEquals("moe29 age=21 job=null", read(t1, "moe29"));

        TransactionId t3 = transactor.beginTransaction(false);
        Assert.assertEquals("moe29 age=21 job=null", read(t3, "moe29"));

        Assert.assertEquals("moe29 age=21 job=null", read(t1, "moe29"));
        transactor.commit(t1);

        TransactionId t4 = transactor.beginTransaction(false);
        Assert.assertEquals("moe29 age=21 job=null", read(t4, "moe29"));
    }

    @Test
    public void childIndependentTransactionWriteRollbackParentRead() throws IOException {
        TransactionId t1 = transactor.beginTransaction();
        insertAge(t1, "joe30", 20);
        TransactionId t2 = transactor.beginChildTransaction(t1, false, true);
        insertAge(t2, "moe30", 21);
        transactor.commit(t2);
        transactor.rollback(t1);

        TransactionId t4 = transactor.beginTransaction(false);
        Assert.assertEquals("joe30 absent", read(t4, "joe30"));
        Assert.assertEquals("moe30 age=21 job=null", read(t4, "moe30"));
    }

    @Test
    public void commitParentOfCommittedIndependent() throws IOException {
        TransactionId t1 = transactor.beginTransaction();
        insertAge(t1, "joe49", 20);
        TransactionId t2 = transactor.beginChildTransaction(t1, false, true);
        insertAge(t2, "moe49", 21);
        transactor.commit(t2);
        final Transaction transactionStatusA = transactorSetup.transactionStore.getTransaction(t2);
        transactor.commit(t1);
        final Transaction transactionStatusB = transactorSetup.transactionStore.getTransaction(t2);
        Assert.assertEquals("committing parent of independent transaction should not change the commit time of the child",
                transactionStatusA.getCommitTimestampDirect(), transactionStatusB.getCommitTimestampDirect());
        Assert.assertEquals("committing parent of independent transaction should not change the global commit time of the child",
                transactionStatusA.getEffectiveCommitTimestamp(), transactionStatusB.getEffectiveCommitTimestamp());
    }

    @Test
    public void independentWriteOverlapWithReadCommittedWriter() throws IOException {
        TransactionId parent = transactor.beginTransaction();

        TransactionId other = transactor.beginTransaction(true, false, true);

        TransactionId child = transactor.beginChildTransaction(parent, false, true);
        insertAge(child, "joe33", 22);
        transactor.commit(child);

        Assert.assertEquals("joe33 age=22 job=null", read(other, "joe33"));
        try {
            insertAge(other, "joe33", 21);
            Assert.fail();
        } catch (WriteConflict e) {
        } catch (RetriesExhaustedWithDetailsException e) {
            assertWriteConflict(e);
        } finally {
            transactor.fail(other);
        }
    }

    @Test
    public void independentWriteFollowedByReadCommittedWriter() throws IOException {
        TransactionId parent = transactor.beginTransaction();

        TransactionId child = transactor.beginChildTransaction(parent, false, true);
        insertAge(child, "joe35", 22);
        transactor.commit(child);

        TransactionId other = transactor.beginTransaction(true, false, true);
        insertAge(other, "joe35", 21);
        transactor.commit(other);
    }

    @Test
    public void writeAllNullsRead() throws IOException {
        TransactionId t1 = transactor.beginTransaction();
        Assert.assertEquals("joe113 absent", read(t1, "joe113"));
        insertAge(t1, "joe113", null);
        Assert.assertEquals("joe113 age=null job=null", read(t1, "joe113"));
        transactor.commit(t1);

        TransactionId t2 = transactor.beginTransaction();
        Assert.assertEquals("joe113 age=null job=null", read(t2, "joe113"));
    }

    @Test
    public void writeAllNullsReadOverlap() throws IOException {
        TransactionId t1 = transactor.beginTransaction();
        Assert.assertEquals("joe114 absent", read(t1, "joe114"));
        insertAge(t1, "joe114", null);
        Assert.assertEquals("joe114 age=null job=null", read(t1, "joe114"));

        TransactionId t2 = transactor.beginTransaction();
        Assert.assertEquals("joe114 age=null job=null", read(t1, "joe114"));
        Assert.assertEquals("joe114 absent", read(t2, "joe114"));
        transactor.commit(t1);
        Assert.assertEquals("joe114 absent", read(t2, "joe114"));
    }


    @Test
    public void writeAllNullsWrite() throws IOException {
        TransactionId t1 = transactor.beginTransaction();
        insertAge(t1, "joe115", null);
        Assert.assertEquals("joe115 age=null job=null", read(t1, "joe115"));
        transactor.commit(t1);

        TransactionId t2 = transactor.beginTransaction();
        Assert.assertEquals("joe115 age=null job=null", read(t2, "joe115"));
        insertAge(t2, "joe115", 30);
        Assert.assertEquals("joe115 age=30 job=null", read(t2, "joe115"));
        transactor.commit(t2);
    }

    @Test
    public void writeAllNullsWriteOverlap() throws IOException {
        TransactionId t1 = transactor.beginTransaction();
        Assert.assertEquals("joe116 absent", read(t1, "joe116"));
        insertAge(t1, "joe116", null);
        Assert.assertEquals("joe116 age=null job=null", read(t1, "joe116"));

        TransactionId t2 = transactor.beginTransaction();
        Assert.assertEquals("joe116 age=null job=null", read(t1, "joe116"));
        Assert.assertEquals("joe116 absent", read(t2, "joe116"));
        try {
            insertAge(t2, "joe116", 30);
            Assert.fail();
        } catch (WriteConflict e) {
        } catch (RetriesExhaustedWithDetailsException e) {
            assertWriteConflict(e);
        } finally {
            transactor.fail(t2);
        }
        Assert.assertEquals("joe116 age=null job=null", read(t1, "joe116"));
        transactor.commit(t1);
        try {
            transactor.commit(t2);
            Assert.fail();
        } catch (DoNotRetryIOException dnrio) {
            Assert.assertTrue("Incorrect message returned!", dnrio.getMessage().matches("transaction [0-9]* is not ACTIVE.*"));
        }
    }

    @Test
    public void readAllNullsAfterCommit() throws IOException {
        TransactionId t1 = transactor.beginTransaction();
        insertAge(t1, "joe117", null);
        transactor.commit(t1);
        Assert.assertEquals("joe117 age=null job=null", read(t1, "joe117"));
    }

    @Test
    public void rollbackAllNullAfterCommit() throws IOException {
        TransactionId t1 = transactor.beginTransaction();
        insertAge(t1, "joe118", null);
        transactor.commit(t1);
        transactor.rollback(t1);
        Assert.assertEquals("joe118 age=null job=null", read(t1, "joe118"));
    }

    @Test
    public void writeAllNullScan() throws IOException {
        TransactionId t1 = transactor.beginTransaction();
        Assert.assertEquals("joe119 absent", read(t1, "joe119"));
        insertAge(t1, "joe119", null);
        Assert.assertEquals("joe119 age=null job=null", read(t1, "joe119"));
        transactor.commit(t1);

        TransactionId t2 = transactor.beginTransaction();
        Assert.assertEquals("joe119 age=null job=null[ S.TX@~9 ]", scan(t2, "joe119"));

        Assert.assertEquals("joe119 age=null job=null", read(t2, "joe119"));
    }

    @Test
    public void batchWriteRead() throws IOException {
        TransactionId t1 = transactor.beginTransaction();
        Assert.assertEquals("joe144 absent", read(t1, "joe144"));
        insertAgeBatch(new Object[]{t1, "joe144", 20}, new Object[]{t1, "bob144", 30});
        Assert.assertEquals("joe144 age=20 job=null", read(t1, "joe144"));
        Assert.assertEquals("bob144 age=30 job=null", read(t1, "bob144"));
        transactor.commit(t1);

        TransactionId t2 = transactor.beginTransaction();
        Assert.assertEquals("joe144 age=20 job=null", read(t2, "joe144"));
        Assert.assertEquals("bob144 age=30 job=null", read(t2, "bob144"));
    }

    @Test
    public void writeDeleteBatchInsertRead() throws IOException {
        TransactionId t1 = transactor.beginTransaction();
        insertAge(t1, "joe145", 10);
        deleteRow(t1, "joe145");
        insertAgeBatch(new Object[]{t1, "joe145", 20}, new Object[]{t1, "bob145", 30});
        Assert.assertEquals("joe145 age=20 job=null", read(t1, "joe145"));
        Assert.assertEquals("bob145 age=30 job=null", read(t1, "bob145"));
        transactor.commit(t1);

        TransactionId t2 = transactor.beginTransaction();
        Assert.assertEquals("joe145 age=20 job=null", read(t2, "joe145"));
        Assert.assertEquals("bob145 age=30 job=null", read(t2, "bob145"));
    }

    @Test
    public void writeManyDeleteBatchInsertRead() throws IOException {
        TransactionId t1 = transactor.beginTransaction();
        insertAge(t1, "146joe", 10);
        insertAge(t1, "146doe", 20);
        insertAge(t1, "146boe", 30);
        insertAge(t1, "146moe", 40);
        insertAge(t1, "146zoe", 50);

        deleteRow(t1, "146joe");
        deleteRow(t1, "146doe");
        deleteRow(t1, "146boe");
        deleteRow(t1, "146moe");
        deleteRow(t1, "146zoe");

        insertAgeBatch(new Object[]{t1, "146zoe", 51}, new Object[]{t1, "146moe", 41},
                new Object[]{t1, "146boe", 31}, new Object[]{t1, "146doe", 21}, new Object[]{t1, "146joe", 11});
        Assert.assertEquals("146joe age=11 job=null", read(t1, "146joe"));
        Assert.assertEquals("146doe age=21 job=null", read(t1, "146doe"));
        Assert.assertEquals("146boe age=31 job=null", read(t1, "146boe"));
        Assert.assertEquals("146moe age=41 job=null", read(t1, "146moe"));
        Assert.assertEquals("146zoe age=51 job=null", read(t1, "146zoe"));
        transactor.commit(t1);

        TransactionId t2 = transactor.beginTransaction();
        Assert.assertEquals("146joe age=11 job=null", read(t2, "146joe"));
        Assert.assertEquals("146doe age=21 job=null", read(t2, "146doe"));
        Assert.assertEquals("146boe age=31 job=null", read(t2, "146boe"));
        Assert.assertEquals("146moe age=41 job=null", read(t2, "146moe"));
        Assert.assertEquals("146zoe age=51 job=null", read(t2, "146zoe"));
    }

    @Test
    public void writeManyDeleteBatchInsertSomeRead() throws IOException {
        TransactionId t1 = transactor.beginTransaction();
        insertAge(t1, "147joe", 10);
        insertAge(t1, "147doe", 20);
        insertAge(t1, "147boe", 30);
        insertAge(t1, "147moe", 40);
        insertAge(t1, "147zoe", 50);

        deleteRow(t1, "147joe");
        deleteRow(t1, "147doe");
        deleteRow(t1, "147boe");
        deleteRow(t1, "147moe");
        deleteRow(t1, "147zoe");

        insertAgeBatch(new Object[]{t1, "147zoe", 51}, new Object[]{t1, "147boe", 31}, new Object[]{t1, "147joe", 11});
        Assert.assertEquals("147joe age=11 job=null", read(t1, "147joe"));
        Assert.assertEquals("147boe age=31 job=null", read(t1, "147boe"));
        Assert.assertEquals("147zoe age=51 job=null", read(t1, "147zoe"));
        transactor.commit(t1);

        TransactionId t2 = transactor.beginTransaction();
        Assert.assertEquals("147joe age=11 job=null", read(t2, "147joe"));
        Assert.assertEquals("147boe age=31 job=null", read(t2, "147boe"));
        Assert.assertEquals("147zoe age=51 job=null", read(t2, "147zoe"));
    }

    @Test
    public void oldestActiveTransactionsOne() throws IOException {
        final TransactionId t1 = transactor.beginTransaction();
        transactorSetup.timestampSource.rememberTimestamp(t1.getId() - 1);
        final List<TransactionId> result = transactor.getActiveTransactionIds(t1);
        Assert.assertEquals(1, result.size());
        Assert.assertEquals(t1.getId(), result.get(0).getId());
    }

    @Test
    public void oldestActiveTransactionsTwo() throws IOException {
        final TransactionId t0 = transactor.beginTransaction();
        transactorSetup.timestampSource.rememberTimestamp(t0.getId());
        final TransactionId t1 = transactor.beginTransaction();
        final List<TransactionId> result = transactor.getActiveTransactionIds(t1);
        Assert.assertEquals(2, result.size());
        Assert.assertEquals(t0.getId(), result.get(0).getId());
        Assert.assertEquals(t1.getId(), result.get(1).getId());
    }

    @Test
    public void oldestActiveTransactionsFuture() throws IOException {
        final TransactionId t0 = transactor.beginTransaction();
        transactorSetup.timestampSource.rememberTimestamp(t0.getId());
        final TransactionId t1 = transactor.beginTransaction();
        transactor.beginTransaction();
        final List<TransactionId> result = transactor.getActiveTransactionIds(t1);
        Assert.assertEquals(2, result.size());
        Assert.assertEquals(t0.getId(), result.get(0).getId());
        Assert.assertEquals(t1.getId(), result.get(1).getId());
    }

    @Test
    public void oldestActiveTransactionsSkipCommitted() throws IOException {
        final TransactionId t0 = transactor.beginTransaction();
        transactorSetup.timestampSource.rememberTimestamp(t0.getId());
        final TransactionId t1 = transactor.beginTransaction();
        final TransactionId t2 = transactor.beginTransaction();
        transactor.commit(t0);
        final List<TransactionId> result = transactor.getActiveTransactionIds(t2);
        Assert.assertEquals(2, result.size());
        Assert.assertEquals(t1.getId(), result.get(0).getId());
        Assert.assertEquals(t2.getId(), result.get(1).getId());
    }

    @Test
    public void oldestActiveTransactionsSkipCommittedGap() throws IOException {
        final TransactionId t0 = transactor.beginTransaction();
        transactorSetup.timestampSource.rememberTimestamp(t0.getId());
        final TransactionId t1 = transactor.beginTransaction();
        final TransactionId t2 = transactor.beginTransaction();
        transactor.commit(t1);
        final List<TransactionId> result = transactor.getActiveTransactionIds(t2);
        Assert.assertEquals(2, result.size());
        Assert.assertEquals(t0.getId(), result.get(0).getId());
        Assert.assertEquals(t2.getId(), result.get(1).getId());
    }

    @Test
    public void oldestActiveTransactionsSavedTimestampAdvances() throws IOException {
        final TransactionId t0 = transactor.beginTransaction();
        transactorSetup.timestampSource.rememberTimestamp(t0.getId() - 1);
        transactor.beginTransaction();
        final TransactionId t2 = transactor.beginTransaction();
        transactor.commit(t0);
        final long originalSavedTimestamp = transactorSetup.timestampSource.retrieveTimestamp();
        transactor.getActiveTransactionIds(t2);
        final long newSavedTimestamp = transactorSetup.timestampSource.retrieveTimestamp();
        Assert.assertEquals(originalSavedTimestamp + 2, newSavedTimestamp);
    }

    @Test
    public void oldestActiveTransactionsFillMissing() throws IOException {
        final TransactionId t0 = transactor.beginTransaction();
        transactorSetup.timestampSource.rememberTimestamp(t0.getId());
        TransactionId committedTransactionID = null;
        for (int i = 0; i < 1000; i++) {
            final TransactionId transactionId = transactor.beginTransaction();
            if (committedTransactionID == null) {
                committedTransactionID = transactionId;
            }
            transactor.commit(transactionId);
        }
        Long commitTimestamp = (committedTransactionID.getId() + 1);
        final TransactionId voidedTransactionID = transactor.transactionIdFromString(commitTimestamp.toString());
        try {
            transactor.getTransactionStatus(voidedTransactionID);
            Assert.fail();
        } catch (RuntimeException ex) {
            Assert.assertTrue(ex.getMessage().startsWith("transaction ID not found:"));
        }

        final TransactionId t1 = transactor.beginTransaction();
        final List<TransactionId> result = transactor.getActiveTransactionIds(t1);
        Assert.assertEquals(2, result.size());
        Assert.assertEquals(t0.getId(), result.get(0).getId());
        Assert.assertEquals(t1.getId(), result.get(1).getId());
        Assert.assertEquals(TransactionStatus.ERROR, transactor.getTransactionStatus(voidedTransactionID));
    }

    @Test
    public void oldestActiveTransactionsManyActive() throws IOException {
        final TransactionId t0 = transactor.beginTransaction();
        transactorSetup.timestampSource.rememberTimestamp(t0.getId());
        for (int i = 0; i < 500; i++) {
            transactor.beginTransaction();
        }
        final TransactionId t1 = transactor.beginTransaction();
        final List<TransactionId> result = transactor.getActiveTransactionIds(t1);
        Assert.assertEquals(502, result.size());
        Assert.assertEquals(t0.getId(), result.get(0).getId());
        Assert.assertEquals(t1.getId(), result.get(result.size() - 1).getId());
    }

    @Test
    public void oldestActiveTransactionsTooManyActive() throws IOException {
        final TransactionId t0 = transactor.beginTransaction();
        transactorSetup.timestampSource.rememberTimestamp(t0.getId());
        for (int i = 0; i < 1000; i++) {
            transactor.beginTransaction();
        }

        final TransactionId t1 = transactor.beginTransaction();
        try {
            transactor.getActiveTransactionIds(t1);
            Assert.fail();
        } catch (RuntimeException ex) {
            Assert.assertTrue(ex.getMessage().startsWith("expected max id of"));
        }
    }
}