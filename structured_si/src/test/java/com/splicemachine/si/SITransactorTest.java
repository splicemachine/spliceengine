package com.splicemachine.si;

import com.carrotsearch.hppc.BitSet;
import com.carrotsearch.hppc.ObjectArrayList;
import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.splicemachine.constants.SIConstants;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.hbase.KeyValueUtils;
import com.splicemachine.si.api.Clock;
import com.splicemachine.si.api.TransactionManager;
import com.splicemachine.si.api.TransactionStatus;
import com.splicemachine.si.api.Transactor;
import com.splicemachine.si.coprocessors.RegionRollForwardAction;
import com.splicemachine.si.data.api.SDataLib;
import com.splicemachine.si.data.api.STableReader;
import com.splicemachine.si.data.hbase.HbRegion;
import com.splicemachine.si.data.hbase.IHTable;
import com.splicemachine.si.data.light.*;
import com.splicemachine.si.impl.*;
import com.splicemachine.si.impl.translate.Transcoder;
import com.splicemachine.si.impl.translate.Translator;
import com.splicemachine.storage.EntryDecoder;
import com.splicemachine.storage.EntryEncoder;
import com.splicemachine.storage.EntryPredicateFilter;
import com.splicemachine.storage.Predicate;
import com.splicemachine.utils.Providers;
import com.splicemachine.utils.kryo.KryoPool;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.OperationStatus;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.*;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@SuppressWarnings("unchecked")
public class SITransactorTest extends SIConstants {
    boolean useSimple = true;
    boolean usePacked = false;

    StoreSetup storeSetup;
    TestTransactionSetup transactorSetup;
    Transactor transactor;
		TransactionManager control;

    @SuppressWarnings("unchecked")
		void baseSetUp() {
        transactor = transactorSetup.transactor;
				control = transactorSetup.control;
        transactorSetup.rollForwardQueue = new SynchronousRollForwardQueue(
								new RollForwardAction() {
                    @Override
                    public Boolean rollForward(long transactionId, List<byte[]> rowList) throws IOException {
                        final STableReader reader = storeSetup.getReader();
                        Object testSTable = reader.open(storeSetup.getPersonTableName());
												new RegionRollForwardAction((IHTable)testSTable,
																Providers.basicProvider(transactorSetup.transactionStore),
																Providers.basicProvider(transactorSetup.dataStore)).rollForward(transactionId,rowList);
//                        transactor.rollForward(testSTable, transactionId, rowList);
                        return true;
                    }
                }, 10, 100, 1000, "test");
    }

    @Before
    public void setUp() {
        SynchronousRollForwardQueue.scheduler = Executors.newScheduledThreadPool(1);
        storeSetup = new LStoreSetup();
        transactorSetup = new TestTransactionSetup(storeSetup, true);
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

        byte[] key = dataLib.newRowKey(new Object[]{name});
        Object scan = makeScan(dataLib, key, null, key);
        transactorSetup.clientTransactor.initializeScan(transactionId.getTransactionIdString(), scan, true);
        Object testSTable = reader.open(storeSetup.getPersonTableName());
        try {
            Iterator<Result> results = reader.scan(testSTable, scan);
            if (results.hasNext()) {
                Result rawTuple = results.next();
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

        byte[] key = dataLib.newRowKey(new Object[]{startKey});
        byte[] endKey = dataLib.newRowKey(new Object[]{stopKey});
        Object get = makeScan(dataLib, endKey, null, key);
        if (!useSimple && filterValue != null) {
            final Scan scan = (Scan) get;
            SingleColumnValueFilter filter = new SingleColumnValueFilter((byte[]) transactorSetup.family,
                    (byte[]) transactorSetup.ageQualifier,
                    CompareFilter.CompareOp.EQUAL,
                    new BinaryComparator(dataLib.encode(filterValue)));
            filter.setFilterIfMissing(true);
            scan.setFilter(filter);
        }
        transactorSetup.clientTransactor.initializeScan(transactionId.getTransactionIdString(), get, includeSIColumn
        );
        Object testSTable = reader.open(storeSetup.getPersonTableName());
        try {
            Iterator<Result> results = reader.scan(testSTable, get);

            StringBuilder result = new StringBuilder();
            while (results.hasNext()) {
                final Result value = results.next();
                final String name = Bytes.toString(value.getRow());
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

    static void insertAgeDirect(boolean useSimple, boolean usePacked, TestTransactionSetup transactorSetup, StoreSetup storeSetup,
                                TransactionId transactionId, String name, Integer age) throws IOException {
        insertField(useSimple, usePacked, transactorSetup, storeSetup, transactionId, name, transactorSetup.ageQualifier, age);
    }

    static void insertAgeDirectBatch(boolean useSimple, boolean usePacked, TestTransactionSetup transactorSetup, StoreSetup storeSetup,
                                     Object[] args) throws IOException {
        insertFieldBatch(useSimple, usePacked, transactorSetup, storeSetup, args, transactorSetup.ageQualifier);
    }

    static void insertJobDirect(boolean useSimple, boolean usePacked, TestTransactionSetup transactorSetup, StoreSetup storeSetup,
                                TransactionId transactionId, String name, String job) throws IOException {
        insertField(useSimple, usePacked, transactorSetup, storeSetup, transactionId, name, transactorSetup.jobQualifier, job);
    }

 		private static void insertField(boolean useSimple, boolean usePacked, TestTransactionSetup transactorSetup, StoreSetup storeSetup,
                                    TransactionId transactionId, String name, byte[] qualifier, Object fieldValue)
            throws IOException {
        final SDataLib dataLib = storeSetup.getDataLib();
        final STableReader reader = storeSetup.getReader();
        Object put = makePut(useSimple, usePacked, transactorSetup, transactionId, name, qualifier, fieldValue, dataLib);
        processPutDirect(useSimple, usePacked, transactorSetup, storeSetup, reader, (OperationWithAttributes)put);
    }

    private static Object makePut(boolean useSimple, boolean usePacked, TestTransactionSetup transactorSetup, TransactionId transactionId, String name, byte[] qualifier, Object fieldValue, SDataLib dataLib) throws IOException {
        byte[] key = dataLib.newRowKey(new Object[]{name});
        OperationWithAttributes put = dataLib.newPut(key);
        if (fieldValue != null) {
            if (usePacked && !useSimple) {
                int columnIndex;
								if(Bytes.equals(qualifier,transactorSetup.ageQualifier)){
                    columnIndex = 0;
                } else if (Bytes.equals(qualifier,transactorSetup.jobQualifier)){
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
                    dataLib.addKeyValueToPut(put, transactorSetup.family, dataLib.encode("x"), -1, packedRow);
                } finally {
                    entryEncoder.close();
                }
            } else {
                dataLib.addKeyValueToPut(put, transactorSetup.family, qualifier, -1l, dataLib.encode(fieldValue));
            }
        }
        transactorSetup.clientTransactor.initializePut(transactionId.getTransactionIdString(), put);
        return put;
    }

    private static void insertFieldBatch(boolean useSimple, boolean usePacked, TestTransactionSetup transactorSetup, StoreSetup storeSetup,
                                         Object[] args, byte[] qualifier) throws IOException {
        final SDataLib dataLib = storeSetup.getDataLib();
        final STableReader reader = storeSetup.getReader();
        OperationWithAttributes[] puts = new OperationWithAttributes[args.length];
        int i = 0;
        for (Object subArgs : args) {
            Object[] subArgsArray = (Object[]) subArgs;
            TransactionId transactionId = (TransactionId) subArgsArray[0];
            String name = (String) subArgsArray[1];
            Object fieldValue = subArgsArray[2];
            Object put = makePut(useSimple, usePacked, transactorSetup, transactionId, name, qualifier, fieldValue, dataLib);
            puts[i] = (OperationWithAttributes)put;
            i++;
        }
        processPutDirectBatch(useSimple, usePacked, transactorSetup, storeSetup, reader, puts);
    }

    static void deleteRowDirect(boolean useSimple, boolean usePacked, TestTransactionSetup transactorSetup, StoreSetup storeSetup,
                                TransactionId transactionId, String name) throws IOException {
        final SDataLib dataLib = storeSetup.getDataLib();
        final STableReader reader = storeSetup.getReader();

        byte[] key = dataLib.newRowKey(new Object[]{name});
        OperationWithAttributes deletePut = transactorSetup.clientTransactor.createDeletePut(transactionId, key);
        processPutDirect(useSimple, usePacked, transactorSetup, storeSetup, reader, deletePut);
    }

    private static void processPutDirect(boolean useSimple, boolean usePacked,
                                         TestTransactionSetup transactorSetup, StoreSetup storeSetup, STableReader reader,
                                         OperationWithAttributes put) throws IOException {
        Object testSTable = reader.open(storeSetup.getPersonTableName());
        try {
            if (useSimple) {
                if (usePacked) {
                    Assert.assertTrue(transactorSetup.transactor.processPut(testSTable, transactorSetup.rollForwardQueue,
                            ((LTuple) put).pack(Bytes.toBytes("V"), Bytes.toBytes("p"))));
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
                                              TestTransactionSetup transactorSetup, StoreSetup storeSetup, STableReader reader,
                                              OperationWithAttributes[] puts) throws IOException {
        final Object testSTable = reader.open(storeSetup.getPersonTableName());
        try {
            Object tableToUse = testSTable;
            if (useSimple) {
                if (usePacked) {
                    OperationWithAttributes[] packedPuts = new OperationWithAttributes[puts.length];
                    for (int i = 0; i < puts.length; i++) {
                        packedPuts[i] = ((LTuple) puts[i]).pack(Bytes.toBytes("V"), Bytes.toBytes("p"));
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

    static String readAgeDirect(boolean useSimple, boolean usePacked, TestTransactionSetup transactorSetup, StoreSetup storeSetup,
                                TransactionId transactionId, String name) throws IOException {
        final SDataLib dataLib = storeSetup.getDataLib();
        final STableReader reader = storeSetup.getReader();

        byte[] key = dataLib.newRowKey(new Object[]{name});
        Object get = makeGet(dataLib, key);
        transactorSetup.clientTransactor.initializeGet(transactionId.getTransactionIdString(), get);
        Object testSTable = reader.open(storeSetup.getPersonTableName());
        try {
            Result rawTuple = reader.get(testSTable, get);
            return readRawTuple(useSimple, usePacked, transactorSetup, storeSetup, transactionId, name, dataLib, rawTuple, true, true, false, true);
        } finally {
            reader.close(testSTable);
        }
    }

    static String scanNoColumnsDirect(boolean useSimple, boolean usePacked, TestTransactionSetup transactorSetup, StoreSetup storeSetup,
                                      TransactionId transactionId, String name, boolean deleted) throws IOException {
        final SDataLib dataLib = storeSetup.getDataLib();
        final STableReader reader = storeSetup.getReader();

        byte[] endKey = dataLib.newRowKey(new Object[]{name});
        final ArrayList families = new ArrayList();
				Object scan = makeScan(dataLib, endKey, families, endKey);
        transactorSetup.clientTransactor.initializeScan(transactionId.getTransactionIdString(), scan, true);
        transactorSetup.readController.preProcessScan(scan);
        Object testSTable = reader.open(storeSetup.getPersonTableName());
        try {
            Iterator<Result> results = reader.scan(testSTable, scan);
						Assert.assertTrue(deleted || results.hasNext());
//						if (!deleted) {
//								Assert.assertTrue(results.hasNext());
//						}
						Result rawTuple = results.next();
            Assert.assertTrue(!results.hasNext());
            return readRawTuple(useSimple, usePacked, transactorSetup, storeSetup, transactionId, name, dataLib, rawTuple, false, true, false, true);
        } finally {
            reader.close(testSTable);
        }
    }

    private static String readRawTuple(boolean useSimple, boolean usePacked, TestTransactionSetup transactorSetup, StoreSetup storeSetup,
                                       TransactionId transactionId, String name, SDataLib dataLib, Result rawTuple,
                                       boolean singleRowRead, boolean includeSIColumn,
                                       boolean dumpKeyValues, boolean decodeTimestamps)
            throws IOException {
        if (rawTuple != null) {
            Result result = rawTuple;
            if (useSimple) {
                IFilterState filterState;
                try {
                    filterState = transactorSetup.readController.newFilterState(transactorSetup.rollForwardQueue,
                            transactionId, includeSIColumn);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                if (usePacked) {
                    filterState = new FilterStatePacked(null, storeSetup.getDataLib(), transactorSetup.dataStore, (FilterState) filterState, new LRowAccumulator());
                }
                result = transactorSetup.readController.filterResult(filterState, rawTuple);
                if (usePacked && result != null) {
										List<KeyValue> kvs = Lists.newArrayList(result.raw());
										result = new Result(LTuple.unpack(kvs,Bytes.toBytes("V")));
//										result = ((LTuple) result).unpack(Bytes.toBytes("V"));
                }
            } else {
                if (result.size() == 0) {
                    return name + " absent";
                }
                if (usePacked) {
                    byte[] resultKey = result.getRow();
                    final List newKeyValues = new ArrayList();
										List<KeyValue> list = dataLib.listResult(result);
										for (KeyValue kv : list) {
												if(kv.matchingColumn(transactorSetup.family,dataLib.encode("x"))){
//                        if (Bytes.equals(dataLib.getKeyValueFamily(kv), transactorSetup.family) && Bytes.equals(dataLib.getKeyValueQualifier(kv), dataLib.encode("x"))) {
                            final byte[] packedColumns = kv.getValue();
                            final EntryDecoder entryDecoder = new EntryDecoder(KryoPool.defaultPool());
                            entryDecoder.set(packedColumns);
                            final MultiFieldDecoder decoder = entryDecoder.getEntryDecoder();
                            if (entryDecoder.isSet(0)) {
                                final int age = decoder.decodeNextInt();
                                final KeyValue ageKeyValue = KeyValueUtils.newKeyValue(resultKey, transactorSetup.family,
																				transactorSetup.ageQualifier,
																				kv.getTimestamp(),
																				dataLib.encode(age));
                                newKeyValues.add(ageKeyValue);
                            }
                            if (entryDecoder.isSet(1)) {
                                final String job = decoder.decodeNextString();
                                final KeyValue jobKeyValue = KeyValueUtils.newKeyValue(resultKey, transactorSetup.family,
                                        transactorSetup.jobQualifier,
                                        kv.getTimestamp(),
                                        dataLib.encode(job));
                                newKeyValues.add(jobKeyValue);
                            }
                        } else {
                            newKeyValues.add(kv);
                        }
                    }
                    result = new Result(newKeyValues);
                }
            }
            if (result != null) {
                String suffix = dumpKeyValues ? "[ " + resultToKeyValueString(transactorSetup, dataLib, result, decodeTimestamps) + "]" : "";
                return resultToStringDirect(transactorSetup, name, dataLib, result) + suffix;
            }
        }
        if (singleRowRead) {
            return name + " absent";
        } else {
            return "";
        }
    }

    private String resultToString(String name, Result result) {
        return resultToStringDirect(transactorSetup, name, storeSetup.getDataLib(), result);
    }

    private static String resultToStringDirect(TestTransactionSetup transactorSetup, String name, SDataLib dataLib, Result result) {
        final byte[] ageValue = result.getValue(transactorSetup.family, transactorSetup.ageQualifier);

        Integer age = (Integer) dataLib.decode(ageValue, Integer.class);
        final byte[] jobValue = result.getValue(transactorSetup.family, transactorSetup.jobQualifier);
        String job = (String) dataLib.decode(jobValue, String.class);
        return name + " age=" + age + " job=" + job;
    }

    private static String resultToKeyValueString(TestTransactionSetup transactorSetup, SDataLib dataLib,
																								 Result result, boolean decodeTimestamps) {
        final Map<Long, String> timestampDecoder = new HashMap<Long, String>();
        final StringBuilder s = new StringBuilder();
				List<KeyValue> list = dataLib.listResult(result);
				for (KeyValue kv : list) {
            final byte[] family = kv.getFamily();
            String qualifier = "?";
            String value = "?";
						if(kv.matchingQualifier(transactorSetup.ageQualifier)){
                value = dataLib.decode(kv.getValue(), Integer.class).toString();
                qualifier = "age";
            } else if(kv.matchingQualifier(transactorSetup.jobQualifier)){
                value = (String) dataLib.decode(kv.getValue(), String.class);
                qualifier = "job";
            } else if(kv.matchingQualifier(transactorSetup.tombstoneQualifier)){
                qualifier = "X";
                value = "";
            } else if(kv.matchingQualifier(transactorSetup.commitTimestampQualifier)){
                qualifier = "TX";
                value = "";
            }
            final long timestamp = kv.getTimestamp();
            String timestampString;
            if (decodeTimestamps) {
                timestampString = timestampToStableString(timestampDecoder, timestamp);
            } else {
                timestampString = Long.toString(timestamp);
            }
            String equalString = value.length() > 0 ? "=" : "";
            s.append(Bytes.toString(family))
										.append(".").append(qualifier)
										.append("@").append(timestampString)
										.append(equalString).append(value).append(" ");
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

//		private void dumpStore(String label) {
//        if (useSimple) {
//            System.out.println("store " + label + " =" + storeSetup.getStore());
//        }
//    }

    @Test
    public void writeRead() throws IOException {
        TransactionId t1 = control.beginTransaction();
        Assert.assertEquals("joe9 absent", read(t1, "joe9"));
        insertAge(t1, "joe9", 20);
        Assert.assertEquals("joe9 age=20 job=null", read(t1, "joe9"));
        control.commit(t1);

        TransactionId t2 = control.beginTransaction();
        Assert.assertEquals("joe9 age=20 job=null", read(t2, "joe9"));
    }

    @Test
    public void writeReadOverlap() throws IOException {
        TransactionId t1 = control.beginTransaction();
        Assert.assertEquals("joe8 absent", read(t1, "joe8"));
        insertAge(t1, "joe8", 20);
        Assert.assertEquals("joe8 age=20 job=null", read(t1, "joe8"));

        TransactionId t2 = control.beginTransaction();
        Assert.assertEquals("joe8 age=20 job=null", read(t1, "joe8"));
        Assert.assertEquals("joe8 absent", read(t2, "joe8"));
        control.commit(t1);
        Assert.assertEquals("joe8 absent", read(t2, "joe8"));
    }

    @Test
    public void writeWrite() throws IOException {
        TransactionId t1 = control.beginTransaction();
        insertAge(t1, "joe", 20);
        Assert.assertEquals("joe age=20 job=null", read(t1, "joe"));
        control.commit(t1);

        TransactionId t2 = control.beginTransaction();
        Assert.assertEquals("joe age=20 job=null", read(t2, "joe"));
        insertAge(t2, "joe", 30);
        Assert.assertEquals("joe age=30 job=null", read(t2, "joe"));
        control.commit(t2);
    }

    @Test
    public void writeWriteOverlap() throws IOException {
        TransactionId t1 = control.beginTransaction();
        Assert.assertEquals("joe012 absent", read(t1, "joe012"));
        insertAge(t1, "joe012", 20);
        Assert.assertEquals("joe012 age=20 job=null", read(t1, "joe012"));

        TransactionId t2 = control.beginTransaction();
        Assert.assertEquals("joe012 age=20 job=null", read(t1, "joe012"));
        Assert.assertEquals("joe012 absent", read(t2, "joe012"));
        try {
            insertAge(t2, "joe012", 30);
            Assert.fail();
        } catch (WriteConflict e) {
        } catch (RetriesExhaustedWithDetailsException e) {
            assertWriteConflict(e);
        } finally {
            control.fail(t2);
        }
        Assert.assertEquals("joe012 age=20 job=null", read(t1, "joe012"));
        control.commit(t1);
        try {
            control.commit(t2);
            Assert.fail();
        } catch (DoNotRetryIOException dnrio) {
            Assert.assertTrue("Incorrect message returned!", dnrio.getMessage().matches("transaction [0-9]* is not ACTIVE.*"));
        }
    }

    @Test
    public void writeWriteOverlapRecovery() throws IOException {
        TransactionId t1 = control.beginTransaction();
        Assert.assertEquals("joe142 absent", read(t1, "joe142"));
        insertAge(t1, "joe142", 20);
        Assert.assertEquals("joe142 age=20 job=null", read(t1, "joe142"));

        TransactionId t2 = control.beginTransaction();
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
        control.commit(t1);
        control.commit(t2);
    }

    @Test
    public void readAfterCommit() throws IOException {
        TransactionId t1 = control.beginTransaction();
        insertAge(t1, "joe3", 20);
        control.commit(t1);
        Assert.assertEquals("joe3 age=20 job=null", read(t1, "joe3"));
    }

    @Test
    public void rollbackAfterCommit() throws IOException {
        TransactionId t1 = control.beginTransaction();
        insertAge(t1, "joe50", 20);
        control.commit(t1);
        control.rollback(t1);
        Assert.assertEquals("joe50 age=20 job=null", read(t1, "joe50"));
    }

    @Test
    public void writeScan() throws IOException {
        TransactionId t1 = control.beginTransaction();
        Assert.assertEquals("joe4 absent", read(t1, "joe4"));
        insertAge(t1, "joe4", 20);
        Assert.assertEquals("joe4 age=20 job=null", read(t1, "joe4"));
        control.commit(t1);

        TransactionId t2 = control.beginTransaction();
        Assert.assertEquals("joe4 age=20 job=null[ S.TX@~9 V.age@~9=20 ]", scan(t2, "joe4"));

        Assert.assertEquals("joe4 age=20 job=null", read(t2, "joe4"));
    }

    @Test
    public void writeScanWithDeleteActive() throws IOException {
        TransactionId t1 = control.beginTransaction();
        insertAge(t1, "joe128", 20);
        control.commit(t1);

        TransactionId t2 = control.beginTransaction();

        TransactionId t3 = control.beginTransaction();

        deleteRow(t2, "joe128");

        Assert.assertEquals("joe128 age=20 job=null[ S.TX@~9 V.age@~9=20 ]", scan(t3, "joe128"));
        control.commit(t2);
    }

    @Test
    public void writeScanNoColumns() throws IOException {
        TransactionId t1 = control.beginTransaction();
        insertAge(t1, "joe84", 20);
        control.commit(t1);

        TransactionId t2 = control.beginTransaction();
        // reading si only (i.e. no data columns) returns the rows but none of the "user" data
        Assert.assertEquals("joe84 age=null job=null", scanNoColumns(t2, "joe84", false));
    }

    @Test
    public void writeDeleteScanNoColumns() throws IOException {
        TransactionId t1 = control.beginTransaction();
        insertAge(t1, "joe85", 20);
        control.commit(t1);

        TransactionId t2 = control.beginTransaction();
        deleteRow(t2, "joe85");
        control.commit(t2);


        TransactionId t3 = control.beginTransaction();
        // reading si only (i.e. no data columns) returns the rows but none of the "user" data
        Assert.assertEquals("", scanNoColumns(t3, "joe85", true));
    }

    @Test
    public void writeScanMultipleRows() throws IOException {
        TransactionId t1 = control.beginTransaction();
        insertAge(t1, "17joe", 20);
        insertAge(t1, "17bob", 30);
        insertAge(t1, "17boe", 40);
        insertAge(t1, "17tom", 50);
        control.commit(t1);

        TransactionId t2 = control.beginTransaction();
        String expected = "17bob age=30 job=null[ V.age@~9=30 ]\n" +
                "17boe age=40 job=null[ V.age@~9=40 ]\n" +
                "17joe age=20 job=null[ V.age@~9=20 ]\n" +
                "17tom age=50 job=null[ V.age@~9=50 ]\n";
        Assert.assertEquals(expected, scanAll(t2, "17a", "17z", null, false));
    }

    @Test
    public void writeScanWithFilter() throws IOException {
        TransactionId t1 = control.beginTransaction();
        insertAge(t1, "91joe", 20);
        insertAge(t1, "91bob", 30);
        insertAge(t1, "91boe", 40);
        insertAge(t1, "91tom", 50);
        control.commit(t1);

        TransactionId t2 = control.beginTransaction();
        String expected = "91boe age=40 job=null[ V.age@~9=40 ]\n";
        if (!useSimple) {
            Assert.assertEquals(expected, scanAll(t2, "91a", "91z", 40, false));
        }
    }

    @Test
    public void writeScanWithFilterAndPendingWrites() throws IOException {
        TransactionId t1 = control.beginTransaction();
        insertAge(t1, "92joe", 20);
        insertAge(t1, "92bob", 30);
        insertAge(t1, "92boe", 40);
        insertAge(t1, "92tom", 50);
        control.commit(t1);

        TransactionId t2 = control.beginTransaction();
        insertAge(t2, "92boe", 41);

        TransactionId t3 = control.beginTransaction();
        String expected = "92boe age=40 job=null[ V.age@~9=40 ]\n";
        if (!useSimple) {
            Assert.assertEquals(expected, scanAll(t3, "92a", "92z", 40, false));
        }
    }

    @Test
    public void writeWriteRead() throws IOException {
        TransactionId t1 = control.beginTransaction();
        insertAge(t1, "joe5", 20);
        Assert.assertEquals("joe5 age=20 job=null", read(t1, "joe5"));
        control.commit(t1);

        TransactionId t2 = control.beginTransaction();
        Assert.assertEquals("joe5 age=20 job=null", read(t2, "joe5"));
        insertJob(t2, "joe5", "baker");
        Assert.assertEquals("joe5 age=20 job=baker", read(t2, "joe5"));
        control.commit(t2);

        TransactionId t3 = control.beginTransaction();
        Assert.assertEquals("joe5 age=20 job=baker", read(t3, "joe5"));
    }

    @Test
    public void multipleWritesSameTransaction() throws IOException {
        TransactionId t1 = control.beginTransaction();
        Assert.assertEquals("joe16 absent", read(t1, "joe16"));
        insertAge(t1, "joe16", 20);
        Assert.assertEquals("joe16 age=20 job=null", read(t1, "joe16"));

        insertAge(t1, "joe16", 21);
        Assert.assertEquals("joe16 age=21 job=null", read(t1, "joe16"));

        insertAge(t1, "joe16", 22);
        Assert.assertEquals("joe16 age=22 job=null", read(t1, "joe16"));
        control.commit(t1);

        TransactionId t2 = control.beginTransaction();
        Assert.assertEquals("joe16 age=22 job=null", read(t2, "joe16"));
        control.commit(t2);
    }

    @Test
    public void manyWritesManyRollbacksRead() throws IOException {
        TransactionId t1 = control.beginTransaction();
        insertAge(t1, "joe6", 20);
        control.commit(t1);

        TransactionId t2 = control.beginTransaction();
        insertJob(t2, "joe6", "baker");
        control.commit(t2);

        TransactionId t3 = control.beginTransaction();
        insertJob(t3, "joe6", "butcher");
        control.commit(t3);

        TransactionId t4 = control.beginTransaction();
        insertJob(t4, "joe6", "blacksmith");
        control.commit(t4);

        TransactionId t5 = control.beginTransaction();
        insertJob(t5, "joe6", "carter");
        control.commit(t5);

        TransactionId t6 = control.beginTransaction();
        insertJob(t6, "joe6", "farrier");
        control.commit(t6);

        TransactionId t7 = control.beginTransaction();
        insertAge(t7, "joe6", 27);
        control.rollback(t7);

        TransactionId t8 = control.beginTransaction();
        insertAge(t8, "joe6", 28);
        control.rollback(t8);

        TransactionId t9 = control.beginTransaction();
        insertAge(t9, "joe6", 29);
        control.rollback(t9);

        TransactionId t10 = control.beginTransaction();
        insertAge(t10, "joe6", 30);
        control.rollback(t10);

        TransactionId t11 = control.beginTransaction();
        Assert.assertEquals("joe6 age=20 job=farrier", read(t11, "joe6"));
    }

    @Test
    public void writeDelete() throws IOException {
        TransactionId t1 = control.beginTransaction();
        insertAge(t1, "joe10", 20);
        Assert.assertEquals("joe10 age=20 job=null", read(t1, "joe10"));
        control.commit(t1);

        TransactionId t2 = control.beginTransaction();
        Assert.assertEquals("joe10 age=20 job=null", read(t2, "joe10"));
        deleteRow(t2, "joe10");
        Assert.assertEquals("joe10 absent", read(t2, "joe10"));
        control.commit(t2);
    }

    @Test
    public void writeDeleteRead() throws IOException {
        TransactionId t1 = control.beginTransaction();
        insertAge(t1, "joe11", 20);
        control.commit(t1);

        TransactionId t2 = control.beginTransaction();
        deleteRow(t2, "joe11");
        control.commit(t2);

        TransactionId t3 = control.beginTransaction();
        Assert.assertEquals("joe11 absent", read(t3, "joe11"));
        control.commit(t3);
    }

    @Test
    public void writeDeleteRollbackRead() throws IOException {
        TransactionId t1 = control.beginTransaction();
        insertAge(t1, "joe90", 20);
        control.commit(t1);

        TransactionId t2 = control.beginTransaction();
        deleteRow(t2, "joe90");
        control.rollback(t2);

        TransactionId t3 = control.beginTransaction();
        Assert.assertEquals("joe90 age=20 job=null", read(t3, "joe90"));
        control.commit(t3);
    }

    @Test
    public void writeChildDeleteParentRollbackDelete() throws IOException {
        TransactionId t1 = control.beginTransaction();
        insertAge(t1, "joe93", 20);
        control.commit(t1);

        TransactionId t2 = control.beginTransaction();
        TransactionId t3 = control.beginChildTransaction(t2, true);
        deleteRow(t3, "joe93");
        control.rollback(t2);

        TransactionId t4 = control.beginTransaction();
        deleteRow(t4, "joe93");
        Assert.assertEquals("joe93 absent", read(t4, "joe93"));
        control.commit(t4);
    }

    @Test
    public void writeDeleteOverlap() throws IOException {
        TransactionId t1 = control.beginTransaction();
        insertAge(t1, "joe2", 20);

        TransactionId t2 = control.beginTransaction();
        try {
            deleteRow(t2, "joe2");
            Assert.fail();
        } catch (WriteConflict e) {
        } catch (RetriesExhaustedWithDetailsException e) {
            assertWriteConflict(e);
        } finally {
            control.fail(t2);
        }
        Assert.assertEquals("joe2 age=20 job=null", read(t1, "joe2"));
        Assert.assertEquals("joe2 age=20 job=null", read(t1, "joe2"));
        control.commit(t1);
        try {
            control.commit(t2);
            Assert.fail();
        } catch (DoNotRetryIOException dnrio) {
            String message = dnrio.getMessage();
            Assert.assertTrue("Incorrect message pattern returned!", message.matches("transaction [0-9]* is not ACTIVE.*"));
        }
    }

    private void assertWriteConflict(RetriesExhaustedWithDetailsException e) {
        Assert.assertEquals(1, e.getNumExceptions());
        Assert.assertTrue(e.getMessage().startsWith("Failed 1 action: com.splicemachine.si.impl.WriteConflict:"));
    }

    private void assertWriteFailed(RetriesExhaustedWithDetailsException e) {
        Assert.assertEquals(1, e.getNumExceptions());
        Assert.assertTrue(e.getMessage().startsWith("commit failed"));
    }

    @Test
    public void writeWriteDeleteOverlap() throws IOException {
        TransactionId t0 = control.beginTransaction();
        insertAge(t0, "joe013", 20);
        control.commit(t0);

        TransactionId t1 = control.beginTransaction();
        deleteRow(t1, "joe013");

        TransactionId t2 = control.beginTransaction();
        try {
            insertAge(t2, "joe013", 21);
            Assert.fail();
        } catch (WriteConflict e) {
        } catch (RetriesExhaustedWithDetailsException e) {
            assertWriteConflict(e);
        } finally {
            control.fail(t2);
        }
        Assert.assertEquals("joe013 absent", read(t1, "joe013"));
        control.commit(t1);
        try {
            control.commit(t2);
            Assert.fail();
        } catch (DoNotRetryIOException dnrio) {
            Assert.assertTrue("Incorrect message returned!", dnrio.getMessage().matches("transaction [0-9]* is not ACTIVE.*"));
        }

        TransactionId t3 = control.beginTransaction();
        Assert.assertEquals("joe013 absent", read(t3, "joe013"));
        control.commit(t3);
    }

    @Test
    public void writeWriteDeleteWriteRead() throws IOException {
        TransactionId t0 = control.beginTransaction();
        insertAge(t0, "joe14", 20);
        control.commit(t0);

        TransactionId t1 = control.beginTransaction();
        insertJob(t1, "joe14", "baker");
        control.commit(t1);

        TransactionId t2 = control.beginTransaction();
        deleteRow(t2, "joe14");
        control.commit(t2);

        TransactionId t3 = control.beginTransaction();
        insertJob(t3, "joe14", "smith");
        Assert.assertEquals("joe14 age=null job=smith", read(t3, "joe14"));
        control.commit(t3);

        TransactionId t4 = control.beginTransaction();
        Assert.assertEquals("joe14 age=null job=smith", read(t4, "joe14"));
        control.commit(t4);
    }

    @Test
    public void writeWriteDeleteWriteDeleteWriteRead() throws IOException {
        TransactionId t0 = control.beginTransaction();
        insertAge(t0, "joe15", 20);
        control.commit(t0);

        TransactionId t1 = control.beginTransaction();
        insertJob(t1, "joe15", "baker");
        control.commit(t1);

        TransactionId t2 = control.beginTransaction();
        deleteRow(t2, "joe15");
        control.commit(t2);

        TransactionId t3 = control.beginTransaction();
        insertJob(t3, "joe15", "smith");
        Assert.assertEquals("joe15 age=null job=smith", read(t3, "joe15"));
        control.commit(t3);

        TransactionId t4 = control.beginTransaction();
        deleteRow(t4, "joe15");
        control.commit(t4);

        TransactionId t5 = control.beginTransaction();
        insertAge(t5, "joe15", 21);
        control.commit(t5);

        TransactionId t6 = control.beginTransaction();
        Assert.assertEquals("joe15 age=21 job=null", read(t6, "joe15"));
        control.commit(t6);
    }

    @Test
    public void writeManyDeleteOneGets() throws IOException {
        TransactionId t1 = control.beginTransaction();
        insertAge(t1, "joe47", 20);
        insertAge(t1, "toe47", 30);
        insertAge(t1, "boe47", 40);
        insertAge(t1, "moe47", 50);
        insertAge(t1, "zoe47", 60);
        control.commit(t1);

        TransactionId t2 = control.beginTransaction();
        deleteRow(t2, "moe47");
        control.commit(t2);

        TransactionId t3 = control.beginTransaction();
        Assert.assertEquals("joe47 age=20 job=null", read(t3, "joe47"));
        Assert.assertEquals("toe47 age=30 job=null", read(t3, "toe47"));
        Assert.assertEquals("boe47 age=40 job=null", read(t3, "boe47"));
        Assert.assertEquals("moe47 absent", read(t3, "moe47"));
        Assert.assertEquals("zoe47 age=60 job=null", read(t3, "zoe47"));
    }

    @Test
    public void writeManyDeleteOneScan() throws IOException {
        TransactionId t1 = control.beginTransaction();
        insertAge(t1, "48joe", 20);
        insertAge(t1, "48toe", 30);
        insertAge(t1, "48boe", 40);
        insertAge(t1, "48moe", 50);
        insertAge(t1, "48xoe", 60);
        control.commit(t1);

        TransactionId t2 = control.beginTransaction();
        deleteRow(t2, "48moe");
        control.commit(t2);

        TransactionId t3 = control.beginTransaction();
        String expected = "48boe age=40 job=null[ V.age@~9=40 ]\n" +
                "48joe age=20 job=null[ V.age@~9=20 ]\n" +
                "48toe age=30 job=null[ V.age@~9=30 ]\n" +
                "48xoe age=60 job=null[ V.age@~9=60 ]\n";
        Assert.assertEquals(expected, scanAll(t3, "48a", "48z", null, false));
    }

    @Test
    public void writeManyDeleteOneScanWithIncludeSIColumn() throws IOException {
        TransactionId t1 = control.beginTransaction();
        insertAge(t1, "110joe", 20);
        insertAge(t1, "110toe", 30);
        insertAge(t1, "110boe", 40);
        insertAge(t1, "110moe", 50);
        insertAge(t1, "110xoe", 60);
        control.commit(t1);

        TransactionId t2 = control.beginTransaction();
        deleteRow(t2, "110moe");
        control.commit(t2);

        TransactionId t3 = control.beginTransaction();
        String expected = "110boe age=40 job=null[ S.TX@~9 V.age@~9=40 ]\n" +
                "110joe age=20 job=null[ S.TX@~9 V.age@~9=20 ]\n" +
                "110toe age=30 job=null[ S.TX@~9 V.age@~9=30 ]\n" +
                "110xoe age=60 job=null[ S.TX@~9 V.age@~9=60 ]\n";
        Assert.assertEquals(expected, scanAll(t3, "110a", "110z", null, true));
    }

    @Test
    public void writeDeleteScanWithIncludeSIColumnAfterRollForward() throws IOException, InterruptedException {
        try {
            Tracer.rollForwardDelayOverride = 200;
            final CountDownLatch latch = makeLatch("140moe");

            TransactionId t1 = control.beginTransaction();
            insertAge(t1, "140moe", 50);
            deleteRow(t1, "140moe");
            control.commit(t1);

            TransactionId t2 = control.beginTransaction();
            String expected = "";
            Assert.assertTrue(latch.await(11, TimeUnit.SECONDS));
            Assert.assertEquals(expected, scanAll(t2, "140a", "140z", null, true));
        } finally {
            Tracer.rollForwardDelayOverride = null;
        }
    }

    @Test
    public void writeManyDeleteOneScanWithIncludeSIColumnSameTransaction() throws IOException {
        TransactionId t1 = control.beginTransaction();
        insertAge(t1, "143joe", 20);
        insertAge(t1, "143toe", 30);
        insertAge(t1, "143boe", 40);
        insertAge(t1, "143moe", 50);
        insertAge(t1, "143xoe", 60);
        control.commit(t1);

        TransactionId t2 = control.beginTransaction();
        deleteRow(t2, "143moe");
        String expected = "143boe age=40 job=null[ S.TX@~9 V.age@~9=40 ]\n" +
                "143joe age=20 job=null[ S.TX@~9 V.age@~9=20 ]\n" +
                "143toe age=30 job=null[ S.TX@~9 V.age@~9=30 ]\n" +
                "143xoe age=60 job=null[ S.TX@~9 V.age@~9=60 ]\n";
        Assert.assertEquals(expected, scanAll(t2, "143a", "143z", null, true));
    }

    @Test
    public void writeManyDeleteOneSameTransactionScanWithIncludeSIColumn() throws IOException {
        TransactionId t1 = control.beginTransaction();
        insertAge(t1, "135joe", 20);
        insertAge(t1, "135toe", 30);
        insertAge(t1, "135boe", 40);
        insertAge(t1, "135xoe", 60);
        control.commit(t1);

        TransactionId t2 = control.beginTransaction();
        insertAge(t1, "135moe", 50);
        deleteRow(t2, "135moe");
        control.commit(t2);

        TransactionId t3 = control.beginTransaction();
        String expected = "135boe age=40 job=null[ S.TX@~9 V.age@~9=40 ]\n" +
                "135joe age=20 job=null[ S.TX@~9 V.age@~9=20 ]\n" +
                "135toe age=30 job=null[ S.TX@~9 V.age@~9=30 ]\n" +
                "135xoe age=60 job=null[ S.TX@~9 V.age@~9=60 ]\n";
        Assert.assertEquals(expected, scanAll(t3, "135a", "135z", null, true));
    }

    @Test
    public void writeManyDeleteOneAllNullsScanWithIncludeSIColumn() throws IOException {
        TransactionId t1 = control.beginTransaction();
        insertAge(t1, "137joe", 20);
        insertAge(t1, "137toe", 30);
        insertAge(t1, "137boe", 40);
        insertAge(t1, "137moe", null);
        insertAge(t1, "137xoe", 60);
        control.commit(t1);

        TransactionId t2 = control.beginTransaction();
        deleteRow(t2, "137moe");
        control.commit(t2);

        TransactionId t3 = control.beginTransaction();
        String expected = "137boe age=40 job=null[ S.TX@~9 V.age@~9=40 ]\n" +
                "137joe age=20 job=null[ S.TX@~9 V.age@~9=20 ]\n" +
                "137toe age=30 job=null[ S.TX@~9 V.age@~9=30 ]\n" +
                "137xoe age=60 job=null[ S.TX@~9 V.age@~9=60 ]\n";
        Assert.assertEquals(expected, scanAll(t3, "137a", "137z", null, true));
    }

    @Test
    public void writeManyDeleteOneAllNullsSameTransactionScanWithIncludeSIColumn() throws IOException {
        TransactionId t1 = control.beginTransaction();
        insertAge(t1, "138joe", 20);
        insertAge(t1, "138toe", 30);
        insertAge(t1, "138boe", 40);
        insertAge(t1, "138xoe", 60);
        control.commit(t1);

        TransactionId t2 = control.beginTransaction();
        insertAge(t2, "138moe", null);
        deleteRow(t2, "138moe");
        control.commit(t2);

        TransactionId t3 = control.beginTransaction();
        String expected = "138boe age=40 job=null[ S.TX@~9 V.age@~9=40 ]\n" +
                "138joe age=20 job=null[ S.TX@~9 V.age@~9=20 ]\n" +
                "138toe age=30 job=null[ S.TX@~9 V.age@~9=30 ]\n" +
                "138xoe age=60 job=null[ S.TX@~9 V.age@~9=60 ]\n";
        Assert.assertEquals(expected, scanAll(t3, "138a", "138z", null, true));
    }

    @Test
    public void writeManyDeleteOneBeforeWriteSameTransactionAsWriteScanWithIncludeSIColumn() throws IOException {
        TransactionId t1 = control.beginTransaction();
        insertAge(t1, "136joe", 20);
        insertAge(t1, "136toe", 30);
        insertAge(t1, "136boe", 40);
        insertAge(t1, "136moe", 50);
        insertAge(t1, "136xoe", 60);
        control.commit(t1);

        TransactionId t2 = control.beginTransaction();
        deleteRow(t2, "136moe");
        insertAge(t2, "136moe", 51);
        control.commit(t2);

        TransactionId t3 = control.beginTransaction();
        String expected = "136boe age=40 job=null[ S.TX@~9 V.age@~9=40 ]\n" +
                "136joe age=20 job=null[ S.TX@~9 V.age@~9=20 ]\n" +
                "136moe age=51 job=null[ S.TX@~9 V.age@~9=51 ]\n" +
                "136toe age=30 job=null[ S.TX@~9 V.age@~9=30 ]\n" +
                "136xoe age=60 job=null[ S.TX@~9 V.age@~9=60 ]\n";
        Assert.assertEquals(expected, scanAll(t3, "136a", "136z", null, true));
    }

    @Test
    public void writeManyDeleteOneBeforeWriteAllNullsSameTransactionAsWriteScanWithIncludeSIColumn() throws IOException {
        TransactionId t1 = control.beginTransaction();
        insertAge(t1, "139joe", 20);
        insertAge(t1, "139toe", 30);
        insertAge(t1, "139boe", 40);
        insertAge(t1, "139moe", 50);
        insertAge(t1, "139xoe", 60);
        control.commit(t1);

        TransactionId t2 = control.beginTransaction();
        deleteRow(t2, "139moe");
        insertAge(t2, "139moe", null);
        control.commit(t2);

        TransactionId t3 = control.beginTransaction();
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
        TransactionId t1 = control.beginTransaction();
        insertAge(t1, "112joe", 20);
        insertAge(t1, "112toe", 30);
        insertAge(t1, "112boe", null);
        insertAge(t1, "112moe", 50);
        insertAge(t1, "112xoe", 60);
        control.commit(t1);

        TransactionId t2 = control.beginTransaction();
        deleteRow(t2, "112moe");
        control.commit(t2);

        TransactionId t3 = control.beginTransaction();
        String expected = "112joe age=20 job=null[ V.age@~9=20 ]\n" +
                "112toe age=30 job=null[ V.age@~9=30 ]\n" +
                "112xoe age=60 job=null[ V.age@~9=60 ]\n";
        Assert.assertEquals(expected, scanAll(t3, "112a", "112z", null, false));
    }

    @Test
    public void writeManyWithOneAllNullsDeleteOneScanWithIncludeSIColumn() throws IOException {
        TransactionId t1 = control.beginTransaction();
        insertAge(t1, "111joe", 20);
        insertAge(t1, "111toe", 30);
        insertAge(t1, "111boe", null);
        insertAge(t1, "111moe", 50);
        insertAge(t1, "111xoe", 60);
        control.commit(t1);

        TransactionId t2 = control.beginTransaction();
        deleteRow(t2, "111moe");
        control.commit(t2);

        TransactionId t3 = control.beginTransaction();
        String expected = "111boe age=null job=null[ S.TX@~9 ]\n" +
                "111joe age=20 job=null[ S.TX@~9 V.age@~9=20 ]\n" +
                "111toe age=30 job=null[ S.TX@~9 V.age@~9=30 ]\n" +
                "111xoe age=60 job=null[ S.TX@~9 V.age@~9=60 ]\n";
        Assert.assertEquals(expected, scanAll(t3, "111a", "111z", null, true));
    }

    @Test
    public void writeDeleteSameTransaction() throws IOException {
        TransactionId t0 = control.beginTransaction();
        insertAge(t0, "joe81", 19);
        control.commit(t0);

        TransactionId t1 = control.beginTransaction();
        insertAge(t1, "joe81", 20);
        deleteRow(t1, "joe81");
        Assert.assertEquals("joe81 absent", read(t1, "joe81"));
        control.commit(t1);

        TransactionId t2 = control.beginTransaction();
        Assert.assertEquals("joe81 absent", read(t2, "joe81"));
        control.commit(t2);
    }

    @Test
    public void deleteWriteSameTransaction() throws IOException {
        TransactionId t0 = control.beginTransaction();
        insertAge(t0, "joe82", 19);
        control.commit(t0);

        TransactionId t1 = control.beginTransaction();
        deleteRow(t1, "joe82");
        insertAge(t1, "joe82", 20);
        Assert.assertEquals("joe82 age=20 job=null", read(t1, "joe82"));
        control.commit(t1);

        TransactionId t2 = control.beginTransaction();
        Assert.assertEquals("joe82 age=20 job=null", read(t2, "joe82"));
        control.commit(t2);
    }

    @Test
    public void fourTransactions() throws Exception {
        TransactionId t1 = control.beginTransaction();
        insertAge(t1, "joe7", 20);
        control.commit(t1);

        TransactionId t2 = control.beginTransaction();
        Assert.assertEquals("joe7 age=20 job=null", read(t2, "joe7"));
        insertAge(t2, "joe7", 30);
        Assert.assertEquals("joe7 age=30 job=null", read(t2, "joe7"));

        TransactionId t3 = control.beginTransaction();
        Assert.assertEquals("joe7 age=20 job=null", read(t3, "joe7"));

        control.commit(t2);

        TransactionId t4 = control.beginTransaction();
        Assert.assertEquals("joe7 age=30 job=null", read(t4, "joe7"));
    }

    @Test
    public void writeReadOnly() throws IOException {
        TransactionId t1 = control.beginTransaction();
        insertAge(t1, "joe18", 20);
        control.commit(t1);

        TransactionId t2 = control.beginTransaction(false);
        Assert.assertEquals("joe18 age=20 job=null", read(t2, "joe18"));
        try {
            insertAge(t2, "joe18", 21);
            Assert.fail("expected exception performing a write on a read-only transaction");
        } catch (IOException e) {
        }
    }

    @Test
    public void writeReadCommitted() throws IOException {
        TransactionId t1 = control.beginTransaction();
        insertAge(t1, "joe19", 20);
        control.commit(t1);

        TransactionId t2 = control.beginTransaction(false, false, true);
        Assert.assertEquals("joe19 age=20 job=null", read(t2, "joe19"));
    }

    @Test
    public void writeReadCommittedOverlap() throws IOException {
        TransactionId t1 = control.beginTransaction();
        insertAge(t1, "joe20", 20);

        TransactionId t2 = control.beginTransaction(false, false, true);

        Assert.assertEquals("joe20 absent", read(t2, "joe20"));
        control.commit(t1);
        Assert.assertEquals("joe20 age=20 job=null", read(t2, "joe20"));
    }

    @Test
    public void writeReadDirty() throws IOException {
        TransactionId t1 = control.beginTransaction();
        insertAge(t1, "joe22", 20);
        control.commit(t1);

        TransactionId t2 = control.beginTransaction(false, true, true);
        Assert.assertEquals("joe22 age=20 job=null", read(t2, "joe22"));
    }

    @Test
    public void writeReadDirtyOverlap() throws IOException {
        TransactionId t1 = control.beginTransaction();
        insertAge(t1, "joe21", 20);

        TransactionId t2 = control.beginTransaction(false, true, true);

        Assert.assertEquals("joe21 age=20 job=null", read(t2, "joe21"));
        control.commit(t1);
        Assert.assertEquals("joe21 age=20 job=null", read(t2, "joe21"));
    }

    @Test
    public void writeRollbackWriteReadDirtyOverlap() throws IOException {
        TransactionId t1 = control.beginTransaction();
        insertAge(t1, "joe23", 20);
        control.commit(t1);

        TransactionId t2 = control.beginTransaction();
        insertAge(t2, "joe23", 21);

        TransactionId t3 = control.beginTransaction(false, true, true);
        Assert.assertEquals("joe23 age=21 job=null", read(t3, "joe23"));

        control.rollback(t2);
        Assert.assertEquals("joe23 age=20 job=null", read(t3, "joe23"));

        TransactionId t4 = control.beginTransaction();
        insertAge(t4, "joe23", 22);
        Assert.assertEquals("joe23 age=22 job=null", read(t3, "joe23"));
    }

    @Test
    public void childDependentTransactionWriteRollbackRead() throws IOException {
        TransactionId t0 = control.beginTransaction();
        insertAge(t0, "joe24", 19);
        control.commit(t0);
        TransactionId t1 = control.beginTransaction();
        insertAge(t1, "joe24", 20);
        TransactionId t2 = control.beginChildTransaction(t1, true);
        insertAge(t2, "moe24", 21);
        Assert.assertEquals("joe24 age=20 job=null", read(t1, "joe24"));
        Assert.assertEquals("moe24 absent", read(t1, "moe24"));
        control.rollback(t2);
        Assert.assertEquals("joe24 age=20 job=null", read(t1, "joe24"));
        Assert.assertEquals("moe24 absent", read(t1, "moe24"));
        control.commit(t1);

        TransactionId t3 = control.beginTransaction(false);
        Assert.assertEquals("joe24 age=20 job=null", read(t3, "joe24"));
        Assert.assertEquals("moe24 absent", read(t3, "moe24"));
    }

    @Test
    public void childDependentTransactionWriteCommitRollbackRead() throws IOException {
        TransactionId t1 = control.beginTransaction();
        TransactionId t2 = control.beginChildTransaction(t1, true);
        insertAge(t2, "joe51", 21);
        control.commit(t2);
        control.rollback(t2);
        Assert.assertEquals("joe51 age=21 job=null", read(t1, "joe51"));
        control.commit(t1);
    }

    @Test
    public void childDependentSeesParentWrites() throws IOException {
        TransactionId t1 = control.beginTransaction();
        insertAge(t1, "joe40", 20);
        TransactionId t2 = control.beginChildTransaction(t1, true);
        Assert.assertEquals("joe40 age=20 job=null", read(t2, "joe40"));
    }

    @Test
    public void childDependentTransactionWriteRead() throws IOException {
        TransactionId t1 = control.beginTransaction();
        insertAge(t1, "joe25", 20);
        TransactionId t2 = control.beginChildTransaction(t1, true);
        insertAge(t2, "moe25", 21);
        Assert.assertEquals("joe25 age=20 job=null", read(t1, "joe25"));
        Assert.assertEquals("moe25 absent", read(t1, "moe25"));
        control.commit(t2);

        TransactionId t3 = control.beginTransaction(false);
        Assert.assertEquals("joe25 absent", read(t3, "joe25"));
        Assert.assertEquals("moe25 absent", read(t3, "moe25"));

        Assert.assertEquals("joe25 age=20 job=null", read(t1, "joe25"));
        Assert.assertEquals("moe25 age=21 job=null", read(t1, "moe25"));
        control.commit(t1);

        TransactionId t4 = control.beginTransaction(false);
        Assert.assertEquals("joe25 age=20 job=null", read(t4, "joe25"));
        Assert.assertEquals("moe25 age=21 job=null", read(t4, "moe25"));
    }

    @Test
    public void childDependentTransactionWithOtherCommitBetweenParentAndChild() throws IOException {
        TransactionId t0 = control.beginTransaction();
        insertAge(t0, "joe37", 20);
        control.commit(t0);

        TransactionId t1 = control.beginTransaction();

        TransactionId otherTransaction = control.beginTransaction();
        insertAge(otherTransaction, "joe37", 30);
        control.commit(otherTransaction);

        TransactionId t2 = control.beginChildTransaction(t1, true);
        Assert.assertEquals("joe37 age=20 job=null", read(t2, "joe37"));
        control.commit(t2);
        control.commit(t1);
    }

    @Test
    public void multipleChildDependentTransactionWriteRead() throws IOException {
        TransactionId t1 = control.beginTransaction();
        insertAge(t1, "joe26", 20);
        TransactionId t2 = control.beginChildTransaction(t1, true);
        TransactionId t3 = control.beginChildTransaction(t1, true);
        insertAge(t2, "moe26", 21);
        insertJob(t3, "boe26", "baker");
        Assert.assertEquals("joe26 age=20 job=null", read(t1, "joe26"));
        Assert.assertEquals("moe26 absent", read(t1, "moe26"));
        Assert.assertEquals("boe26 absent", read(t1, "boe26"));
        control.commit(t2);

        TransactionId t4 = control.beginTransaction(false);
        Assert.assertEquals("joe26 absent", read(t4, "joe26"));
        Assert.assertEquals("moe26 absent", read(t4, "moe26"));
        Assert.assertEquals("boe26 absent", read(t4, "boe26"));

        Assert.assertEquals("joe26 age=20 job=null", read(t1, "joe26"));
        Assert.assertEquals("moe26 age=21 job=null", read(t1, "moe26"));
        Assert.assertEquals("boe26 absent", read(t1, "boe26"));
        control.commit(t3);

        TransactionId t5 = control.beginTransaction(false);
        Assert.assertEquals("joe26 absent", read(t5, "joe26"));
        Assert.assertEquals("moe26 absent", read(t5, "moe26"));
        Assert.assertEquals("boe26 absent", read(t5, "boe26"));

        Assert.assertEquals("joe26 age=20 job=null", read(t1, "joe26"));
        Assert.assertEquals("moe26 age=21 job=null", read(t1, "moe26"));
        Assert.assertEquals("boe26 age=null job=baker", read(t1, "boe26"));
        control.commit(t1);

        TransactionId t6 = control.beginTransaction(false);
        Assert.assertEquals("joe26 age=20 job=null", read(t6, "joe26"));
        Assert.assertEquals("moe26 age=21 job=null", read(t6, "moe26"));
        Assert.assertEquals("boe26 age=null job=baker", read(t6, "boe26"));
    }

    @Test
    public void multipleChildDependentTransactionsRollbackThenWrite() throws IOException {
        TransactionId t1 = control.beginTransaction();
        insertAge(t1, "joe45", 20);
        insertAge(t1, "boe45", 19);
        TransactionId t2 = control.beginChildTransaction(t1, true, true);
        insertAge(t2, "joe45", 21);
        TransactionId t3 = control.beginChildTransaction(t1, true, true);
        insertJob(t3, "boe45", "baker");
        Assert.assertEquals("joe45 age=20 job=null", read(t1, "joe45"));
        Assert.assertEquals("joe45 age=21 job=null", read(t2, "joe45"));
        Assert.assertEquals("joe45 age=20 job=null", read(t3, "joe45"));
        control.rollback(t2);
        Assert.assertEquals("joe45 age=20 job=null", read(t1, "joe45"));
        Assert.assertEquals("joe45 age=20 job=null", read(t3, "joe45"));
        Assert.assertEquals("boe45 age=19 job=null", read(t1, "boe45"));
        Assert.assertEquals("boe45 age=19 job=baker", read(t3, "boe45"));
        control.rollback(t3);
        Assert.assertEquals("joe45 age=20 job=null", read(t1, "joe45"));
        TransactionId t4 = control.beginChildTransaction(t1, true, true);
        insertAge(t4, "joe45", 24);
        Assert.assertEquals("joe45 age=20 job=null", read(t1, "joe45"));
        Assert.assertEquals("joe45 age=24 job=null", read(t4, "joe45"));
        control.commit(t4);
        Assert.assertEquals("joe45 age=24 job=null", read(t1, "joe45"));
        Assert.assertEquals("joe45 age=24 job=null", read(t4, "joe45"));

        TransactionId t5 = control.beginTransaction(false, false, true);
        Assert.assertEquals("joe45 absent", read(t5, "joe45"));
        Assert.assertEquals("boe45 absent", read(t5, "boe45"));
        control.commit(t1);
        Assert.assertEquals("joe45 age=24 job=null", read(t5, "joe45"));
        Assert.assertEquals("boe45 age=19 job=null", read(t5, "boe45"));
    }

    @Test
    public void multipleChildCommitParentRollback() throws IOException {
        TransactionId t1 = control.beginTransaction();
        insertAge(t1, "joe46", 20);
        TransactionId t2 = control.beginChildTransaction(t1, true);
        insertJob(t2, "moe46", "baker");
        control.commit(t2);
        control.rollback(t1);
        // @TODO: it should not be possible to start a child on a rolled back transaction
        TransactionId t3 = control.beginChildTransaction(t1, true);
        Assert.assertEquals("joe46 absent", read(t3, "joe46"));
        Assert.assertEquals("moe46 age=null job=baker", read(t3, "moe46"));
    }

    @Test
    public void childDependentTransactionWriteRollbackParentRead() throws IOException {
        TransactionId t1 = control.beginTransaction();
        insertAge(t1, "joe27", 20);
        TransactionId t2 = control.beginChildTransaction(t1, true);
        insertAge(t2, "moe27", 21);
        control.commit(t2);
        control.rollback(t1);

        TransactionId t4 = control.beginTransaction(false);
        Assert.assertEquals("joe27 absent", read(t4, "joe27"));
        Assert.assertEquals("moe27 absent", read(t4, "moe27"));
    }

    @Test
    public void commitParentOfCommittedDependent() throws IOException {
        TransactionId t1 = control.beginTransaction();
        insertAge(t1, "joe32", 20);
        TransactionId t2 = control.beginChildTransaction(t1, true);
        insertAge(t2, "moe32", 21);
        control.commit(t2);
        final Transaction transactionStatusA = transactorSetup.transactionStore.getTransaction(t2);
        Assert.assertEquals("committing a dependent child sets a local commit timestamp", 2L, (long) transactionStatusA.getCommitTimestampDirect());
        Assert.assertNull(transactionStatusA.getEffectiveCommitTimestamp());
        control.commit(t1);
        final Transaction transactionStatusB = transactorSetup.transactionStore.getTransaction(t2);
        Assert.assertEquals("committing parent of dependent transaction should not change the commit time of the child", 2L, (long) transactionStatusB.getCommitTimestampDirect());
        Assert.assertNotNull(transactionStatusB.getEffectiveCommitTimestamp());
    }

    @Test
    public void dependentWriteFollowedByReadCommittedWriter() throws IOException {
        TransactionId parent = control.beginTransaction();

        TransactionId child = control.beginChildTransaction(parent, true);
        insertAge(child, "joe34", 22);
        control.commit(child);

        TransactionId other = control.beginTransaction(true, false, true);
        try {
            insertAge(other, "joe34", 21);
            Assert.fail();
        } catch (WriteConflict e) {
        } catch (RetriesExhaustedWithDetailsException e) {
            assertWriteConflict(e);
        } finally {
            control.fail(other);
        }
    }

    @Test
    public void dependentWriteCommitParentFollowedByReadCommittedWriter() throws IOException {
        TransactionId parent = control.beginTransaction();

        TransactionId child = control.beginChildTransaction(parent, true);
        insertAge(child, "joe94", 22);
        control.commit(child);
        control.commit(parent);

        TransactionId other = control.beginTransaction(true, false, true);
        insertAge(other, "joe94", 21);
        control.commit(other);
    }

    @Test
    public void dependentWriteOverlapWithReadCommittedWriter() throws IOException {
        TransactionId parent = control.beginTransaction();

        TransactionId other = control.beginTransaction(true, false, true);

        TransactionId child = control.beginChildTransaction(parent, true);
        insertAge(child, "joe36", 22);
        control.commit(child);

        try {
            insertAge(other, "joe36", 21);
            Assert.fail();
        } catch (WriteConflict e) {
        } catch (RetriesExhaustedWithDetailsException e) {
            assertWriteConflict(e);
        } finally {
            control.fail(other);
        }
    }

    @Test
    public void rollbackUpdate() throws IOException {
        TransactionId t1 = control.beginTransaction();
        insertAge(t1, "joe43", 20);
        control.commit(t1);

        TransactionId t2 = control.beginTransaction();
        insertAge(t2, "joe43", 21);
        control.rollback(t2);

        TransactionId t3 = control.beginTransaction();
        Assert.assertEquals("joe43 age=20 job=null", read(t3, "joe43"));
    }

    @Test
    public void rollbackInsert() throws IOException {
        TransactionId t1 = control.beginTransaction();
        insertAge(t1, "joe44", 20);
        control.rollback(t1);

        TransactionId t2 = control.beginTransaction();
        Assert.assertEquals("joe44 absent", read(t2, "joe44"));
    }

    @Test
    public void childrenOfChildrenCommitCommitCommit() throws IOException {
        TransactionId t1 = control.beginTransaction();
        TransactionId t2 = control.beginChildTransaction(t1, true);
        TransactionId t3 = control.beginChildTransaction(t2, true);
        insertAge(t3, "joe53", 20);
        Assert.assertEquals("joe53 age=20 job=null", read(t3, "joe53"));
        control.commit(t3);
        Assert.assertEquals("joe53 age=20 job=null", read(t3, "joe53"));
        Assert.assertEquals("joe53 age=20 job=null", read(t2, "joe53"));
        insertAge(t2, "boe53", 21);
        Assert.assertEquals("boe53 age=21 job=null", read(t2, "boe53"));
        control.commit(t2);
        Assert.assertEquals("joe53 age=20 job=null", read(t1, "joe53"));
        Assert.assertEquals("boe53 age=21 job=null", read(t1, "boe53"));
        control.commit(t1);
        TransactionId t4 = control.beginTransaction(false);
        Assert.assertEquals("joe53 age=20 job=null", read(t4, "joe53"));
        Assert.assertEquals("boe53 age=21 job=null", read(t4, "boe53"));
    }

    @Test
    public void childrenOfChildrenCommitCommitCommitParentWriteFirst() throws IOException {
        TransactionId t1 = control.beginTransaction();
        TransactionId t2 = control.beginChildTransaction(t1, true, true);
        TransactionId t3 = control.beginChildTransaction(t2, false, true);
        insertAge(t1, "joe57", 18);
        insertAge(t1, "boe57", 19);
        insertAge(t2, "boe57", 21);
        insertAge(t3, "joe57", 20);
        Assert.assertEquals("joe57 age=20 job=null", read(t3, "joe57"));
        control.commit(t3);
        Assert.assertEquals("joe57 age=20 job=null", read(t3, "joe57"));
        Assert.assertEquals("joe57 age=20 job=null", read(t2, "joe57"));
        Assert.assertEquals("boe57 age=21 job=null", read(t2, "boe57"));
        control.commit(t2);
        Assert.assertEquals("joe57 age=20 job=null", read(t1, "joe57"));
        Assert.assertEquals("boe57 age=21 job=null", read(t1, "boe57"));
        control.commit(t1);
        TransactionId t4 = control.beginTransaction(false);
        Assert.assertEquals("joe57 age=20 job=null", read(t4, "joe57"));
        Assert.assertEquals("boe57 age=21 job=null", read(t4, "boe57"));
    }

    @Test
    public void childrenOfChildrenCommitCommitRollback() throws IOException {
        TransactionId t1 = control.beginTransaction();
        TransactionId t2 = control.beginChildTransaction(t1, true);
        TransactionId t3 = control.beginChildTransaction(t2, true);
        insertAge(t3, "joe54", 20);
        Assert.assertEquals("joe54 age=20 job=null", read(t3, "joe54"));
        control.rollback(t3);
        Assert.assertEquals("joe54 absent", read(t2, "joe54"));
        insertAge(t2, "boe54", 21);
        Assert.assertEquals("boe54 age=21 job=null", read(t2, "boe54"));
        control.commit(t2);
        Assert.assertEquals("joe54 absent", read(t1, "joe54"));
        Assert.assertEquals("boe54 age=21 job=null", read(t1, "boe54"));
        control.commit(t1);
        TransactionId t4 = control.beginTransaction(false);
        Assert.assertEquals("joe54 absent", read(t4, "joe54"));
        Assert.assertEquals("boe54 age=21 job=null", read(t4, "boe54"));
    }

    @Test
    public void childrenOfChildrenWritesDoNotConflict() throws IOException {
        TransactionId t1 = control.beginTransaction();
        TransactionId t2 = control.beginChildTransaction(t1, true);
        TransactionId t3 = control.beginChildTransaction(t2, true);
        insertAge(t1, "joe95", 18);
        insertAge(t3, "joe95", 20);
        Assert.assertEquals("joe95 age=18 job=null", read(t1, "joe95"));
        Assert.assertEquals("joe95 age=18 job=null", read(t2, "joe95"));
        Assert.assertEquals("joe95 age=20 job=null", read(t3, "joe95"));
    }

    @Test
    public void parentWritesDoNotConflictWithPriorChildWrites() throws IOException {
        TransactionId t1 = control.beginTransaction();
        TransactionId t2 = control.beginChildTransaction(t1, true);
        insertAge(t2, "joe101", 20);
        control.commit(t2);
        insertAge(t1, "joe101", 21);
        Assert.assertEquals("joe101 age=21 job=null", read(t1, "joe101"));
    }

    @Test
    public void parentWritesDoNotConflictWithPriorChildDelete() throws IOException {
        TransactionId t1 = control.beginTransaction();
        TransactionId t2 = control.beginChildTransaction(t1, true);
        deleteRow(t2, "joe105");
        control.commit(t2);
        insertAge(t1, "joe105", 21);
        Assert.assertEquals("joe105 age=21 job=null", read(t1, "joe105"));
    }

    @Test
    public void parentWritesDoNotConflictWithPriorChildDelete2() throws IOException {
        TransactionId t0 = control.beginTransaction();
        insertAge(t0, "joe141", 20);
        control.commit(t0);
        TransactionId t1 = control.beginTransaction();
        TransactionId t2 = control.beginChildTransaction(t1, true);
        deleteRow(t2, "joe141");
        control.commit(t2);
        insertAge(t1, "joe141", 21);
        Assert.assertEquals("joe141 age=21 job=null", read(t1, "joe141"));
    }

    @Test
    public void parentDeleteDoesNotConflictWithPriorChildDelete() throws IOException {
        TransactionId t1 = control.beginTransaction();
        TransactionId t2 = control.beginChildTransaction(t1, true);
        deleteRow(t2, "joe109");
        control.commit(t2);
        deleteRow(t1, "joe109");
        Assert.assertEquals("joe109 absent", read(t1, "joe109"));
        insertAge(t1, "joe109", 21);
        Assert.assertEquals("joe109 age=21 job=null", read(t1, "joe109"));
    }

    @Test
    public void parentWritesDoNotConflictWithPriorActiveChildWrites() throws IOException {
        TransactionId t1 = control.beginTransaction();
        TransactionId t2 = control.beginChildTransaction(t1, true);
        insertAge(t2, "joe102", 20);
        insertAge(t1, "joe102", 21);
        Assert.assertEquals("joe102 age=21 job=null", read(t1, "joe102"));
        control.commit(t2);
        Assert.assertEquals("joe102 age=21 job=null", read(t1, "joe102"));
    }

    @Test
    public void parentWritesDoNotConflictWithPriorActiveChildDelete() throws IOException {
        TransactionId t1 = control.beginTransaction();
        TransactionId t2 = control.beginChildTransaction(t1, true);
        deleteRow(t2, "joe106");
        insertAge(t1, "joe106", 21);
        Assert.assertEquals("joe106 age=21 job=null", read(t1, "joe106"));
        control.commit(t2);
        Assert.assertEquals("joe106 age=21 job=null", read(t1, "joe106"));
    }

    @Test
    public void parentWritesDoNotConflictWithPriorIndependentChildWrites() throws IOException {
        TransactionId t1 = control.beginTransaction();
        TransactionId t2 = control.beginChildTransaction(t1, false, true);
        insertAge(t2, "joe103", 20);
        control.commit(t2);
        insertAge(t1, "joe103", 21);
        Assert.assertEquals("joe103 age=21 job=null", read(t1, "joe103"));
    }

    @Test
    public void parentWritesDoNotConflictWithPriorIndependentChildDelete() throws IOException {
        TransactionId t1 = control.beginTransaction();
        TransactionId t2 = control.beginChildTransaction(t1, false, true);
        deleteRow(t2, "joe107");
        control.commit(t2);
        insertAge(t1, "joe107", 21);
        Assert.assertEquals("joe107 age=21 job=null", read(t1, "joe107"));
    }

    @Test
    public void parentWritesDoNotConflictWithPriorActiveIndependentChildWrites() throws IOException {
        TransactionId t1 = control.beginTransaction();
        TransactionId t2 = control.beginChildTransaction(t1, false, true);
        insertAge(t2, "joe104", 20);
        insertAge(t1, "joe104", 21);
        Assert.assertEquals("joe104 age=21 job=null", read(t1, "joe104"));
        control.commit(t2);
        Assert.assertEquals("joe104 age=21 job=null", read(t1, "joe104"));
    }

    @Test
    public void parentWritesDoNotConflictWithPriorActiveIndependentChildDelete() throws IOException {
        TransactionId t1 = control.beginTransaction();
        TransactionId t2 = control.beginChildTransaction(t1, false, true);
        deleteRow(t2, "joe108");
        insertAge(t1, "joe108", 21);
        Assert.assertEquals("joe108 age=21 job=null", read(t1, "joe108"));
        control.commit(t2);
        Assert.assertEquals("joe108 age=21 job=null", read(t1, "joe108"));
    }

    @Test
    public void childrenOfChildrenCommitCommitRollbackParentWriteFirst() throws IOException {
        TransactionId t1 = control.beginTransaction();
        TransactionId t2 = control.beginChildTransaction(t1, true, true);
        TransactionId t3 = control.beginChildTransaction(t2, false, true);
        insertAge(t1, "joe58", 18);
        insertAge(t1, "boe58", 19);
        insertAge(t2, "boe58", 21);
        insertAge(t3, "joe58", 20);
        Assert.assertEquals("joe58 age=20 job=null", read(t3, "joe58"));
        control.rollback(t3);
        Assert.assertEquals("joe58 age=18 job=null", read(t2, "joe58"));
        Assert.assertEquals("boe58 age=21 job=null", read(t2, "boe58"));
        control.commit(t2);
        Assert.assertEquals("joe58 age=18 job=null", read(t1, "joe58"));
        Assert.assertEquals("boe58 age=21 job=null", read(t1, "boe58"));
        control.commit(t1);
        TransactionId t4 = control.beginTransaction(false);
        Assert.assertEquals("joe58 age=18 job=null", read(t4, "joe58"));
        Assert.assertEquals("boe58 age=21 job=null", read(t4, "boe58"));
    }

    @Test
    public void childrenOfChildrenCommitRollbackCommit() throws IOException {
        TransactionId t1 = control.beginTransaction();
        TransactionId t2 = control.beginChildTransaction(t1, true);
        TransactionId t3 = control.beginChildTransaction(t2, true);
        insertAge(t3, "joe55", 20);
        Assert.assertEquals("joe55 age=20 job=null", read(t3, "joe55"));
        control.commit(t3);
        Assert.assertEquals("joe55 age=20 job=null", read(t3, "joe55"));
        Assert.assertEquals("joe55 age=20 job=null", read(t2, "joe55"));
        insertAge(t2, "boe55", 21);
        Assert.assertEquals("boe55 age=21 job=null", read(t2, "boe55"));
        control.rollback(t2);
        Assert.assertEquals("joe55 absent", read(t1, "joe55"));
        Assert.assertEquals("boe55 absent", read(t1, "boe55"));
        control.commit(t1);
        TransactionId t4 = control.beginTransaction(false);
        Assert.assertEquals("joe55 absent", read(t4, "joe55"));
        Assert.assertEquals("boe55 absent", read(t4, "boe55"));
    }

    @Test
    public void childrenOfChildrenCommitRollbackCommitParentWriteFirst() throws IOException {
        TransactionId t1 = control.beginTransaction();
        TransactionId t2 = control.beginChildTransaction(t1, true);
        TransactionId t3 = control.beginChildTransaction(t2, true);
        insertAge(t1, "joe59", 18);
        insertAge(t1, "boe59", 19);
        insertAge(t2, "boe59", 21);
        insertAge(t3, "joe59", 20);
        Assert.assertEquals("joe59 age=20 job=null", read(t3, "joe59"));
        control.commit(t3);
        Assert.assertEquals("joe59 age=20 job=null", read(t2, "joe59"));
        Assert.assertEquals("boe59 age=21 job=null", read(t2, "boe59"));
        control.rollback(t2);
        Assert.assertEquals("joe59 age=18 job=null", read(t1, "joe59"));
        Assert.assertEquals("boe59 age=19 job=null", read(t1, "boe59"));
        control.commit(t1);
        TransactionId t4 = control.beginTransaction(false);
        Assert.assertEquals("joe59 age=18 job=null", read(t4, "joe59"));
        Assert.assertEquals("boe59 age=19 job=null", read(t4, "boe59"));
    }

    @Test
    public void childrenOfChildrenRollbackCommitCommitParentWriteFirst() throws IOException {
        TransactionId t1 = control.beginTransaction();
        TransactionId t2 = control.beginChildTransaction(t1, true);
        TransactionId t3 = control.beginChildTransaction(t2, true);
        insertAge(t1, "joe60", 18);
        insertAge(t1, "boe60", 19);
        insertAge(t2, "doe60", 21);
        insertAge(t3, "moe60", 30);
        Assert.assertEquals("moe60 age=30 job=null", read(t3, "moe60"));
        control.commit(t3);
        Assert.assertEquals("moe60 age=30 job=null", read(t3, "moe60"));
        Assert.assertEquals("doe60 age=21 job=null", read(t2, "doe60"));
        insertAge(t2, "doe60", 22);
        Assert.assertEquals("doe60 age=22 job=null", read(t2, "doe60"));
        control.commit(t2);
        Assert.assertEquals("joe60 age=18 job=null", read(t1, "joe60"));
        Assert.assertEquals("boe60 age=19 job=null", read(t1, "boe60"));
        Assert.assertEquals("moe60 age=30 job=null", read(t1, "moe60"));
        Assert.assertEquals("doe60 age=22 job=null", read(t1, "doe60"));
        control.rollback(t1);
        TransactionId t4 = control.beginTransaction(false);
        Assert.assertEquals("joe60 absent", read(t4, "joe60"));
        Assert.assertEquals("boe60 absent", read(t4, "boe60"));
    }

    @Test
    public void childrenOfChildrenRollbackCommitCommit() throws IOException {
        TransactionId t1 = control.beginTransaction();
        TransactionId t2 = control.beginChildTransaction(t1, true);
        TransactionId t3 = control.beginChildTransaction(t2, true);
        insertAge(t3, "joe56", 20);
        Assert.assertEquals("joe56 age=20 job=null", read(t3, "joe56"));
        control.commit(t3);
        Assert.assertEquals("joe56 age=20 job=null", read(t3, "joe56"));
        Assert.assertEquals("joe56 age=20 job=null", read(t2, "joe56"));
        insertAge(t2, "boe56", 21);
        Assert.assertEquals("boe56 age=21 job=null", read(t2, "boe56"));
        control.commit(t2);
        Assert.assertEquals("joe56 age=20 job=null", read(t1, "joe56"));
        Assert.assertEquals("boe56 age=21 job=null", read(t1, "boe56"));
        control.rollback(t1);
        TransactionId t4 = control.beginTransaction(false);
        Assert.assertEquals("joe56 absent", read(t4, "joe56"));
        Assert.assertEquals("boe56 absent", read(t4, "boe56"));
    }

    @Ignore
    @Test
    public void readWriteMechanics() throws Exception {
        final SDataLib dataLib = storeSetup.getDataLib();
        final STableReader reader = storeSetup.getReader();

        final byte[] testKey = dataLib.newRowKey(new Object[]{"jim"});
        OperationWithAttributes put = dataLib.newPut(testKey);
        byte[] family = dataLib.encode(DEFAULT_FAMILY);
        byte[] ageQualifier = dataLib.encode("age");
        dataLib.addKeyValueToPut(put, family, ageQualifier, -1, dataLib.encode(25));
        TransactionId t = control.beginTransaction();
        transactorSetup.clientTransactor.initializePut(t.getTransactionIdString(), put);
        OperationWithAttributes put2 = dataLib.newPut(testKey);
        dataLib.addKeyValueToPut(put2, family, ageQualifier, -1, dataLib.encode(27));
        transactorSetup.clientTransactor.initializePut(
                transactorSetup.clientTransactor.transactionIdFromPut(put).getTransactionIdString(),
                put2);
        Assert.assertTrue(Bytes.equals(dataLib.encode((short) 0), put2.getAttribute(SIConstants.SI_NEEDED)));
        Object testSTable = reader.open(storeSetup.getPersonTableName());
        try {
            Assert.assertTrue(transactor.processPut(testSTable, transactorSetup.rollForwardQueue, put));
            Assert.assertTrue(transactor.processPut(testSTable, transactorSetup.rollForwardQueue, put2));
            Object get1 = makeGet(dataLib, testKey);
            transactorSetup.clientTransactor.initializeGet(t.getTransactionIdString(), get1);
            Result result = reader.get(testSTable, get1);
            if (useSimple) {
                result = transactorSetup.readController.filterResult(transactorSetup.readController.newFilterState(transactorSetup.rollForwardQueue, t, false), result);
            }
            final int ageRead = (Integer) dataLib.decode(result.getValue(family, ageQualifier), Integer.class);
            Assert.assertEquals(27, ageRead);
        } finally {
            reader.close(testSTable);
        }

        TransactionId t2 = control.beginTransaction();
        Object get = makeGet(dataLib, testKey);
        testSTable = reader.open(storeSetup.getPersonTableName());
        try {
            final Result resultTuple = reader.get(testSTable, get);
            final IFilterState filterState = transactorSetup.readController.newFilterState(transactorSetup.rollForwardQueue, t2, false);
            if (useSimple) {
                transactorSetup.readController.filterResult(filterState, resultTuple);
            }
        } finally {
            reader.close(testSTable);
        }

        control.commit(t);
        t = control.beginTransaction();

        dataLib.addKeyValueToPut(put, family, ageQualifier, -1, dataLib.encode(35));
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
                KeyValue cell = (KeyValue)input[1];
                final SDataLib dataLib = storeSetup.getDataLib();
                final long timestamp = (Long) dataLib.decode(cell.getValue(), Long.class);
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
                KeyValue cell = (KeyValue)input[1];
                final SDataLib dataLib = storeSetup.getDataLib();
                final long timestamp = (Integer) dataLib.decode(cell.getValue(), Integer.class);
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
                KeyValue cell = (KeyValue)input[1];
                final SDataLib dataLib = storeSetup.getDataLib();
                final long timestamp = (Integer) dataLib.decode(cell.getValue(), Integer.class);
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
                KeyValue cell = (KeyValue)input[1];
                final SDataLib dataLib = storeSetup.getDataLib();
                final long timestamp = (Integer) dataLib.decode(cell.getValue(), Integer.class);
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
                KeyValue cell = (KeyValue)input[1];
                final SDataLib dataLib = storeSetup.getDataLib();
                final long timestamp = (Integer) dataLib.decode(cell.getValue(), Integer.class);
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
                KeyValue cell = (KeyValue)input[1];
                final SDataLib dataLib = storeSetup.getDataLib();
                final long timestamp = (Long) dataLib.decode(cell.getValue(), Long.class);
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
                KeyValue cell = (KeyValue)input[1];
                final SDataLib dataLib = storeSetup.getDataLib();
                final long timestamp = (Integer) dataLib.decode(cell.getValue(), Integer.class);
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
                KeyValue cell = (KeyValue)input[1];
                final SDataLib dataLib = storeSetup.getDataLib();
                final long timestamp = (Long) dataLib.decode(cell.getValue(), Long.class);
                Assert.assertEquals(t.getId() + 2, timestamp);
                return null;
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
                t0 = control.beginTransaction();
            }
            TransactionId t1;
            if (nested) {
                t1 = control.beginChildTransaction(t0, true);
            } else {
                t1 = control.beginTransaction();
            }
            TransactionId t1b = null;
            if (conflictingWrite) {
                t1b = control.beginTransaction();
            }
            final String testRow = "joe" + testIndex;
            insertAge(t1, testRow, 20);
            final CountDownLatch latch = makeLatch(testRow);
            if (commitRollBackOrFail.equals("commit")) {
                control.commit(t1);
            } else if (commitRollBackOrFail.equals("rollback")) {
                control.rollback(t1);
            } else if (commitRollBackOrFail.equals("fail")) {
                control.fail(t1);
            } else {
                throw new RuntimeException("unknown value");
            }
            if (nested) {
                if (parentCommitRollBackOrFail.equals("commit")) {
                    control.commit(t0);
                } else if (parentCommitRollBackOrFail.equals("rollback")) {
                    control.rollback(t0);
                } else if (parentCommitRollBackOrFail.equals("fail")) {
                    control.fail(t0);
                } else {
                    throw new RuntimeException("unknown value");
                }
            }
            Result result = readRaw(testRow);
            final SDataLib dataLib = storeSetup.getDataLib();
            final List<KeyValue> commitTimestamps = result.getColumn(dataLib.encode(SNAPSHOT_ISOLATION_FAMILY),
                    dataLib.encode(SNAPSHOT_ISOLATION_COMMIT_TIMESTAMP_COLUMN_STRING));
            for (KeyValue c : commitTimestamps) {
                final int timestamp = (Integer) dataLib.decode(c.getValue(), Integer.class);
                Assert.assertEquals(-1, timestamp);
                Assert.assertEquals(t1.getId(), c.getTimestamp());
            }
            Assert.assertTrue("Latch timed out", latch.await(11, TimeUnit.SECONDS));

            Result result2 = readRaw(testRow);
            final List<KeyValue> commitTimestamps2 = result2.getColumn(dataLib.encode(SNAPSHOT_ISOLATION_FAMILY),
                    dataLib.encode(SNAPSHOT_ISOLATION_COMMIT_TIMESTAMP_COLUMN_STRING));
            for (KeyValue c2 : commitTimestamps2) {
                timestampDecoder.apply(new Object[]{t1, c2});
                Assert.assertEquals(t1.getId(), c2.getTimestamp());
            }
            TransactionId t2 = control.beginTransaction(false);
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
                    control.fail(t1b);
                }
            }
        } finally {
            Tracer.rollForwardDelayOverride = null;
        }
    }

    private CountDownLatch makeLatch(final String targetKey) {
        final SDataLib dataLib = storeSetup.getDataLib();
        final CountDownLatch latch = new CountDownLatch(1);
        Tracer.registerRowRollForward(new Function<byte[],byte[]>() {
            @Override
            public byte[] apply(@Nullable byte[] input) {
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
                KeyValue cell = (KeyValue)input[1];
                final SDataLib dataLib = storeSetup.getDataLib();
                final long timestamp = (Long) dataLib.decode(cell.getValue(), Long.class);
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
                KeyValue cell = (KeyValue)input[1];
                final SDataLib dataLib = storeSetup.getDataLib();
                final byte[] keyValueValue = cell.getValue();
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
            TransactionId t1 = control.beginTransaction();
            final CountDownLatch transactionlatch = makeTransactionLatch(t1);
            insertAge(t1, testRow, 20);
            Assert.assertTrue(transactionlatch.await(11, TimeUnit.SECONDS));
            if (commit) {
                control.commit(t1);
            } else {
                control.rollback(t1);
            }
            Result result = readRaw(testRow);
            final SDataLib dataLib = storeSetup.getDataLib();
            final List<KeyValue> commitTimestamps = result.getColumn(dataLib.encode(SNAPSHOT_ISOLATION_FAMILY),
                    dataLib.encode(SNAPSHOT_ISOLATION_COMMIT_TIMESTAMP_COLUMN_STRING));
            for (KeyValue c : commitTimestamps) {
                final int timestamp = (Integer) dataLib.decode(c.getValue(), Integer.class);
                Assert.assertEquals(-1, timestamp);
                Assert.assertEquals(t1.getId(), c.getTimestamp());
            }

            final CountDownLatch latch = makeLatch(testRow);
            TransactionId t2 = control.beginTransaction(false);
            if (commit) {
                Assert.assertEquals(testRow + " age=20 job=null", read(t2, testRow));
            } else {
                Assert.assertEquals(testRow + " absent", read(t2, testRow));
            }
            Assert.assertTrue(latch.await(11, TimeUnit.SECONDS));

            Result result2 = readRaw(testRow);

            final List<KeyValue> commitTimestamps2 = result2.getColumn(dataLib.encode(SNAPSHOT_ISOLATION_FAMILY),
                    dataLib.encode(SNAPSHOT_ISOLATION_COMMIT_TIMESTAMP_COLUMN_STRING));
            for (KeyValue c2 : commitTimestamps2) {
                timestampProcessor.apply(new Object[]{t1, c2});
                Assert.assertEquals(t1.getId(), c2.getTimestamp());
            }
        } finally {
            Tracer.rollForwardDelayOverride = null;
        }
    }

    private Result readRaw(String name) throws IOException {
        return readAgeRawDirect(storeSetup, name, false);
    }

    private Result readRaw(String name, boolean allVersions) throws IOException {
        return readAgeRawDirect(storeSetup, name, allVersions);
    }

    static Result readAgeRawDirect(StoreSetup storeSetup, String name, boolean allversions) throws IOException {
        final SDataLib dataLib = storeSetup.getDataLib();
        final STableReader reader = storeSetup.getReader();

        byte[] key = dataLib.newRowKey(new Object[]{name});
        OperationWithAttributes get = makeGet(dataLib, key);
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

    private static OperationWithAttributes makeGet(SDataLib dataLib, byte[] key) throws IOException {
        final OperationWithAttributes get = dataLib.newGet(key, null, null, null);
        addPredicateFilter(dataLib, get);
        return get;
    }

    private static Object makeScan(SDataLib dataLib, byte[] endKey, ArrayList families, byte[] startKey) throws IOException {
        final Object scan = dataLib.newScan(startKey, endKey, families, null, null);
        addPredicateFilter(dataLib, scan);
        return scan;
    }

    private static void addPredicateFilter(SDataLib dataLib, Object operation) throws IOException {
        final BitSet bitSet = new BitSet(2);
        bitSet.set(0);
        bitSet.set(1);
        EntryPredicateFilter filter = new EntryPredicateFilter(bitSet, new ObjectArrayList<Predicate>(), true);
				((OperationWithAttributes)operation).setAttribute(SpliceConstants.ENTRY_PREDICATE_LABEL,filter.toBytes());
    }

    @Ignore
    public void transactionTimeout() throws IOException, InterruptedException {
        TransactionId t1 = control.beginTransaction();
        insertAge(t1, "joe63", 20);
        sleep();
        TransactionId t2 = control.beginTransaction();
        insertAge(t2, "joe63", 21);
        control.commit(t2);
    }

    @Test
    public void transactionNoTimeoutWithKeepAlive() throws IOException, InterruptedException {
        TransactionId t1 = control.beginTransaction();
        insertAge(t1, "joe64", 20);
        sleep();
        control.keepAlive(t1);
        TransactionId t2 = control.beginTransaction();
        try {
            insertAge(t2, "joe64", 21);
            Assert.fail();
        } catch (WriteConflict e) {
        } catch (RetriesExhaustedWithDetailsException e) {
            assertWriteConflict(e);
        } finally {
            control.fail(t2);
        }
    }

    @Ignore
    public void transactionTimeoutAfterKeepAlive() throws IOException, InterruptedException {
        TransactionId t1 = control.beginTransaction();
        insertAge(t1, "joe65", 20);
        sleep();
        control.keepAlive(t1);
        sleep();
        TransactionId t2 = control.beginTransaction();
        insertAge(t2, "joe65", 21);
        control.commit(t2);
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
        final TransactionId t1 = control.beginTransaction();
        insertAge(t1, "joe66", 20);
        final TransactionId t2 = control.beginTransaction();
        try {
            insertAge(t2, "joe66", 22);
        } catch (WriteConflict e) {
        } catch (RetriesExhaustedWithDetailsException e) {
            assertWriteConflict(e);
        } finally {
            control.fail(t2);
        }
        try {
            insertAge(t2, "joe66", 23);
        } catch (WriteConflict e) {
        } catch (RetriesExhaustedWithDetailsException e) {
            assertWriteConflict(e);
        } finally {
            control.fail(t2);
        }
    }

    @Test
    public void transactionCommitFailRaceFailWins() throws Exception {
        final TransactionId t1 = control.beginTransaction();
        insertAge(t1, "joe67", 20);
        final TransactionId t2 = control.beginTransaction();

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
                    control.commit(t2);
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
            control.fail(t2);
        }

        latch2.await(2, TimeUnit.SECONDS);
        Assert.assertEquals("committing failed", exception[0].getMessage());
        Assert.assertEquals(IOException.class, exception[0].getClass());
    }

    @Test
    public void transactionCommitFailRaceCommitWins() throws Exception {
        final TransactionId t1 = control.beginTransaction();
        insertAge(t1, "joe68", 20);
        final TransactionId t2 = control.beginTransaction();

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
                    control.commit(t2);
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
            control.fail(t2);
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
                KeyValue cell = (KeyValue)input[1];
                final SDataLib dataLib = storeSetup.getDataLib();
                final int timestamp = (Integer) dataLib.decode(cell.getValue(), Integer.class);
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
                KeyValue cell = (KeyValue)input[1];
                final SDataLib dataLib = storeSetup.getDataLib();
                final int timestamp = (Integer) dataLib.decode(cell.getValue(), Integer.class);
                Assert.assertEquals(-1, timestamp);
                return null;
            }
        });
    }

    private void checkNoCompaction(int testIndex, boolean commit, Function<Object[], Object> timestampProcessor) throws IOException {
        TransactionId t0 = null;
        String testKey = "joe" + testIndex;
        for (int i = 0; i < 10; i++) {
            TransactionId tx = control.beginTransaction();
            if (i == 0) {
                t0 = tx;
            }
            insertAge(tx, testKey + "-" + i, i);
            if (commit) {
                control.commit(tx);
            } else {
                control.rollback(tx);
            }
        }
        Result result = readRaw(testKey + "-0");
        final SDataLib dataLib = storeSetup.getDataLib();
        final List<KeyValue> commitTimestamps = result.getColumn(dataLib.encode(SNAPSHOT_ISOLATION_FAMILY),
                dataLib.encode(SNAPSHOT_ISOLATION_COMMIT_TIMESTAMP_COLUMN));
        for (KeyValue c : commitTimestamps) {
            timestampProcessor.apply(new Object[]{t0, c});
            Assert.assertEquals(t0.getId(), c.getTimestamp());
        }
    }

    @Test
    public void compaction() throws IOException, InterruptedException {
        checkCompaction(70, true, new Function<Object[], Object>() {
            @Override
            public Object apply(@Nullable Object[] input) {
                TransactionId t = (TransactionId) input[0];
                KeyValue cell = (KeyValue)input[1];
                final SDataLib dataLib = storeSetup.getDataLib();
                final long timestamp = (Long) dataLib.decode(cell.getValue(), Long.class);
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
                KeyValue cell = (KeyValue)input[1];
                final SDataLib dataLib = storeSetup.getDataLib();
                final int timestamp = (Integer) dataLib.decode(cell.getValue(), Integer.class);
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
            TransactionId tx = control.beginTransaction();
            if (i == 0) {
                t0 = tx;
            }
            insertAge(tx, testRow + "-" + i, i);
            if (!useSimple) {
                admin.flush(storeSetup.getPersonTableName());
            }
            if (commit) {
                control.commit(tx);
            } else {
                control.rollback(tx);
            }
        }

        if (useSimple) {
            final LStore store = (LStore) storeSetup.getStore();
            store.compact(transactor, storeSetup.getPersonTableName());
        } else {
            admin.majorCompact(storeSetup.getPersonTableName());
            Assert.assertTrue(latch.await(2, TimeUnit.SECONDS));
        }
        Result result = readRaw(testRow + "-0");
        final SDataLib dataLib = storeSetup.getDataLib();
        final List<KeyValue> commitTimestamps = result.getColumn(dataLib.encode(SNAPSHOT_ISOLATION_FAMILY),
                dataLib.encode(SNAPSHOT_ISOLATION_COMMIT_TIMESTAMP_COLUMN_STRING));
        for (KeyValue c : commitTimestamps) {
            timestampProcessor.apply(new Object[]{t0, c});
            Assert.assertEquals(t0.getId(), c.getTimestamp());
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
            TransactionId t1 = control.beginTransaction();
            insertAge(t1, "joe86", 20);

            new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        waits[0] = latch.await(2, TimeUnit.SECONDS);
                        Assert.assertTrue(waits[0]);
                        TransactionId t2 = control.beginTransaction();
                        try {
                            insertAge(t2, "joe86", 21);
                            Assert.fail();
                        } catch (WriteConflict e) {
                        } catch (RetriesExhaustedWithDetailsException e) {
                            assertWriteConflict(e);
                        } finally {
                            control.fail(t2);
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

            control.commit(t1);
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

    @Ignore
    public void committingRaceNested() throws IOException {
        try {
            final CountDownLatch latch = new CountDownLatch(1);
            final CountDownLatch latch2 = new CountDownLatch(1);
            final CountDownLatch latch3 = new CountDownLatch(1);
            final Boolean[] success = new Boolean[]{false};
            final Boolean[] waits = new Boolean[]{false, false};
            final TransactionId t1 = control.beginTransaction();
            final TransactionId t1a = control.beginChildTransaction(t1, true);
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
                        TransactionId t2 = control.beginTransaction();
                        try {
                            insertAge(t2, "joe88", 21);
                            Assert.fail();
                        } catch (WriteConflict e) {
                        } catch (RetriesExhaustedWithDetailsException e) {
                            assertWriteConflict(e);
                        } finally {
                            control.fail(t2);
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

            control.commit(t1a);
            control.commit(t1);
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
            final TransactionId t1 = control.beginTransaction();
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
                        TransactionId t2 = control.beginTransaction();
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
                control.commit(t1);
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
        TransactionId t1 = control.beginTransaction();
        insertAge(t1, "joe89", 20);
        control.commit(t1);

        TransactionId t2 = control.beginTransaction();
        insertAge(t2, "joe89", 21);

        TransactionId t3 = control.beginTransaction();
        Result result = readRaw("joe89", true);
        final IFilterState filterState = transactorSetup.readController.newFilterState(t3);
        result = transactorSetup.readController.filterResult(filterState, result);
        Assert.assertEquals("joe89 age=20 job=null", resultToString("joe89", result));
    }

    @Test
    public void childIndependentTransactionWriteCommitRollbackRead() throws IOException {
        TransactionId t1 = control.beginTransaction();
        TransactionId t2 = control.beginChildTransaction(t1, true, true);
        insertAge(t2, "joe52", 21);
        control.commit(t2);
        control.rollback(t2);
        Assert.assertEquals("joe52 age=21 job=null", read(t1, "joe52"));
        control.commit(t1);
    }

    @Test
    public void childIndependentSeesParentWrites() throws IOException {
        TransactionId t1 = control.beginTransaction();
        insertAge(t1, "joe41", 20);
        TransactionId t2 = control.beginChildTransaction(t1, false, true);
        Assert.assertEquals("joe41 age=20 job=null", read(t2, "joe41"));
    }

    @Test
    public void childIndependentReadOnlySeesParentWrites() throws IOException {
        TransactionId t1 = control.beginTransaction();
        insertAge(t1, "joe96", 20);
        final TransactionId t2 = control.beginChildTransaction(t1, false, false);
        Assert.assertEquals("joe96 age=20 job=null", read(t2, "joe96"));
    }

    @Test
    public void childIndependentReadUncommittedDoesSeeParentWrites() throws IOException {
        TransactionId t1 = control.beginTransaction();
        insertAge(t1, "joe99", 20);
        TransactionId t2 = control.beginChildTransaction(t1, false, true, false, true, true, null);
        Assert.assertEquals("joe99 age=20 job=null", read(t2, "joe99"));
    }

    @Test
    public void childIndependentReadOnlyUncommittedDoesSeeParentWrites() throws IOException {
        TransactionId t1 = control.beginTransaction();
        insertAge(t1, "joe100", 20);
        final TransactionId t2 = control.beginChildTransaction(t1, false, false, false, true, true, null);
        Assert.assertEquals("joe100 age=20 job=null", read(t2, "joe100"));
    }

    @Test
    public void childIndependentTransactionWithOtherCommitBetweenParentAndChild() throws IOException {
        TransactionId t0 = control.beginTransaction();
        insertAge(t0, "joe38", 20);
        control.commit(t0);

        TransactionId t1 = control.beginTransaction();

        TransactionId otherTransaction = control.beginTransaction();
        insertAge(otherTransaction, "joe38", 30);
        control.commit(otherTransaction);

        TransactionId t2 = control.beginChildTransaction(t1, false, true, false, null, true, null);
        Assert.assertEquals("joe38 age=30 job=null", read(t2, "joe38"));
        control.commit(t2);
        control.commit(t1);
    }

    @Test
    public void childIndependentReadOnlyTransactionWithOtherCommitBetweenParentAndChild() throws IOException {
        TransactionId t0 = control.beginTransaction();
        insertAge(t0, "joe97", 20);
        control.commit(t0);

        TransactionId t1 = control.beginTransaction();

        TransactionId otherTransaction = control.beginTransaction();
        insertAge(otherTransaction, "joe97", 30);
        control.commit(otherTransaction);

        TransactionId t2 = control.beginChildTransaction(t1, false, false, false, null, true, null);
        Assert.assertEquals("joe97 age=30 job=null", read(t2, "joe97"));
        control.commit(t2);
        control.commit(t1);
    }

    @Test
    public void childIndependentTransactionWithReadCommittedOffWithOtherCommitBetweenParentAndChild() throws IOException {
        TransactionId t0 = control.beginTransaction();
        insertAge(t0, "joe39", 20);
        control.commit(t0);

        TransactionId t1 = control.beginTransaction();

        TransactionId otherTransaction = control.beginTransaction();
        insertAge(otherTransaction, "joe39", 30);
        control.commit(otherTransaction);

        TransactionId t2 = control.beginChildTransaction(t1, false, true);
        Assert.assertEquals("joe39 age=20 job=null", read(t2, "joe39"));
        control.commit(t2);
        control.commit(t1);
    }

    @Test
    public void childIndependentReadOnlyTransactionWithReadCommittedOffWithOtherCommitBetweenParentAndChild() throws IOException {
        TransactionId t0 = control.beginTransaction();
        insertAge(t0, "joe98", 20);
        control.commit(t0);

        TransactionId t1 = control.beginTransaction();

        TransactionId otherTransaction = control.beginTransaction();
        insertAge(otherTransaction, "joe98", 30);
        control.commit(otherTransaction);

        TransactionId t2 = control.beginChildTransaction(t1, false, false);
        Assert.assertEquals("joe98 age=20 job=null", read(t2, "joe98"));
        control.commit(t2);
        control.commit(t1);
    }

    @Test
    public void childIndependentTransactionWriteRollbackRead() throws IOException {
        TransactionId t1 = control.beginTransaction();
        insertAge(t1, "joe28", 20);
        TransactionId t2 = control.beginChildTransaction(t1, false, true);
        insertAge(t2, "moe28", 21);
        Assert.assertEquals("joe28 age=20 job=null", read(t1, "joe28"));
        Assert.assertEquals("moe28 absent", read(t1, "moe28"));
        control.rollback(t2);
        Assert.assertEquals("joe28 age=20 job=null", read(t1, "joe28"));
        Assert.assertEquals("moe28 absent", read(t1, "moe28"));
        control.commit(t1);

        TransactionId t3 = control.beginTransaction(false);
        Assert.assertEquals("joe28 age=20 job=null", read(t3, "joe28"));
        Assert.assertEquals("moe28 absent", read(t3, "moe28"));
    }

    @Test
    public void multipleChildIndependentTransactionWriteRead() throws IOException {
        TransactionId t1 = control.beginTransaction();
        insertAge(t1, "joe31", 20);
        TransactionId t2 = control.beginChildTransaction(t1, false, true);
        TransactionId t3 = control.beginChildTransaction(t1, false, true);
        insertAge(t2, "moe31", 21);
        insertJob(t3, "boe31", "baker");
        Assert.assertEquals("joe31 age=20 job=null", read(t1, "joe31"));
        Assert.assertEquals("moe31 absent", read(t1, "moe31"));
        Assert.assertEquals("boe31 absent", read(t1, "boe31"));
        control.commit(t2);

        TransactionId t4 = control.beginTransaction(false);
        Assert.assertEquals("moe31 age=21 job=null", read(t4, "moe31"));

        control.commit(t3);

        TransactionId t5 = control.beginTransaction(false);
        Assert.assertEquals("joe31 absent", read(t5, "joe31"));
        Assert.assertEquals("moe31 age=21 job=null", read(t5, "moe31"));
        Assert.assertEquals("boe31 age=null job=baker", read(t5, "boe31"));

        Assert.assertEquals("joe31 age=20 job=null", read(t1, "joe31"));
        Assert.assertEquals("moe31 age=21 job=null", read(t1, "moe31"));
        Assert.assertEquals("boe31 age=null job=baker", read(t1, "boe31"));
        control.commit(t1);

        TransactionId t6 = control.beginTransaction(false);
        Assert.assertEquals("joe31 age=20 job=null", read(t6, "joe31"));
        Assert.assertEquals("moe31 age=21 job=null", read(t6, "moe31"));
        Assert.assertEquals("boe31 age=null job=baker", read(t6, "boe31"));
    }

    @Test
    public void multipleChildIndependentConflict() throws IOException {
        TransactionId t1 = control.beginTransaction();
        TransactionId t2 = control.beginChildTransaction(t1, false, true);
        TransactionId t3 = control.beginChildTransaction(t1, false, true);
        insertAge(t2, "moe31", 21);
        try {
            insertJob(t3, "moe31", "baker");
            Assert.fail();
        } catch (WriteConflict e) {
        } catch (RetriesExhaustedWithDetailsException e) {
            assertWriteConflict(e);
        } finally {
            control.fail(t3);
        }
    }

    @Test
    public void childIndependentTransactionWriteRead() throws IOException {
        TransactionId t1 = control.beginTransaction();
        insertAge(t1, "joe29", 20);
        TransactionId t2 = control.beginChildTransaction(t1, false, true);
        insertAge(t2, "moe29", 21);
        Assert.assertEquals("moe29 absent", read(t1, "moe29"));
        control.commit(t2);
        Assert.assertEquals("moe29 age=21 job=null", read(t1, "moe29"));

        TransactionId t3 = control.beginTransaction(false);
        Assert.assertEquals("moe29 age=21 job=null", read(t3, "moe29"));

        Assert.assertEquals("moe29 age=21 job=null", read(t1, "moe29"));
        control.commit(t1);

        TransactionId t4 = control.beginTransaction(false);
        Assert.assertEquals("moe29 age=21 job=null", read(t4, "moe29"));
    }

    @Test
    public void childIndependentTransactionWriteRollbackParentRead() throws IOException {
        TransactionId t1 = control.beginTransaction();
        insertAge(t1, "joe30", 20);
        TransactionId t2 = control.beginChildTransaction(t1, false, true);
        insertAge(t2, "moe30", 21);
        control.commit(t2);
        control.rollback(t1);

        TransactionId t4 = control.beginTransaction(false);
        Assert.assertEquals("joe30 absent", read(t4, "joe30"));
        Assert.assertEquals("moe30 age=21 job=null", read(t4, "moe30"));
    }

    @Test
    public void commitParentOfCommittedIndependent() throws IOException {
        TransactionId t1 = control.beginTransaction();
        insertAge(t1, "joe49", 20);
        TransactionId t2 = control.beginChildTransaction(t1, false, true);
        insertAge(t2, "moe49", 21);
        control.commit(t2);
        final Transaction transactionStatusA = transactorSetup.transactionStore.getTransaction(t2);
        control.commit(t1);
        final Transaction transactionStatusB = transactorSetup.transactionStore.getTransaction(t2);
        Assert.assertEquals("committing parent of independent transaction should not change the commit time of the child",
                transactionStatusA.getCommitTimestampDirect(), transactionStatusB.getCommitTimestampDirect());
        Assert.assertEquals("committing parent of independent transaction should not change the global commit time of the child",
                transactionStatusA.getEffectiveCommitTimestamp(), transactionStatusB.getEffectiveCommitTimestamp());
    }

    @Test
    public void independentWriteOverlapWithReadCommittedWriter() throws IOException {
        TransactionId parent = control.beginTransaction();

        TransactionId other = control.beginTransaction(true, false, true);

        TransactionId child = control.beginChildTransaction(parent, false, true);
        insertAge(child, "joe33", 22);
        control.commit(child);

        Assert.assertEquals("joe33 age=22 job=null", read(other, "joe33"));
        try {
            insertAge(other, "joe33", 21);
            Assert.fail();
        } catch (WriteConflict e) {
        } catch (RetriesExhaustedWithDetailsException e) {
            assertWriteConflict(e);
        } finally {
            control.fail(other);
        }
    }

    @Test
    public void independentWriteFollowedByReadCommittedWriter() throws IOException {
        TransactionId parent = control.beginTransaction();

        TransactionId child = control.beginChildTransaction(parent, false, true);
        insertAge(child, "joe35", 22);
        control.commit(child);

        TransactionId other = control.beginTransaction(true, false, true);
        insertAge(other, "joe35", 21);
        control.commit(other);
    }

    @Test
    public void writeAllNullsRead() throws IOException {
        TransactionId t1 = control.beginTransaction();
        Assert.assertEquals("joe113 absent", read(t1, "joe113"));
        insertAge(t1, "joe113", null);
        Assert.assertEquals("joe113 age=null job=null", read(t1, "joe113"));
        control.commit(t1);

        TransactionId t2 = control.beginTransaction();
        Assert.assertEquals("joe113 age=null job=null", read(t2, "joe113"));
    }

    @Test
    public void writeAllNullsReadOverlap() throws IOException {
        TransactionId t1 = control.beginTransaction();
        Assert.assertEquals("joe114 absent", read(t1, "joe114"));
        insertAge(t1, "joe114", null);
        Assert.assertEquals("joe114 age=null job=null", read(t1, "joe114"));

        TransactionId t2 = control.beginTransaction();
        Assert.assertEquals("joe114 age=null job=null", read(t1, "joe114"));
        Assert.assertEquals("joe114 absent", read(t2, "joe114"));
        control.commit(t1);
        Assert.assertEquals("joe114 absent", read(t2, "joe114"));
    }


    @Test
    public void writeAllNullsWrite() throws IOException {
        TransactionId t1 = control.beginTransaction();
        insertAge(t1, "joe115", null);
        Assert.assertEquals("joe115 age=null job=null", read(t1, "joe115"));
        control.commit(t1);

        TransactionId t2 = control.beginTransaction();
        Assert.assertEquals("joe115 age=null job=null", read(t2, "joe115"));
        insertAge(t2, "joe115", 30);
        Assert.assertEquals("joe115 age=30 job=null", read(t2, "joe115"));
        control.commit(t2);
    }

    @Test
    public void writeAllNullsWriteOverlap() throws IOException {
        TransactionId t1 = control.beginTransaction();
        Assert.assertEquals("joe116 absent", read(t1, "joe116"));
        insertAge(t1, "joe116", null);
        Assert.assertEquals("joe116 age=null job=null", read(t1, "joe116"));

        TransactionId t2 = control.beginTransaction();
        Assert.assertEquals("joe116 age=null job=null", read(t1, "joe116"));
        Assert.assertEquals("joe116 absent", read(t2, "joe116"));
        try {
            insertAge(t2, "joe116", 30);
            Assert.fail();
        } catch (WriteConflict e) {
        } catch (RetriesExhaustedWithDetailsException e) {
            assertWriteConflict(e);
        } finally {
            control.fail(t2);
        }
        Assert.assertEquals("joe116 age=null job=null", read(t1, "joe116"));
        control.commit(t1);
        try {
            control.commit(t2);
            Assert.fail();
        } catch (DoNotRetryIOException dnrio) {
            Assert.assertTrue("Incorrect message returned!", dnrio.getMessage().matches("transaction [0-9]* is not ACTIVE.*"));
        }
    }

    @Test
    public void readAllNullsAfterCommit() throws IOException {
        TransactionId t1 = control.beginTransaction();
        insertAge(t1, "joe117", null);
        control.commit(t1);
        Assert.assertEquals("joe117 age=null job=null", read(t1, "joe117"));
    }

    @Test
    public void rollbackAllNullAfterCommit() throws IOException {
        TransactionId t1 = control.beginTransaction();
        insertAge(t1, "joe118", null);
        control.commit(t1);
        control.rollback(t1);
        Assert.assertEquals("joe118 age=null job=null", read(t1, "joe118"));
    }

    @Test
    public void writeAllNullScan() throws IOException {
        TransactionId t1 = control.beginTransaction();
        Assert.assertEquals("joe119 absent", read(t1, "joe119"));
        insertAge(t1, "joe119", null);
        Assert.assertEquals("joe119 age=null job=null", read(t1, "joe119"));
        control.commit(t1);

        TransactionId t2 = control.beginTransaction();
        Assert.assertEquals("joe119 age=null job=null[ S.TX@~9 ]", scan(t2, "joe119"));

        Assert.assertEquals("joe119 age=null job=null", read(t2, "joe119"));
    }

    @Test
    public void batchWriteRead() throws IOException {
        TransactionId t1 = control.beginTransaction();
        Assert.assertEquals("joe144 absent", read(t1, "joe144"));
        insertAgeBatch(new Object[]{t1, "joe144", 20}, new Object[]{t1, "bob144", 30});
        Assert.assertEquals("joe144 age=20 job=null", read(t1, "joe144"));
        Assert.assertEquals("bob144 age=30 job=null", read(t1, "bob144"));
        control.commit(t1);

        TransactionId t2 = control.beginTransaction();
        Assert.assertEquals("joe144 age=20 job=null", read(t2, "joe144"));
        Assert.assertEquals("bob144 age=30 job=null", read(t2, "bob144"));
    }

    @Test
    public void writeDeleteBatchInsertRead() throws IOException {
        TransactionId t1 = control.beginTransaction();
        insertAge(t1, "joe145", 10);
        deleteRow(t1, "joe145");
        insertAgeBatch(new Object[]{t1, "joe145", 20}, new Object[]{t1, "bob145", 30});
        Assert.assertEquals("joe145 age=20 job=null", read(t1, "joe145"));
        Assert.assertEquals("bob145 age=30 job=null", read(t1, "bob145"));
        control.commit(t1);

        TransactionId t2 = control.beginTransaction();
        Assert.assertEquals("joe145 age=20 job=null", read(t2, "joe145"));
        Assert.assertEquals("bob145 age=30 job=null", read(t2, "bob145"));
    }

    @Test
    public void writeManyDeleteBatchInsertRead() throws IOException {
        TransactionId t1 = control.beginTransaction();
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
        control.commit(t1);

        TransactionId t2 = control.beginTransaction();
        Assert.assertEquals("146joe age=11 job=null", read(t2, "146joe"));
        Assert.assertEquals("146doe age=21 job=null", read(t2, "146doe"));
        Assert.assertEquals("146boe age=31 job=null", read(t2, "146boe"));
        Assert.assertEquals("146moe age=41 job=null", read(t2, "146moe"));
        Assert.assertEquals("146zoe age=51 job=null", read(t2, "146zoe"));
    }

    @Test
    public void writeManyDeleteBatchInsertSomeRead() throws IOException {
        TransactionId t1 = control.beginTransaction();
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
        control.commit(t1);

        TransactionId t2 = control.beginTransaction();
        Assert.assertEquals("147joe age=11 job=null", read(t2, "147joe"));
        Assert.assertEquals("147boe age=31 job=null", read(t2, "147boe"));
        Assert.assertEquals("147zoe age=51 job=null", read(t2, "147zoe"));
    }

    // "Oldest Active" tests

    @Test
    public void oldestActiveTransactionsOne() throws IOException {
        final TransactionId t1 = control.beginTransaction();
        transactorSetup.timestampSource.rememberTimestamp(t1.getId() - 1);
        final List<TransactionId> result = control.getActiveTransactionIds(t1);
        Assert.assertEquals(1, result.size());
        Assert.assertEquals(t1.getId(), result.get(0).getId());
    }

    @Test
    public void oldestActiveTransactionsTwo() throws IOException {
        final TransactionId t0 = control.beginTransaction();
        transactorSetup.timestampSource.rememberTimestamp(t0.getId());
        final TransactionId t1 = control.beginTransaction();
        final List<TransactionId> result = control.getActiveTransactionIds(t1);
        Assert.assertEquals(2, result.size());
        Assert.assertEquals(t0.getId(), result.get(0).getId());
        Assert.assertEquals(t1.getId(), result.get(1).getId());
    }

    @Test
    public void oldestActiveTransactionsFuture() throws IOException {
        final TransactionId t0 = control.beginTransaction();
        transactorSetup.timestampSource.rememberTimestamp(t0.getId());
        final TransactionId t1 = control.beginTransaction();
        control.beginTransaction();
        final List<TransactionId> result = control.getActiveTransactionIds(t1);
        Assert.assertEquals(2, result.size());
        Assert.assertEquals(t0.getId(), result.get(0).getId());
        Assert.assertEquals(t1.getId(), result.get(1).getId());
    }

    @Test
    public void oldestActiveTransactionsSkipCommitted() throws IOException {
        final TransactionId t0 = control.beginTransaction();
        transactorSetup.timestampSource.rememberTimestamp(t0.getId());
        final TransactionId t1 = control.beginTransaction();
        final TransactionId t2 = control.beginTransaction();
        control.commit(t0);
        final List<TransactionId> result = control.getActiveTransactionIds(t2);
        Assert.assertEquals(2, result.size());
        Assert.assertEquals(t1.getId(), result.get(0).getId());
        Assert.assertEquals(t2.getId(), result.get(1).getId());
    }

    @Test
    public void oldestActiveTransactionsSkipCommittedGap() throws IOException {
        final TransactionId t0 = control.beginTransaction();
        transactorSetup.timestampSource.rememberTimestamp(t0.getId());
        final TransactionId t1 = control.beginTransaction();
        final TransactionId t2 = control.beginTransaction();
        control.commit(t1);
        final List<TransactionId> result = control.getActiveTransactionIds(t2);
        Assert.assertEquals(2, result.size());
        Assert.assertEquals(t0.getId(), result.get(0).getId());
        Assert.assertEquals(t2.getId(), result.get(1).getId());
    }

    @Test
    public void oldestActiveTransactionsSavedTimestampAdvances() throws IOException {
        final TransactionId t0 = control.beginTransaction();
        transactorSetup.timestampSource.rememberTimestamp(t0.getId() - 1);
        control.beginTransaction();
        final TransactionId t2 = control.beginTransaction();
        control.commit(t0);
        final long originalSavedTimestamp = transactorSetup.timestampSource.retrieveTimestamp();
        control.getActiveTransactionIds(t2);
        final long newSavedTimestamp = transactorSetup.timestampSource.retrieveTimestamp();
        Assert.assertEquals(originalSavedTimestamp + 2, newSavedTimestamp);
    }

    @Test
    public void oldestActiveTransactionsIgnoreEffectiveStatus() throws IOException {
        final TransactionId t0 = control.beginTransaction();
        transactorSetup.timestampSource.rememberTimestamp(t0.getId());
        final TransactionId t1 = control.beginChildTransaction(t0, true);
        control.commit(t1);
        final TransactionId t2 = control.beginChildTransaction(t0, true);
        List<TransactionId> active = control.getActiveTransactionIds(t2);
        Assert.assertEquals(2, active.size());
        Assert.assertTrue(active.contains(t0));
        Assert.assertTrue(active.contains(t2));
    }

    @Test
    public void oldestActiveTransactionsFillMissing() throws IOException {
        final TransactionId t0 = control.beginTransaction();
        transactorSetup.timestampSource.rememberTimestamp(t0.getId());
        TransactionId committedTransactionID = null;
        for (int i = 0; i < SITransactor.MAX_ACTIVE_COUNT; i++) {
            final TransactionId transactionId = control.beginTransaction();
            if (committedTransactionID == null) {
                committedTransactionID = transactionId;
            }
            control.commit(transactionId);
        }
        Long commitTimestamp = (committedTransactionID.getId() + 1);
        final TransactionId voidedTransactionID = control.transactionIdFromString(commitTimestamp.toString());
        try {
            control.getTransactionStatus(voidedTransactionID);
            Assert.fail();
        } catch (RuntimeException ex) {
            Assert.assertTrue(ex.getMessage().startsWith("transaction ID not found:"));
        }

        final TransactionId t1 = control.beginTransaction();
        final List<TransactionId> result = control.getActiveTransactionIds(t1);
        Assert.assertEquals(2, result.size());
        Assert.assertEquals(t0.getId(), result.get(0).getId());
        Assert.assertEquals(t1.getId(), result.get(1).getId());
        Assert.assertEquals(TransactionStatus.ERROR, control.getTransactionStatus(voidedTransactionID));
    }

    @Test
    public void oldestActiveTransactionsManyActive() throws IOException {
        final TransactionId t0 = control.beginTransaction();
        transactorSetup.timestampSource.rememberTimestamp(t0.getId());
        for (int i = 0; i < SITransactor.MAX_ACTIVE_COUNT / 2; i++) {
            control.beginTransaction();
        }
        final TransactionId t1 = control.beginTransaction();
        final List<TransactionId> result = control.getActiveTransactionIds(t1);
        Assert.assertEquals(SITransactor.MAX_ACTIVE_COUNT / 2 + 2, result.size());
        Assert.assertEquals(t0.getId(), result.get(0).getId());
        Assert.assertEquals(t1.getId(), result.get(result.size() - 1).getId());
    }

    @Test
    public void oldestActiveTransactionsTooManyActive() throws IOException {
        final TransactionId t0 = control.beginTransaction();
        transactorSetup.timestampSource.rememberTimestamp(t0.getId());
        for (int i = 0; i < SITransactor.MAX_ACTIVE_COUNT; i++) {
            control.beginTransaction();
        }

        final TransactionId t1 = control.beginTransaction();
        try {
            control.getActiveTransactionIds(t1);
            Assert.fail();
        } catch (RuntimeException ex) {
            Assert.assertTrue(ex.getMessage().startsWith("expected max id of"));
        }
    }

    // Permission tests

    @Test
    @Ignore("disabled until DDL changes are merged")
    public void forbidWrites() throws IOException {
        final TransactionId t1 = control.beginTransaction();
        Assert.assertTrue(control.forbidWrites(storeSetup.getPersonTableName(), t1));
        try {
            insertAge(t1, "joe69", 20);
            Assert.fail();
        } catch (PermissionFailure ex) {
            Assert.assertTrue(ex.getMessage().startsWith("permission fail"));
        } catch (RetriesExhaustedWithDetailsException e) {
            assertPermissionFailure(e);
        }
    }

    @Test
    @Ignore("disabled until DDL changes are merged")
    public void forbidWritesAfterWritten() throws IOException {
        final TransactionId t1 = control.beginTransaction();
        insertAge(t1, "joe69", 20);
        Assert.assertFalse(control.forbidWrites(storeSetup.getPersonTableName(), t1));
    }

    private void assertPermissionFailure(RetriesExhaustedWithDetailsException e) {
        Assert.assertEquals(1, e.getNumExceptions());
        Assert.assertTrue(e.getMessage().indexOf("permission fail") >= 0);
    }

    // Additive tests

    @Test
    public void additiveWritesSecond() throws IOException {
        final TransactionId t1 = control.beginTransaction();
        insertAge(t1, "joe70", 20);
        final TransactionId t2 = control.beginTransaction();
        final TransactionId t3 = control.beginChildTransaction(t2, true, true, true, null, null, null);
        insertJob(t3, "joe70", "butcher");
        control.commit(t3);
        control.commit(t2);
        control.commit(t1);
        final TransactionId t4 = control.beginTransaction();
        Assert.assertEquals("joe70 age=20 job=butcher", read(t4, "joe70"));
    }

    @Test
    public void additiveWritesFirst() throws IOException {
        final TransactionId t1 = control.beginTransaction();
        final TransactionId t2 = control.beginChildTransaction(t1, true, true, true, null, null, null);
        insertJob(t2, "joe70", "butcher");
        final TransactionId t3 = control.beginTransaction();
        insertAge(t3, "joe70", 20);
        control.commit(t3);
        control.commit(t2);
        control.commit(t1);
        final TransactionId t4 = control.beginTransaction();
        Assert.assertEquals("joe70 age=20 job=butcher", read(t4, "joe70"));
    }

    // Commit & begin together tests

    @Test
    public void testCommitAndBeginSeparate() throws IOException {
        final TransactionId t1 = control.beginTransaction();
        final TransactionId t2 = control.beginTransaction();
        control.commit(t1);
        final TransactionId t3 = control.beginChildTransaction(t2, true, true, false, null, null, null);
        Assert.assertEquals(t1.getId() + 1, t2.getId());
        // next ID burned for commit
        Assert.assertEquals(t1.getId() + 3, t3.getId());
    }

    @Test
    public void testCommitAndBeginTogether() throws IOException {
        final TransactionId t1 = control.beginTransaction();
        final TransactionId t2 = control.beginTransaction();
        final TransactionId t3 = control.beginChildTransaction(t2, true, true, false, null, null, t1);
        Assert.assertEquals(t1.getId() + 1, t2.getId());
        // no ID burned for commit
        Assert.assertEquals(t1.getId() + 2, t3.getId());
    }

    @Test
    public void testCommitNonRootAndBeginTogether() throws IOException {
        final TransactionId t1 = control.beginTransaction();
        final TransactionId t2 = control.beginChildTransaction(t1, true);
        final TransactionId t3 = control.beginTransaction();
        try {
            control.beginChildTransaction(t3, true, true, false, null, null, t2);
            Assert.fail();
        } catch (RuntimeException ex) {
            Assert.assertTrue(ex.getMessage().startsWith("Cannot begin a child transaction at the time a non-root transaction commits:"));
        }
    }

    @Test
    @Ignore
    public void test() throws IOException {
        final TransactionId t1 = control.beginTransaction();
        insertAge(t1, "joe70", 20);
        control.commit(t1);

        final LDataLib dataLib2 = new LDataLib();
        final Clock clock2 = new IncrementingClock(1000);
        final LStore store2 = new LStore(clock2);

        final Transcoder transcoder = new Transcoder() {
            @Override
            public Object transcode(Object data) {
                return data;
            }

            @Override
            public Object transcodeKey(Object key) {
                return storeSetup.getDataLib().decode((byte[])key, String.class);
            }

            @Override
            public Object transcodeFamily(Object family) {
                return storeSetup.getDataLib().decode((byte[])family, String.class);
            }

            @Override
            public Object transcodeQualifier(Object qualifier) {
                return storeSetup.getDataLib().decode((byte[])qualifier, String.class);
            }
        };
        final Transcoder transcoder2 = new Transcoder() {
            @Override
            public Object transcode(Object data) {
                return data;
            }

            @Override
            public Object transcodeKey(Object key) {
                return dataLib2.decode((byte[])key, String.class);
            }

            @Override
            public Object transcodeFamily(Object family) {
                return dataLib2.decode((byte[])family, String.class);
            }

            @Override
            public Object transcodeQualifier(Object qualifier) {
                return dataLib2.decode((byte[])qualifier, String.class);
            }
        };
        final Translator translator = new Translator(storeSetup.getDataLib(), storeSetup.getReader(), dataLib2, store2, store2, transcoder, transcoder2);

        translator.translate(storeSetup.getPersonTableName());
        final LGet get = dataLib2.newGet(dataLib2.encode("joe70"), null, null, null);
        final LTable table2 = store2.open(storeSetup.getPersonTableName());
        final Result result = store2.get(table2, get);
        Assert.assertNotNull(result);
        final List<KeyValue> results = dataLib2.listResult(result);
        Assert.assertEquals(2, results.size());
        final KeyValue kv = results.get(1);
        Assert.assertEquals("joe70", Bytes.toString(kv.getRow()));
        Assert.assertEquals("V", Bytes.toString(kv.getFamily()));
        Assert.assertEquals("age", Bytes.toString(kv.getQualifier()));
        Assert.assertEquals(1L, kv.getTimestamp());
        Assert.assertEquals(20, Bytes.toInt(kv.getValue()));

    }
}