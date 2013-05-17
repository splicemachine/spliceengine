package com.splicemachine.si;

import com.google.common.base.Function;
import com.splicemachine.constants.SIConstants;
import com.splicemachine.si.api.FilterState;
import com.splicemachine.si.api.TransactionId;
import com.splicemachine.si.api.Transactor;
import com.splicemachine.si.data.api.SDataLib;
import com.splicemachine.si.data.api.SGet;
import com.splicemachine.si.data.api.SScan;
import com.splicemachine.si.data.api.STable;
import com.splicemachine.si.data.api.STableReader;
import com.splicemachine.si.data.light.IncrementingClock;
import com.splicemachine.si.data.light.LStore;
import com.splicemachine.si.impl.RollForwardAction;
import com.splicemachine.si.impl.RollForwardQueue;
import com.splicemachine.si.impl.SICompactionState;
import com.splicemachine.si.impl.SITransactionId;
import com.splicemachine.si.impl.Tracer;
import com.splicemachine.si.impl.Transaction;
import com.splicemachine.si.impl.TransactionStatus;
import com.splicemachine.si.impl.WriteConflict;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class SITransactorTest extends SIConstants {
    boolean useSimple = true;

    StoreSetup storeSetup;
    TransactorSetup transactorSetup;
    Transactor transactor;

    void baseSetUp() {
        transactor = transactorSetup.transactor;
        transactorSetup.rollForwardQueue = new RollForwardQueue(new RollForwardAction() {
            @Override
            public void rollForward(long transactionId, List rowList) throws IOException {
                final STableReader reader = storeSetup.getReader();
                STable testSTable = reader.open(storeSetup.getPersonTableName());
                transactor.rollForward(testSTable, transactionId, rowList);
            }
        }, 10, 100, 1000);
    }

    @Before
    public void setUp() {
        RollForwardQueue.scheduler = Executors.newScheduledThreadPool(1);
        storeSetup = new LStoreSetup();
        transactorSetup = new TransactorSetup(storeSetup, true);
        baseSetUp();
    }

    @After
    public void tearDown() throws Exception {
    }

    private void insertAge(TransactionId transactionId, String name, int age) throws IOException {
        insertAgeDirect(useSimple, transactorSetup, storeSetup, transactionId, name, age);
    }

    private void insertJob(TransactionId transactionId, String name, String job) throws IOException {
        insertJobDirect(useSimple, transactorSetup, storeSetup, transactionId, name, job);
    }

    private void deleteRow(TransactionId transactionId, String name) throws IOException {
        deleteRowDirect(useSimple, transactorSetup, storeSetup, transactionId, name);
    }

    private String read(TransactionId transactionId, String name) throws IOException {
        return readAgeDirect(useSimple, transactorSetup, storeSetup, transactionId, name);
    }

    private String scan(TransactionId transactionId, String name) throws IOException {
        return scanAgeDirect(useSimple, transactorSetup, storeSetup, transactionId, name);
    }

    private String scanNoColumns(TransactionId transactionId, String name, boolean deleted) throws IOException {
        return scanNoColumnsDirect(useSimple, transactorSetup, storeSetup, transactionId, name, deleted);
    }

    private String scanAll(TransactionId transactionId, String startKey, String stopKey) throws IOException {
        return scanAllDirect(useSimple, transactorSetup, storeSetup, transactionId, startKey, stopKey);
    }

    static void insertAgeDirect(boolean useSimple, TransactorSetup transactorSetup, StoreSetup storeSetup,
                                TransactionId transactionId, String name, int age) throws IOException {
        insertField(useSimple, transactorSetup, storeSetup, transactionId, name, transactorSetup.ageQualifier, age);
    }

    static void insertJobDirect(boolean useSimple, TransactorSetup transactorSetup, StoreSetup storeSetup,
                                TransactionId transactionId, String name, String job) throws IOException {
        insertField(useSimple, transactorSetup, storeSetup, transactionId, name, transactorSetup.jobQualifier, job);
    }

    private static void insertField(boolean useSimple, TransactorSetup transactorSetup, StoreSetup storeSetup,
                                    TransactionId transactionId, String name, Object qualifier, Object fieldValue)
            throws IOException {
        final SDataLib dataLib = storeSetup.getDataLib();
        final STableReader reader = storeSetup.getReader();

        Object key = dataLib.newRowKey(new Object[]{name});
        Object put = dataLib.newPut(key);
        dataLib.addKeyValueToPut(put, transactorSetup.family, qualifier, null, dataLib.encode(fieldValue));
        transactorSetup.clientTransactor.initializePut(transactionId.getTransactionIdString(), put);

        processPutDirect(useSimple, transactorSetup, storeSetup, reader, put);
    }

    static void deleteRowDirect(boolean useSimple, TransactorSetup transactorSetup, StoreSetup storeSetup,
                                TransactionId transactionId, String name) throws IOException {
        final SDataLib dataLib = storeSetup.getDataLib();
        final STableReader reader = storeSetup.getReader();

        Object key = dataLib.newRowKey(new Object[]{name});
        Object deletePut = transactorSetup.clientTransactor.createDeletePut(transactionId, key);
        processPutDirect(useSimple, transactorSetup, storeSetup, reader, deletePut);
    }

    private static void processPutDirect(boolean useSimple,
                                         TransactorSetup transactorSetup, StoreSetup storeSetup, STableReader reader,
                                         Object put) throws IOException {
        STable testSTable = reader.open(storeSetup.getPersonTableName());
        try {
            if (useSimple) {
                Assert.assertTrue(transactorSetup.transactor.processPut(testSTable, transactorSetup.rollForwardQueue, put));
            } else {
                storeSetup.getWriter().write(testSTable, put);
            }
        } finally {
            reader.close(testSTable);
        }
    }

    static String readAgeDirect(boolean useSimple, TransactorSetup transactorSetup, StoreSetup storeSetup,
                                TransactionId transactionId, String name) throws IOException {
        final SDataLib dataLib = storeSetup.getDataLib();
        final STableReader reader = storeSetup.getReader();

        Object key = dataLib.newRowKey(new Object[]{name});
        SGet get = dataLib.newGet(key, null, null, null);
        transactorSetup.clientTransactor.initializeGet(transactionId.getTransactionIdString(), get);
        STable testSTable = reader.open(storeSetup.getPersonTableName());
        try {
            Object rawTuple = reader.get(testSTable, get);
            return readRawTuple(useSimple, transactorSetup, transactionId, name, dataLib, rawTuple, true, false);
        } finally {
            reader.close(testSTable);
        }
    }

    static String scanAgeDirect(boolean useSimple, TransactorSetup transactorSetup, StoreSetup storeSetup,
                                TransactionId transactionId, String name) throws IOException {
        final SDataLib dataLib = storeSetup.getDataLib();
        final STableReader reader = storeSetup.getReader();

        Object key = dataLib.newRowKey(new Object[]{name});
        SScan get = dataLib.newScan(key, key, null, null, null);
        transactorSetup.clientTransactor.initializeScan(transactionId.getTransactionIdString(), get);
        STable testSTable = reader.open(storeSetup.getPersonTableName());
        try {
            Iterator results = reader.scan(testSTable, get);
            Assert.assertTrue(results.hasNext());
            Object rawTuple = results.next();
            Assert.assertTrue(!results.hasNext());
            return readRawTuple(useSimple, transactorSetup, transactionId, name, dataLib, rawTuple, false, false);
        } finally {
            reader.close(testSTable);
        }
    }

    static String scanNoColumnsDirect(boolean useSimple, TransactorSetup transactorSetup, StoreSetup storeSetup,
                                      TransactionId transactionId, String name, boolean deleted) throws IOException {
        final SDataLib dataLib = storeSetup.getDataLib();
        final STableReader reader = storeSetup.getReader();

        Object key = dataLib.newRowKey(new Object[]{name});
        SScan scan = dataLib.newScan(key, key, new ArrayList(), null, null);
        transactorSetup.clientTransactor.initializeScan(transactionId.getTransactionIdString(), scan, true);
        transactorSetup.transactor.preProcessScan(scan);
        STable testSTable = reader.open(storeSetup.getPersonTableName());
        try {
            Iterator results = reader.scan(testSTable, scan);
            if (deleted) {
            } else {
                Assert.assertTrue(results.hasNext());
            }
            Object rawTuple = results.next();
            Assert.assertTrue(!results.hasNext());
            return readRawTuple(useSimple, transactorSetup, transactionId, name, dataLib, rawTuple, false, true);
        } finally {
            reader.close(testSTable);
        }
    }

    static String scanAllDirect(boolean useSimple, TransactorSetup transactorSetup, StoreSetup storeSetup,
                                TransactionId transactionId, String startKey, String stopKey) throws IOException {
        final SDataLib dataLib = storeSetup.getDataLib();
        final STableReader reader = storeSetup.getReader();

        Object key = dataLib.newRowKey(new Object[]{startKey});
        Object endKey = dataLib.newRowKey(new Object[]{stopKey});
        SScan get = dataLib.newScan(key, endKey, null, null, null);
        transactorSetup.clientTransactor.initializeScan(transactionId.getTransactionIdString(), get);
        STable testSTable = reader.open(storeSetup.getPersonTableName());
        try {
            Iterator results = reader.scan(testSTable, get);
            StringBuilder result = new StringBuilder();
            while (results.hasNext()) {
                final Object value = results.next();
                final String name = (String) dataLib.decode(dataLib.getResultKey(value), String.class);
                final String s = readRawTuple(useSimple, transactorSetup, transactionId, name, dataLib, value, false, false);
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

    private static String readRawTuple(boolean useSimple, TransactorSetup transactorSetup, TransactionId transactionId,
                                       String name, SDataLib dataLib, Object rawTuple, boolean singleRowRead,
                                       boolean siOnly) throws IOException {
        if (rawTuple != null) {
            Object result = rawTuple;
            if (useSimple) {
                final FilterState filterState;
                try {
                    filterState = transactorSetup.transactor.newFilterState(transactorSetup.rollForwardQueue, transactionId, siOnly);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                result = transactorSetup.transactor.filterResult(filterState, rawTuple);
            }
            if (result != null) {
                return resultToStringDirect(transactorSetup, name, dataLib, result);
            }
        }
        if (singleRowRead) {
            return name + " age=" + null + " job=" + null;
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
        TransactionId t1 = transactor.beginTransaction(true, false, false);
        Assert.assertEquals("joe9 age=null job=null", read(t1, "joe9"));
        insertAge(t1, "joe9", 20);
        Assert.assertEquals("joe9 age=20 job=null", read(t1, "joe9"));
        transactor.commit(t1);

        TransactionId t2 = transactor.beginTransaction(true, false, false);
        Assert.assertEquals("joe9 age=20 job=null", read(t2, "joe9"));
    }

    @Test
    public void writeReadOverlap() throws IOException {
        TransactionId t1 = transactor.beginTransaction(true, false, false);
        Assert.assertEquals("joe8 age=null job=null", read(t1, "joe8"));
        insertAge(t1, "joe8", 20);
        Assert.assertEquals("joe8 age=20 job=null", read(t1, "joe8"));

        TransactionId t2 = transactor.beginTransaction(true, false, false);
        Assert.assertEquals("joe8 age=20 job=null", read(t1, "joe8"));
        Assert.assertEquals("joe8 age=null job=null", read(t2, "joe8"));
        transactor.commit(t1);
        Assert.assertEquals("joe8 age=null job=null", read(t2, "joe8"));
    }

    @Test
    public void writeWrite() throws IOException {
        TransactionId t1 = transactor.beginTransaction(true, false, false);
        insertAge(t1, "joe", 20);
        Assert.assertEquals("joe age=20 job=null", read(t1, "joe"));
        transactor.commit(t1);

        TransactionId t2 = transactor.beginTransaction(true, false, false);
        Assert.assertEquals("joe age=20 job=null", read(t2, "joe"));
        insertAge(t2, "joe", 30);
        Assert.assertEquals("joe age=30 job=null", read(t2, "joe"));
        transactor.commit(t2);
    }

    @Test
    public void writeWriteOverlap() throws IOException {
        TransactionId t1 = transactor.beginTransaction(true, false, false);
        Assert.assertEquals("joe2 age=null job=null", read(t1, "joe2"));
        insertAge(t1, "joe2", 20);
        Assert.assertEquals("joe2 age=20 job=null", read(t1, "joe2"));

        TransactionId t2 = transactor.beginTransaction(true, false, false);
        Assert.assertEquals("joe2 age=20 job=null", read(t1, "joe2"));
        Assert.assertEquals("joe2 age=null job=null", read(t2, "joe2"));
        try {
            insertAge(t2, "joe2", 30);
            Assert.fail();
        } catch (WriteConflict e) {
        } catch (RetriesExhaustedWithDetailsException e) {
            assertWriteConflict(e);
        }
        Assert.assertEquals("joe2 age=20 job=null", read(t1, "joe2"));
        transactor.commit(t1);
        try {
            transactor.commit(t2);
            Assert.fail();
        } catch (DoNotRetryIOException dnrio) {
            Assert.assertTrue(dnrio.getMessage().startsWith("transaction is not ACTIVE"));
        }
    }

    @Test
    public void readAfterCommit() throws IOException {
        TransactionId t1 = transactor.beginTransaction(true, false, false);
        insertAge(t1, "joe3", 20);
        transactor.commit(t1);
        Assert.assertEquals("joe3 age=20 job=null", read(t1, "joe3"));
    }

    @Test
    public void rollbackAfterCommit() throws IOException {
        TransactionId t1 = transactor.beginTransaction(true, false, false);
        insertAge(t1, "joe50", 20);
        transactor.commit(t1);
        transactor.rollback(t1);
        Assert.assertEquals("joe50 age=20 job=null", read(t1, "joe50"));
    }

    @Test
    public void writeScan() throws IOException {
        TransactionId t1 = transactor.beginTransaction(true, false, false);
        Assert.assertEquals("joe4 age=null job=null", read(t1, "joe4"));
        insertAge(t1, "joe4", 20);
        Assert.assertEquals("joe4 age=20 job=null", read(t1, "joe4"));
        transactor.commit(t1);

        TransactionId t2 = transactor.beginTransaction(true, false, false);
        Assert.assertEquals("joe4 age=20 job=null", scan(t2, "joe4"));

        Assert.assertEquals("joe4 age=20 job=null", read(t2, "joe4"));
    }

    @Test
    public void writeScanNoColumns() throws IOException {
        TransactionId t1 = transactor.beginTransaction(true, false, false);
        insertAge(t1, "joe84", 20);
        transactor.commit(t1);

        TransactionId t2 = transactor.beginTransaction(true, false, false);
        // reading si only (i.e. no data columns) returns the rows but none of the "user" data
        Assert.assertEquals("joe84 age=null job=null", scanNoColumns(t2, "joe84", false));
    }

    @Test
    public void writeDeleteScanNoColumns() throws IOException {
        TransactionId t1 = transactor.beginTransaction(true, false, false);
        insertAge(t1, "joe85", 20);
        transactor.commit(t1);

        TransactionId t2 = transactor.beginTransaction(true, false, false);
        deleteRow(t2, "joe85");
        transactor.commit(t2);


        TransactionId t3 = transactor.beginTransaction(true, false, false);
        // reading si only (i.e. no data columns) returns the rows but none of the "user" data
        Assert.assertEquals("", scanNoColumns(t3, "joe85", true));
    }

    @Test
    public void writeScanMultipleRows() throws IOException {
        TransactionId t1 = transactor.beginTransaction(true, false, false);
        insertAge(t1, "17joe", 20);
        insertAge(t1, "17bob", 30);
        insertAge(t1, "17boe", 40);
        insertAge(t1, "17tom", 50);
        transactor.commit(t1);

        TransactionId t2 = transactor.beginTransaction(true, false, false);
        String expected = "17bob age=30 job=null\n" +
                "17boe age=40 job=null\n" +
                "17joe age=20 job=null\n" +
                "17tom age=50 job=null\n";
        Assert.assertEquals(expected, scanAll(t2, "17a", "17z"));
    }

    @Test
    public void writeWriteRead() throws IOException {
        TransactionId t1 = transactor.beginTransaction(true, false, false);
        insertAge(t1, "joe5", 20);
        Assert.assertEquals("joe5 age=20 job=null", read(t1, "joe5"));
        transactor.commit(t1);

        TransactionId t2 = transactor.beginTransaction(true, false, false);
        Assert.assertEquals("joe5 age=20 job=null", read(t2, "joe5"));
        insertJob(t2, "joe5", "baker");
        Assert.assertEquals("joe5 age=20 job=baker", read(t2, "joe5"));
        transactor.commit(t2);

        TransactionId t3 = transactor.beginTransaction(true, false, false);
        Assert.assertEquals("joe5 age=20 job=baker", read(t3, "joe5"));
    }

    @Test
    public void multipleWritesSameTransaction() throws IOException {
        TransactionId t1 = transactor.beginTransaction(true, false, false);
        Assert.assertEquals("joe16 age=null job=null", read(t1, "joe16"));
        insertAge(t1, "joe16", 20);
        Assert.assertEquals("joe16 age=20 job=null", read(t1, "joe16"));

        insertAge(t1, "joe16", 21);
        Assert.assertEquals("joe16 age=21 job=null", read(t1, "joe16"));

        insertAge(t1, "joe16", 22);
        Assert.assertEquals("joe16 age=22 job=null", read(t1, "joe16"));
        transactor.commit(t1);

        TransactionId t2 = transactor.beginTransaction(true, false, false);
        Assert.assertEquals("joe16 age=22 job=null", read(t2, "joe16"));
        transactor.commit(t2);
    }

    @Test
    public void manyWritesManyRollbacksRead() throws IOException {
        TransactionId t1 = transactor.beginTransaction(true, false, false);
        insertAge(t1, "joe6", 20);
        transactor.commit(t1);

        TransactionId t2 = transactor.beginTransaction(true, false, false);
        insertJob(t2, "joe6", "baker");
        transactor.commit(t2);

        TransactionId t3 = transactor.beginTransaction(true, false, false);
        insertJob(t3, "joe6", "butcher");
        transactor.commit(t3);

        TransactionId t4 = transactor.beginTransaction(true, false, false);
        insertJob(t4, "joe6", "blacksmith");
        transactor.commit(t4);

        TransactionId t5 = transactor.beginTransaction(true, false, false);
        insertJob(t5, "joe6", "carter");
        transactor.commit(t5);

        TransactionId t6 = transactor.beginTransaction(true, false, false);
        insertJob(t6, "joe6", "farrier");
        transactor.commit(t6);

        TransactionId t7 = transactor.beginTransaction(true, false, false);
        insertAge(t7, "joe6", 27);
        transactor.rollback(t7);

        TransactionId t8 = transactor.beginTransaction(true, false, false);
        insertAge(t8, "joe6", 28);
        transactor.rollback(t8);

        TransactionId t9 = transactor.beginTransaction(true, false, false);
        insertAge(t9, "joe6", 29);
        transactor.rollback(t9);

        TransactionId t10 = transactor.beginTransaction(true, false, false);
        insertAge(t10, "joe6", 30);
        transactor.rollback(t10);

        TransactionId t11 = transactor.beginTransaction(true, false, false);
        Assert.assertEquals("joe6 age=20 job=farrier", read(t11, "joe6"));
    }

    @Test
    public void writeDelete() throws IOException {
        TransactionId t1 = transactor.beginTransaction(true, false, false);
        insertAge(t1, "joe10", 20);
        Assert.assertEquals("joe10 age=20 job=null", read(t1, "joe10"));
        transactor.commit(t1);

        TransactionId t2 = transactor.beginTransaction(true, false, false);
        Assert.assertEquals("joe10 age=20 job=null", read(t2, "joe10"));
        deleteRow(t2, "joe10");
        Assert.assertEquals("joe10 age=null job=null", read(t2, "joe10"));
        transactor.commit(t2);
    }

    @Test
    public void writeDeleteRead() throws IOException {
        TransactionId t1 = transactor.beginTransaction(true, false, false);
        insertAge(t1, "joe11", 20);
        transactor.commit(t1);

        TransactionId t2 = transactor.beginTransaction(true, false, false);
        deleteRow(t2, "joe11");
        transactor.commit(t2);

        TransactionId t3 = transactor.beginTransaction(true, false, false);
        Assert.assertEquals("joe11 age=null job=null", read(t3, "joe11"));
        transactor.commit(t3);
    }

    @Test
    public void writeDeleteOverlap() throws IOException {
        TransactionId t1 = transactor.beginTransaction(true, false, false);
        insertAge(t1, "joe12", 20);

        TransactionId t2 = transactor.beginTransaction(true, false, false);
        try {
            deleteRow(t2, "joe12");
            Assert.fail();
        } catch (WriteConflict e) {
        } catch (RetriesExhaustedWithDetailsException e) {
            assertWriteConflict(e);
        }
        Assert.assertEquals("joe12 age=20 job=null", read(t1, "joe12"));
        Assert.assertEquals("joe12 age=20 job=null", read(t1, "joe12"));
        transactor.commit(t1);
        try {
            transactor.commit(t2);
            Assert.fail();
        } catch (DoNotRetryIOException dnrio) {
            Assert.assertTrue(dnrio.getMessage().startsWith("transaction is not ACTIVE"));
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
        TransactionId t0 = transactor.beginTransaction(true, false, false);
        insertAge(t0, "jo13", 20);
        transactor.commit(t0);

        TransactionId t1 = transactor.beginTransaction(true, false, false);
        deleteRow(t1, "joe13");

        TransactionId t2 = transactor.beginTransaction(true, false, false);
        try {
            insertAge(t2, "joe13", 21);
            Assert.fail();
        } catch (WriteConflict e) {
        } catch (RetriesExhaustedWithDetailsException e) {
            assertWriteConflict(e);
        }
        Assert.assertEquals("joe13 age=null job=null", read(t1, "joe13"));
        transactor.commit(t1);
        try {
            transactor.commit(t2);
            Assert.fail();
        } catch (DoNotRetryIOException dnrio) {
            Assert.assertTrue(dnrio.getMessage().startsWith("transaction is not ACTIVE"));
        }

        TransactionId t3 = transactor.beginTransaction(true, false, false);
        Assert.assertEquals("joe13 age=null job=null", read(t3, "joe13"));
        transactor.commit(t3);
    }

    @Test
    public void writeWriteDeleteWriteRead() throws IOException {
        TransactionId t0 = transactor.beginTransaction(true, false, false);
        insertAge(t0, "joe14", 20);
        transactor.commit(t0);

        TransactionId t1 = transactor.beginTransaction(true, false, false);
        insertJob(t1, "joe14", "baker");
        transactor.commit(t1);

        TransactionId t2 = transactor.beginTransaction(true, false, false);
        deleteRow(t2, "joe14");
        transactor.commit(t2);

        TransactionId t3 = transactor.beginTransaction(true, false, false);
        insertJob(t3, "joe14", "smith");
        Assert.assertEquals("joe14 age=null job=smith", read(t3, "joe14"));
        transactor.commit(t3);

        TransactionId t4 = transactor.beginTransaction(true, false, false);
        Assert.assertEquals("joe14 age=null job=smith", read(t4, "joe14"));
        transactor.commit(t4);
    }

    @Test
    public void writeWriteDeleteWriteDeleteWriteRead() throws IOException {
        TransactionId t0 = transactor.beginTransaction(true, false, false);
        insertAge(t0, "joe15", 20);
        transactor.commit(t0);

        TransactionId t1 = transactor.beginTransaction(true, false, false);
        insertJob(t1, "joe15", "baker");
        transactor.commit(t1);

        TransactionId t2 = transactor.beginTransaction(true, false, false);
        deleteRow(t2, "joe15");
        transactor.commit(t2);

        TransactionId t3 = transactor.beginTransaction(true, false, false);
        insertJob(t3, "joe15", "smith");
        Assert.assertEquals("joe15 age=null job=smith", read(t3, "joe15"));
        transactor.commit(t3);

        TransactionId t4 = transactor.beginTransaction(true, false, false);
        deleteRow(t4, "joe15");
        transactor.commit(t4);

        TransactionId t5 = transactor.beginTransaction(true, false, false);
        insertAge(t5, "joe15", 21);
        transactor.commit(t5);

        TransactionId t6 = transactor.beginTransaction(true, false, false);
        Assert.assertEquals("joe15 age=21 job=null", read(t6, "joe15"));
        transactor.commit(t6);
    }

    @Test
    public void writeManyDeleteOneGets() throws IOException {
        TransactionId t1 = transactor.beginTransaction(true, false, false);
        insertAge(t1, "joe47", 20);
        insertAge(t1, "toe47", 30);
        insertAge(t1, "boe47", 40);
        insertAge(t1, "moe47", 50);
        insertAge(t1, "zoe47", 60);
        transactor.commit(t1);

        TransactionId t2 = transactor.beginTransaction(true, false, false);
        deleteRow(t2, "moe47");
        transactor.commit(t2);

        TransactionId t3 = transactor.beginTransaction(true, false, false);
        Assert.assertEquals("joe47 age=20 job=null", read(t3, "joe47"));
        Assert.assertEquals("toe47 age=30 job=null", read(t3, "toe47"));
        Assert.assertEquals("boe47 age=40 job=null", read(t3, "boe47"));
        Assert.assertEquals("moe47 age=null job=null", read(t3, "moe47"));
        Assert.assertEquals("zoe47 age=60 job=null", read(t3, "zoe47"));
    }

    @Test
    public void writeManyDeleteOneScan() throws IOException {
        TransactionId t1 = transactor.beginTransaction(true, false, false);
        insertAge(t1, "48joe", 20);
        insertAge(t1, "48toe", 30);
        insertAge(t1, "48boe", 40);
        insertAge(t1, "48moe", 50);
        insertAge(t1, "48xoe", 60);
        transactor.commit(t1);

        TransactionId t2 = transactor.beginTransaction(true, false, false);
        deleteRow(t2, "48moe");
        transactor.commit(t2);

        TransactionId t3 = transactor.beginTransaction(true, false, false);
        String expected = "48boe age=40 job=null\n" +
                "48joe age=20 job=null\n" +
                "48toe age=30 job=null\n" +
                "48xoe age=60 job=null\n";
        Assert.assertEquals(expected, scanAll(t3, "48a", "48z"));
    }

    @Test
    public void writeDeleteSameTransaction() throws IOException {
        TransactionId t0 = transactor.beginTransaction(true, false, false);
        insertAge(t0, "joe81", 19);
        transactor.commit(t0);

        TransactionId t1 = transactor.beginTransaction(true, false, false);
        insertAge(t1, "joe81", 20);
        deleteRow(t1, "joe81");
        Assert.assertEquals("joe81 age=null job=null", read(t1, "joe81"));
        transactor.commit(t1);

        TransactionId t2 = transactor.beginTransaction(true, false, false);
        Assert.assertEquals("joe81 age=null job=null", read(t2, "joe81"));
        transactor.commit(t2);
    }

    @Test
    public void deleteWriteSameTransaction() throws IOException {
        TransactionId t0 = transactor.beginTransaction(true, false, false);
        insertAge(t0, "joe82", 19);
        transactor.commit(t0);

        TransactionId t1 = transactor.beginTransaction(true, false, false);
        deleteRow(t1, "joe82");
        insertAge(t1, "joe82", 20);
        Assert.assertEquals("joe82 age=20 job=null", read(t1, "joe82"));
        transactor.commit(t1);

        TransactionId t2 = transactor.beginTransaction(true, false, false);
        Assert.assertEquals("joe82 age=20 job=null", read(t2, "joe82"));
        transactor.commit(t2);
    }

    @Test
    public void fourTransactions() throws Exception {
        TransactionId t1 = transactor.beginTransaction(true, false, false);
        insertAge(t1, "joe7", 20);
        transactor.commit(t1);

        TransactionId t2 = transactor.beginTransaction(true, false, false);
        Assert.assertEquals("joe7 age=20 job=null", read(t2, "joe7"));
        insertAge(t2, "joe7", 30);
        Assert.assertEquals("joe7 age=30 job=null", read(t2, "joe7"));

        TransactionId t3 = transactor.beginTransaction(true, false, false);
        Assert.assertEquals("joe7 age=20 job=null", read(t3, "joe7"));

        transactor.commit(t2);

        TransactionId t4 = transactor.beginTransaction(true, false, false);
        Assert.assertEquals("joe7 age=30 job=null", read(t4, "joe7"));
        //System.out.println(store);
    }

    @Test
    public void writeReadOnly() throws IOException {
        TransactionId t1 = transactor.beginTransaction(true, false, false);
        insertAge(t1, "joe18", 20);
        transactor.commit(t1);

        TransactionId t2 = transactor.beginTransaction(false, false, false);
        Assert.assertEquals("joe18 age=20 job=null", read(t2, "joe18"));
        try {
            insertAge(t2, "joe18", 21);
            Assert.fail("expected exception performing a write on a read-only transaction");
        } catch (IOException e) {
        }
    }

    @Test
    public void writeReadCommitted() throws IOException {
        TransactionId t1 = transactor.beginTransaction(true, false, false);
        insertAge(t1, "joe19", 20);
        transactor.commit(t1);

        TransactionId t2 = transactor.beginTransaction(false, false, true);
        Assert.assertEquals("joe19 age=20 job=null", read(t2, "joe19"));
    }

    @Test
    public void writeReadCommittedOverlap() throws IOException {
        TransactionId t1 = transactor.beginTransaction(true, false, false);
        insertAge(t1, "joe20", 20);

        TransactionId t2 = transactor.beginTransaction(false, false, true);

        Assert.assertEquals("joe20 age=null job=null", read(t2, "joe20"));
        transactor.commit(t1);
        Assert.assertEquals("joe20 age=20 job=null", read(t2, "joe20"));
    }

    @Test
    public void writeReadDirty() throws IOException {
        TransactionId t1 = transactor.beginTransaction(true, false, false);
        insertAge(t1, "joe22", 20);
        transactor.commit(t1);

        TransactionId t2 = transactor.beginTransaction(false, true, true);
        Assert.assertEquals("joe22 age=20 job=null", read(t2, "joe22"));
    }

    @Test
    public void writeReadDirtyOverlap() throws IOException {
        TransactionId t1 = transactor.beginTransaction(true, false, false);
        insertAge(t1, "joe21", 20);

        TransactionId t2 = transactor.beginTransaction(false, true, true);

        Assert.assertEquals("joe21 age=20 job=null", read(t2, "joe21"));
        transactor.commit(t1);
        Assert.assertEquals("joe21 age=20 job=null", read(t2, "joe21"));
    }

    @Test
    public void writeRollbackWriteReadDirtyOverlap() throws IOException {
        TransactionId t1 = transactor.beginTransaction(true, false, false);
        insertAge(t1, "joe23", 20);
        transactor.commit(t1);

        TransactionId t2 = transactor.beginTransaction(true, false, false);
        insertAge(t2, "joe23", 21);

        TransactionId t3 = transactor.beginTransaction(false, true, true);
        Assert.assertEquals("joe23 age=21 job=null", read(t3, "joe23"));

        transactor.rollback(t2);
        Assert.assertEquals("joe23 age=20 job=null", read(t3, "joe23"));

        TransactionId t4 = transactor.beginTransaction(true, false, false);
        insertAge(t4, "joe23", 22);
        Assert.assertEquals("joe23 age=22 job=null", read(t3, "joe23"));
    }

    @Test
    public void nestedReadOnlyIds() {
        final SITransactionId id = new SITransactionId(100L, true);
        Assert.assertEquals(100L, id.getId());
        Assert.assertEquals("100.IRO", id.getTransactionIdString());
        final SITransactionId id2 = new SITransactionId("200.IRO");
        Assert.assertEquals(200L, id2.getId());
        Assert.assertEquals("200.IRO", id2.getTransactionIdString());
    }

    @Test
    public void childDependentTransactionWriteRollbackRead() throws IOException {
        TransactionId t1 = transactor.beginTransaction(true, false, false);
        insertAge(t1, "joe24", 20);
        TransactionId t2 = transactor.beginChildTransaction(t1, true, true, null, null);
        insertAge(t2, "joe24", 21);
        Assert.assertEquals("joe24 age=21 job=null", read(t1, "joe24"));
        transactor.rollback(t2);
        Assert.assertEquals("joe24 age=20 job=null", read(t1, "joe24"));
        transactor.commit(t1);

        TransactionId t3 = transactor.beginTransaction(false, false, false);
        Assert.assertEquals("joe24 age=20 job=null", read(t3, "joe24"));
    }

    @Test
    public void childDependentTransactionWriteCommitRollbackRead() throws IOException {
        TransactionId t1 = transactor.beginTransaction(true, false, false);
        TransactionId t2 = transactor.beginChildTransaction(t1, true, true, null, null);
        insertAge(t2, "joe51", 21);
        transactor.commit(t2);
        transactor.rollback(t2);
        Assert.assertEquals("joe51 age=21 job=null", read(t1, "joe51"));
        transactor.commit(t1);
    }

    @Test
    public void childIndependentTransactionWriteCommitRollbackRead() throws IOException {
        TransactionId t1 = transactor.beginTransaction(true, false, false);
        TransactionId t2 = transactor.beginChildTransaction(t1, true, true, null, null);
        insertAge(t2, "joe52", 21);
        transactor.commit(t2);
        transactor.rollback(t2);
        Assert.assertEquals("joe52 age=21 job=null", read(t1, "joe52"));
        transactor.commit(t1);
    }

    @Test
    public void childDependentSeesParentWrites() throws IOException {
        TransactionId t1 = transactor.beginTransaction(true, false, false);
        insertAge(t1, "joe40", 20);
        TransactionId t2 = transactor.beginChildTransaction(t1, true, true, null, null);
        Assert.assertEquals("joe40 age=20 job=null", read(t2, "joe40"));
    }

    @Test
    public void childIndependentSeesParentWrites() throws IOException {
        TransactionId t1 = transactor.beginTransaction(true, false, false);
        insertAge(t1, "joe41", 20);
        TransactionId t2 = transactor.beginChildTransaction(t1, false, true, null, null);
        Assert.assertEquals("joe41 age=20 job=null", read(t2, "joe41"));
    }

    @Test
    public void childDependentTransactionWriteRead() throws IOException {
        TransactionId t1 = transactor.beginTransaction(true, false, false);
        insertAge(t1, "joe25", 20);
        TransactionId t2 = transactor.beginChildTransaction(t1, true, true, null, null);
        insertAge(t2, "joe25", 21);
        Assert.assertEquals("joe25 age=21 job=null", read(t1, "joe25"));
        transactor.commit(t2);

        TransactionId t3 = transactor.beginTransaction(false, false, false);
        Assert.assertEquals("joe25 age=null job=null", read(t3, "joe25"));

        Assert.assertEquals("joe25 age=21 job=null", read(t1, "joe25"));
        transactor.commit(t1);

        TransactionId t4 = transactor.beginTransaction(false, false, false);
        Assert.assertEquals("joe25 age=21 job=null", read(t4, "joe25"));
    }

    @Test
    public void childDependentTransactionWithOtherCommitBetweenParentAndChild() throws IOException {
        TransactionId t0 = transactor.beginTransaction(true, false, false);
        insertAge(t0, "joe37", 20);
        transactor.commit(t0);

        TransactionId t1 = transactor.beginTransaction(true, false, false);

        TransactionId otherTransaction = transactor.beginTransaction(true, false, false);
        insertAge(otherTransaction, "joe37", 30);
        transactor.commit(otherTransaction);

        TransactionId t2 = transactor.beginChildTransaction(t1, true, true, null, null);
        Assert.assertEquals("joe37 age=20 job=null", read(t2, "joe37"));
        transactor.commit(t2);
        transactor.commit(t1);
    }

    @Test
    public void childIndependentTransactionWithOtherCommitBetweenParentAndChild() throws IOException {
        TransactionId t0 = transactor.beginTransaction(true, false, false);
        insertAge(t0, "joe38", 20);
        transactor.commit(t0);

        TransactionId t1 = transactor.beginTransaction(true, false, false);

        TransactionId otherTransaction = transactor.beginTransaction(true, false, false);
        insertAge(otherTransaction, "joe38", 30);
        transactor.commit(otherTransaction);

        TransactionId t2 = transactor.beginChildTransaction(t1, false, true, null, true);
        Assert.assertEquals("joe38 age=30 job=null", read(t2, "joe38"));
        transactor.commit(t2);
        transactor.commit(t1);
    }

    @Test
    public void childIndependentTransactionWithReadCommittedOffWithOtherCommitBetweenParentAndChild() throws IOException {
        TransactionId t0 = transactor.beginTransaction(true, false, false);
        insertAge(t0, "joe39", 20);
        transactor.commit(t0);

        TransactionId t1 = transactor.beginTransaction(true, false, false);

        TransactionId otherTransaction = transactor.beginTransaction(true, false, false);
        insertAge(otherTransaction, "joe39", 30);
        transactor.commit(otherTransaction);

        TransactionId t2 = transactor.beginChildTransaction(t1, false, true, null, null);
        Assert.assertEquals("joe39 age=20 job=null", read(t2, "joe39"));
        transactor.commit(t2);
        transactor.commit(t1);
    }

    @Test
    public void multipleChildDependentTransactionWriteRead() throws IOException {
        TransactionId t1 = transactor.beginTransaction(true, false, false);
        insertAge(t1, "joe26", 20);
        TransactionId t2 = transactor.beginChildTransaction(t1, true, true, null, null);
        TransactionId t3 = transactor.beginChildTransaction(t1, true, true, null, null);
        insertAge(t2, "joe26", 21);
        insertJob(t3, "joe26", "baker");
        Assert.assertEquals("joe26 age=21 job=baker", read(t1, "joe26"));
        transactor.commit(t2);

        TransactionId t4 = transactor.beginTransaction(false, false, false);
        Assert.assertEquals("joe26 age=null job=null", read(t4, "joe26"));

        transactor.commit(t3);

        TransactionId t5 = transactor.beginTransaction(false, false, false);
        Assert.assertEquals("joe26 age=null job=null", read(t5, "joe26"));

        Assert.assertEquals("joe26 age=21 job=baker", read(t1, "joe26"));
        transactor.commit(t1);

        TransactionId t6 = transactor.beginTransaction(false, false, false);
        Assert.assertEquals("joe26 age=21 job=baker", read(t6, "joe26"));
    }

    @Test
    public void multipleChildDependentTransactionsRollbackThenWrite() throws IOException {
        TransactionId t1 = transactor.beginTransaction(true, false, false);
        insertAge(t1, "joe45", 20);
        TransactionId t2 = transactor.beginChildTransaction(t1, true, true, null, null);
        insertAge(t2, "joe45", 21);
        TransactionId t3 = transactor.beginChildTransaction(t1, true, true, null, null);
        insertJob(t3, "joe45", "baker");
        Assert.assertEquals("joe45 age=21 job=baker", read(t1, "joe45"));
        Assert.assertEquals("joe45 age=21 job=baker", read(t2, "joe45"));
        Assert.assertEquals("joe45 age=21 job=baker", read(t3, "joe45"));
        transactor.rollback(t2);
        Assert.assertEquals("joe45 age=20 job=baker", read(t1, "joe45"));
        Assert.assertEquals("joe45 age=20 job=baker", read(t3, "joe45"));
        transactor.rollback(t3);
        Assert.assertEquals("joe45 age=20 job=null", read(t1, "joe45"));
        TransactionId t4 = transactor.beginChildTransaction(t1, true, true, null, null);
        insertAge(t4, "joe45", 24);
        Assert.assertEquals("joe45 age=24 job=null", read(t1, "joe45"));
        Assert.assertEquals("joe45 age=24 job=null", read(t4, "joe45"));
        transactor.commit(t4);
        Assert.assertEquals("joe45 age=24 job=null", read(t1, "joe45"));
        Assert.assertEquals("joe45 age=24 job=null", read(t4, "joe45"));

        TransactionId t5 = transactor.beginTransaction(false, false, true);
        Assert.assertEquals("joe45 age=null job=null", read(t5, "joe45"));
        transactor.commit(t1);
        Assert.assertEquals("joe45 age=24 job=null", read(t5, "joe45"));
    }

    @Test
    public void multipleChildCommitParentRollback() throws IOException {
        TransactionId t1 = transactor.beginTransaction(true, false, false);
        insertAge(t1, "joe46", 20);
        TransactionId t2 = transactor.beginChildTransaction(t1, true, true, null, null);
        insertJob(t2, "joe46", "baker");
        transactor.commit(t2);
        transactor.rollback(t1);
        TransactionId t3 = transactor.beginChildTransaction(t1, true, true, null, null);
        Assert.assertEquals("joe46 age=null job=null", read(t3, "joe46"));
    }

    @Test
    public void childDependentTransactionWriteRollbackParentRead() throws IOException {
        TransactionId t1 = transactor.beginTransaction(true, false, false);
        insertAge(t1, "joe27", 20);
        TransactionId t2 = transactor.beginChildTransaction(t1, true, true, null, null);
        insertAge(t2, "joe27", 21);
        transactor.commit(t2);
        transactor.rollback(t1);

        TransactionId t4 = transactor.beginTransaction(false, false, false);
        Assert.assertEquals("joe27 age=null job=null", read(t4, "joe27"));
    }

    @Test
    public void childIndependentTransactionWriteRollbackRead() throws IOException {
        TransactionId t1 = transactor.beginTransaction(true, false, false);
        insertAge(t1, "joe28", 20);
        TransactionId t2 = transactor.beginChildTransaction(t1, false, true, null, null);
        insertAge(t2, "joe28", 21);
        Assert.assertEquals("joe28 age=21 job=null", read(t1, "joe28"));
        transactor.rollback(t2);
        Assert.assertEquals("joe28 age=20 job=null", read(t1, "joe28"));
        transactor.commit(t1);

        TransactionId t3 = transactor.beginTransaction(false, false, false);
        Assert.assertEquals("joe28 age=20 job=null", read(t3, "joe28"));
    }

    @Test
    public void multipleChildIndependentTransactionWriteRead() throws IOException {
        TransactionId t1 = transactor.beginTransaction(true, false, false);
        insertAge(t1, "joe31", 20);
        TransactionId t2 = transactor.beginChildTransaction(t1, false, true, null, null);
        TransactionId t3 = transactor.beginChildTransaction(t1, false, true, null, null);
        insertAge(t2, "joe31", 21);
        insertJob(t3, "joe31", "baker");
        Assert.assertEquals("joe31 age=21 job=baker", read(t1, "joe31"));
        transactor.commit(t2);

        TransactionId t4 = transactor.beginTransaction(false, false, false);
        Assert.assertEquals("joe31 age=21 job=null", read(t4, "joe31"));

        transactor.commit(t3);

        TransactionId t5 = transactor.beginTransaction(false, false, false);
        Assert.assertEquals("joe31 age=21 job=baker", read(t5, "joe31"));

        Assert.assertEquals("joe31 age=21 job=baker", read(t1, "joe31"));
        transactor.commit(t1);

        TransactionId t6 = transactor.beginTransaction(false, false, false);
        Assert.assertEquals("joe31 age=21 job=baker", read(t6, "joe31"));
    }

    @Test
    public void childIndependentTransactionWriteRead() throws IOException {
        TransactionId t1 = transactor.beginTransaction(true, false, false);
        insertAge(t1, "joe29", 20);
        TransactionId t2 = transactor.beginChildTransaction(t1, false, true, null, null);
        insertAge(t2, "joe29", 21);
        Assert.assertEquals("joe29 age=21 job=null", read(t1, "joe29"));
        transactor.commit(t2);

        TransactionId t3 = transactor.beginTransaction(false, false, false);
        Assert.assertEquals("joe29 age=21 job=null", read(t3, "joe29"));

        Assert.assertEquals("joe29 age=21 job=null", read(t1, "joe29"));
        transactor.commit(t1);

        TransactionId t4 = transactor.beginTransaction(false, false, false);
        Assert.assertEquals("joe29 age=21 job=null", read(t4, "joe29"));
    }

    @Test
    public void childIndependentTransactionWriteRollbackParentRead() throws IOException {
        TransactionId t1 = transactor.beginTransaction(true, false, false);
        insertAge(t1, "joe30", 20);
        TransactionId t2 = transactor.beginChildTransaction(t1, false, true, null, null);
        insertAge(t2, "joe30", 21);
        transactor.commit(t2);
        transactor.rollback(t1);

        TransactionId t4 = transactor.beginTransaction(false, false, false);
        Assert.assertEquals("joe30 age=21 job=null", read(t4, "joe30"));
    }

    @Test
    public void commitParentOfCommittedDependent() throws IOException {
        TransactionId t1 = transactor.beginTransaction(true, false, false);
        insertAge(t1, "joe32", 20);
        TransactionId t2 = transactor.beginChildTransaction(t1, true, true, null, null);
        insertAge(t2, "joe32", 21);
        transactor.commit(t2);
        final Transaction transactionStatusA = transactorSetup.transactionStore.getTransaction(t2);
        Assert.assertNull("committing a dependent child does not set a commit timestamp", transactionStatusA.commitTimestamp);
        transactor.commit(t1);
        final Transaction transactionStatusB = transactorSetup.transactionStore.getTransaction(t2);
        Assert.assertNotNull("committing parent of dependent transaction should set the commit time of the child",
                transactionStatusB.commitTimestamp);
    }

    @Test
    public void commitParentOfCommittedIndependent() throws IOException {
        TransactionId t1 = transactor.beginTransaction(true, false, false);
        insertAge(t1, "joe49", 20);
        TransactionId t2 = transactor.beginChildTransaction(t1, false, true, null, null);
        insertAge(t2, "joe49", 21);
        transactor.commit(t2);
        final Transaction transactionStatusA = transactorSetup.transactionStore.getTransaction(t2);
        transactor.commit(t1);
        final Transaction transactionStatusB = transactorSetup.transactionStore.getTransaction(t2);
        Assert.assertEquals("committing parent of independent transaction should not change the commit time of the child",
                transactionStatusA.commitTimestamp, transactionStatusB.commitTimestamp);
    }

    @Test
    public void independentWriteOverlapWithReadCommittedWriter() throws IOException {
        TransactionId parent = transactor.beginTransaction(true, false, false);

        TransactionId other = transactor.beginTransaction(true, false, true);

        TransactionId child = transactor.beginChildTransaction(parent, false, true, null, null);
        insertAge(child, "joe33", 22);
        transactor.commit(child);

        try {
            // TODO: make this test pass, writes should be allowed on top of committed transactions in READ_COMMITTED mode
            insertAge(other, "joe33", 21);
            Assert.fail();
        } catch (WriteConflict e) {
        } catch (RetriesExhaustedWithDetailsException e) {
            assertWriteConflict(e);
        }
    }

    @Test
    public void dependentWriteFollowedByReadCommittedWriter() throws IOException {
        TransactionId parent = transactor.beginTransaction(true, false, false);

        TransactionId child = transactor.beginChildTransaction(parent, false, true, null, null);
        insertAge(child, "joe34", 22);
        transactor.commit(child);

        TransactionId other = transactor.beginTransaction(true, false, true);
        insertAge(other, "joe34", 21);
        transactor.commit(other);
    }

    @Test
    public void independentWriteFollowedByReadCommittedWriter() throws IOException {
        TransactionId parent = transactor.beginTransaction(true, false, false);

        TransactionId child = transactor.beginChildTransaction(parent, false, true, null, null);
        insertAge(child, "joe35", 22);
        transactor.commit(child);

        TransactionId other = transactor.beginTransaction(true, false, true);
        insertAge(other, "joe35", 21);
        transactor.commit(other);
    }

    @Test
    public void dependentWriteOverlapWithReadCommittedWriter() throws IOException {
        TransactionId parent = transactor.beginTransaction(true, false, false);

        TransactionId other = transactor.beginTransaction(true, false, true);

        TransactionId child = transactor.beginChildTransaction(parent, false, true, null, null);
        insertAge(child, "joe36", 22);
        transactor.commit(child);

        try {
            insertAge(other, "joe36", 21);
            Assert.fail();
        } catch (WriteConflict e) {
        } catch (RetriesExhaustedWithDetailsException e) {
            assertWriteConflict(e);
        }
    }

    @Test
    public void rollbackUpdate() throws IOException {
        TransactionId t1 = transactor.beginTransaction(true, false, false);
        insertAge(t1, "joe43", 20);
        transactor.commit(t1);

        TransactionId t2 = transactor.beginTransaction(true, false, false);
        insertAge(t2, "joe43", 21);
        transactor.rollback(t2);

        TransactionId t3 = transactor.beginTransaction(true, false, false);
        Assert.assertEquals("joe43 age=20 job=null", read(t3, "joe43"));
    }

    @Test
    public void rollbackInsert() throws IOException {
        TransactionId t1 = transactor.beginTransaction(true, false, false);
        insertAge(t1, "joe44", 20);
        transactor.rollback(t1);

        TransactionId t2 = transactor.beginTransaction(true, false, false);
        Assert.assertEquals("joe44 age=null job=null", read(t2, "joe44"));
    }

    @Test
    public void childrenOfChildrenCommitCommitCommit() throws IOException {
        TransactionId t1 = transactor.beginTransaction(true, false, false);
        TransactionId t2 = transactor.beginChildTransaction(t1, true, true, null, null);
        TransactionId t3 = transactor.beginChildTransaction(t2, false, true, null, null);
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
        TransactionId t4 = transactor.beginTransaction(false, false, false);
        Assert.assertEquals("joe53 age=20 job=null", read(t4, "joe53"));
        Assert.assertEquals("boe53 age=21 job=null", read(t4, "boe53"));
    }

    @Test
    public void childrenOfChildrenCommitCommitCommitParentWriteFirst() throws IOException {
        TransactionId t1 = transactor.beginTransaction(true, false, false);
        TransactionId t2 = transactor.beginChildTransaction(t1, true, true, null, null);
        TransactionId t3 = transactor.beginChildTransaction(t2, false, true, null, null);
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
        TransactionId t4 = transactor.beginTransaction(false, false, false);
        Assert.assertEquals("joe57 age=20 job=null", read(t4, "joe57"));
        Assert.assertEquals("boe57 age=21 job=null", read(t4, "boe57"));
    }

    @Test
    public void childrenOfChildrenCommitCommitRollback() throws IOException {
        TransactionId t1 = transactor.beginTransaction(true, false, false);
        TransactionId t2 = transactor.beginChildTransaction(t1, true, true, null, null);
        TransactionId t3 = transactor.beginChildTransaction(t2, false, true, null, null);
        insertAge(t3, "joe54", 20);
        Assert.assertEquals("joe54 age=20 job=null", read(t3, "joe54"));
        transactor.rollback(t3);
        Assert.assertEquals("joe54 age=null job=null", read(t2, "joe54"));
        insertAge(t2, "boe54", 21);
        Assert.assertEquals("boe54 age=21 job=null", read(t2, "boe54"));
        transactor.commit(t2);
        Assert.assertEquals("joe54 age=null job=null", read(t1, "joe54"));
        Assert.assertEquals("boe54 age=21 job=null", read(t1, "boe54"));
        transactor.commit(t1);
        TransactionId t4 = transactor.beginTransaction(false, false, false);
        Assert.assertEquals("joe54 age=null job=null", read(t4, "joe54"));
        Assert.assertEquals("boe54 age=21 job=null", read(t4, "boe54"));
    }

    @Test
    public void childrenOfChildrenCommitCommitRollbackParentWriteFirst() throws IOException {
        TransactionId t1 = transactor.beginTransaction(true, false, false);
        TransactionId t2 = transactor.beginChildTransaction(t1, true, true, null, null);
        TransactionId t3 = transactor.beginChildTransaction(t2, false, true, null, null);
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
        TransactionId t4 = transactor.beginTransaction(false, false, false);
        Assert.assertEquals("joe58 age=18 job=null", read(t4, "joe58"));
        Assert.assertEquals("boe58 age=21 job=null", read(t4, "boe58"));
    }

    @Test
    public void childrenOfChildrenCommitRollbackCommit() throws IOException {
        TransactionId t1 = transactor.beginTransaction(true, false, false);
        TransactionId t2 = transactor.beginChildTransaction(t1, true, true, null, null);
        TransactionId t3 = transactor.beginChildTransaction(t2, false, true, null, null);
        insertAge(t3, "joe55", 20);
        Assert.assertEquals("joe55 age=20 job=null", read(t3, "joe55"));
        transactor.commit(t3);
        Assert.assertEquals("joe55 age=20 job=null", read(t3, "joe55"));
        Assert.assertEquals("joe55 age=20 job=null", read(t2, "joe55"));
        insertAge(t2, "boe55", 21);
        Assert.assertEquals("boe55 age=21 job=null", read(t2, "boe55"));
        transactor.rollback(t2);
        Assert.assertEquals("joe55 age=20 job=null", read(t1, "joe55"));
        Assert.assertEquals("boe55 age=null job=null", read(t1, "boe55"));
        transactor.commit(t1);
        TransactionId t4 = transactor.beginTransaction(false, false, false);
        Assert.assertEquals("joe55 age=20 job=null", read(t4, "joe55"));
        Assert.assertEquals("boe55 age=null job=null", read(t4, "boe55"));
    }

    @Test
    public void childrenOfChildrenCommitRollbackCommitParentWriteFirst() throws IOException {
        TransactionId t1 = transactor.beginTransaction(true, false, false);
        TransactionId t2 = transactor.beginChildTransaction(t1, true, true, null, null);
        TransactionId t3 = transactor.beginChildTransaction(t2, false, true, null, null);
        insertAge(t1, "joe59", 18);
        insertAge(t1, "boe59", 19);
        insertAge(t2, "boe59", 21);
        insertAge(t3, "joe59", 20);
        Assert.assertEquals("joe59 age=20 job=null", read(t3, "joe59"));
        transactor.commit(t3);
        Assert.assertEquals("joe59 age=20 job=null", read(t2, "joe59"));
        Assert.assertEquals("boe59 age=21 job=null", read(t2, "boe59"));
        transactor.rollback(t2);
        Assert.assertEquals("joe59 age=20 job=null", read(t1, "joe59"));
        Assert.assertEquals("boe59 age=19 job=null", read(t1, "boe59"));
        transactor.commit(t1);
        TransactionId t4 = transactor.beginTransaction(false, false, false);
        Assert.assertEquals("joe59 age=20 job=null", read(t4, "joe59"));
        Assert.assertEquals("boe59 age=19 job=null", read(t4, "boe59"));
    }

    @Test
    public void childrenOfChildrenRollbackCommitCommitParentWriteFirst() throws IOException {
        TransactionId t1 = transactor.beginTransaction(true, false, false);
        TransactionId t2 = transactor.beginChildTransaction(t1, true, true, null, null);
        TransactionId t3 = transactor.beginChildTransaction(t2, false, true, null, null);
        insertAge(t1, "joe60", 18);
        insertAge(t1, "boe60", 19);
        insertAge(t2, "boe60", 21);
        insertAge(t3, "joe60", 20);
        Assert.assertEquals("joe60 age=20 job=null", read(t3, "joe60"));
        transactor.commit(t3);
        Assert.assertEquals("joe60 age=20 job=null", read(t3, "joe60"));
        Assert.assertEquals("joe60 age=20 job=null", read(t2, "joe60"));
        insertAge(t2, "boe60", 21);
        Assert.assertEquals("boe60 age=21 job=null", read(t2, "boe60"));
        transactor.commit(t2);
        Assert.assertEquals("joe60 age=20 job=null", read(t1, "joe60"));
        Assert.assertEquals("boe60 age=21 job=null", read(t1, "boe60"));
        transactor.rollback(t1);
        TransactionId t4 = transactor.beginTransaction(false, false, false);
        Assert.assertEquals("joe60 age=20 job=null", read(t4, "joe60"));
        Assert.assertEquals("boe60 age=null job=null", read(t4, "boe60"));
    }

    @Test
    public void childrenOfChildrenRollbackCommitCommit() throws IOException {
        TransactionId t1 = transactor.beginTransaction(true, false, false);
        TransactionId t2 = transactor.beginChildTransaction(t1, true, true, null, null);
        TransactionId t3 = transactor.beginChildTransaction(t2, false, true, null, null);
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
        TransactionId t4 = transactor.beginTransaction(false, false, false);
        Assert.assertEquals("joe56 age=20 job=null", read(t4, "joe56"));
        Assert.assertEquals("boe56 age=null job=null", read(t4, "boe56"));
    }

    @Test
    public void readWriteMechanics() throws Exception {
        final SDataLib dataLib = storeSetup.getDataLib();
        final STableReader reader = storeSetup.getReader();

        final Object testKey = dataLib.newRowKey(new Object[]{"jim"});
        Object put = dataLib.newPut(testKey);
        Object family = dataLib.encode(DEFAULT_FAMILY);
        Object ageQualifier = dataLib.encode("age");
        dataLib.addKeyValueToPut(put, family, ageQualifier, null, dataLib.encode(25));
        TransactionId t = transactor.beginTransaction(true, false, false);
        transactorSetup.clientTransactor.initializePut(t.getTransactionIdString(), put);
        Object put2 = dataLib.newPut(testKey);
        dataLib.addKeyValueToPut(put2, family, ageQualifier, null, dataLib.encode(27));
        transactorSetup.clientTransactor.initializePut(
                transactorSetup.clientTransactor.transactionIdFromPut(put).getTransactionIdString(),
                put2);
        Assert.assertTrue(dataLib.valuesEqual(dataLib.encode((short) 0), dataLib.getAttribute(put2, "si-needed")));
        STable testSTable = reader.open(storeSetup.getPersonTableName());
        try {
            Assert.assertTrue(transactor.processPut(testSTable, transactorSetup.rollForwardQueue, put));
            Assert.assertTrue(transactor.processPut(testSTable, transactorSetup.rollForwardQueue, put2));
            SGet get1 = dataLib.newGet(testKey, null, null, null);
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

        TransactionId t2 = transactor.beginTransaction(true, false, false);
        SGet get = dataLib.newGet(testKey, null, null, null);
        testSTable = reader.open(storeSetup.getPersonTableName());
        try {
            final Object resultTuple = reader.get(testSTable, get);
            for (Object keyValue : dataLib.listResult(resultTuple)) {
                //System.out.println(((SITransactor) transactor).shouldKeep(keyValue, t2));
            }
            final FilterState filterState = transactor.newFilterState(transactorSetup.rollForwardQueue, t2, false);
            if (useSimple) {
                transactor.filterResult(filterState, resultTuple);
            }
        } finally {
            reader.close(testSTable);
        }

        transactor.commit(t);

        t = transactor.beginTransaction(true, false, false);

        dataLib.addKeyValueToPut(put, family, ageQualifier, null, dataLib.encode(35));
        transactorSetup.clientTransactor.initializePut(t.getTransactionIdString(), put);
        testSTable = reader.open(storeSetup.getPersonTableName());
        try {
            Assert.assertTrue(transactor.processPut(testSTable, transactorSetup.rollForwardQueue, put));
        } finally {
            reader.close(testSTable);
        }

        //System.out.println("store2 = " + store);
    }

    @Test
    public void asynchRollForward() throws IOException, InterruptedException {
        checkAsynchRollForward(61, "commit", false, new Function<Object[], Object>() {
            @Override
            public Object apply(@Nullable Object[] input) {
                TransactionId t = (TransactionId) input[0];
                Object cell = input[1];
                final SDataLib dataLib = storeSetup.getDataLib();
                final long timestamp = (Long) dataLib.decode(dataLib.getKeyValueValue(cell), Long.class);
                Assert.assertEquals(t.getId() + 1, timestamp);
                return null;  //To change body of implemented methods use File | Settings | File Templates.
            }
        });
    }

    @Test
    public void asynchRollForwardRolledBackTransaction() throws IOException, InterruptedException {
        checkAsynchRollForward(71, "rollback", false, new Function<Object[], Object>() {
            @Override
            public Object apply(@Nullable Object[] input) {
                TransactionId t = (TransactionId) input[0];
                Object cell = input[1];
                final SDataLib dataLib = storeSetup.getDataLib();
                final long timestamp = (Integer) dataLib.decode(dataLib.getKeyValueValue(cell), Integer.class);
                Assert.assertEquals(-2, timestamp);
                return null;  //To change body of implemented methods use File | Settings | File Templates.
            }
        });
    }

    @Test
    public void asynchRollForwardFailedTransaction() throws IOException, InterruptedException {
        checkAsynchRollForward(71, "fail", false, new Function<Object[], Object>() {
            @Override
            public Object apply(@Nullable Object[] input) {
                TransactionId t = (TransactionId) input[0];
                Object cell = input[1];
                final SDataLib dataLib = storeSetup.getDataLib();
                final long timestamp = (Integer) dataLib.decode(dataLib.getKeyValueValue(cell), Integer.class);
                Assert.assertEquals(-2, timestamp);
                return null;  //To change body of implemented methods use File | Settings | File Templates.
            }
        });
    }

    @Test
    public void asynchRollForwardFollowedByWriteConflict() throws IOException, InterruptedException {
        checkAsynchRollForward(83, "commit", true, new Function<Object[], Object>() {
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

    private void checkAsynchRollForward(int testIndex, String commitRollBackOrFail, boolean conflictingWrite, Function<Object[], Object> timestampDecoder) throws IOException, InterruptedException {
        try {
            Tracer.rollForwardDelayOverride = 100;
            TransactionId t1 = transactor.beginTransaction(true, false, false);
            TransactionId t1b = null;
            if (conflictingWrite) {
                t1b = transactor.beginTransaction(true, false, false);
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
            Object result = readRaw(testRow);
            final SDataLib dataLib = storeSetup.getDataLib();
            final List commitTimestamps = dataLib.getResultColumn(result, dataLib.encode(SNAPSHOT_ISOLATION_FAMILY),
                    dataLib.encode(SNAPSHOT_ISOLATION_COMMIT_TIMESTAMP_COLUMN_STRING));
            for (Object c : commitTimestamps) {
                final int timestamp = (Integer) dataLib.decode(dataLib.getKeyValueValue(c), Integer.class);
                Assert.assertEquals(-1, timestamp);
                Assert.assertEquals(t1.getId(), dataLib.getKeyValueTimestamp(c));
            }
            Assert.assertTrue(latch.await(11, TimeUnit.SECONDS));

            Object result2 = readRaw(testRow);
            final List commitTimestamps2 = dataLib.getResultColumn(result2, dataLib.encode(SNAPSHOT_ISOLATION_FAMILY),
                    dataLib.encode(SNAPSHOT_ISOLATION_COMMIT_TIMESTAMP_COLUMN_STRING));
            for (Object c2 : commitTimestamps2) {
                timestampDecoder.apply(new Object[]{t1, c2});
                Assert.assertEquals(t1.getId(), dataLib.getKeyValueTimestamp(c2));
            }
            TransactionId t2 = transactor.beginTransaction(false, false, false);
            if (commitRollBackOrFail.equals("commit")) {
                Assert.assertEquals(testRow + " age=20 job=null", read(t2, testRow));
            } else {
                Assert.assertEquals(testRow + " age=null job=null", read(t2, testRow));
            }
            if (conflictingWrite) {
                try {
                    insertAge(t1b, testRow, 21);
                    Assert.fail();
                } catch (WriteConflict e) {
                } catch (RetriesExhaustedWithDetailsException e) {
                    assertWriteConflict(e);
                }
            }
        } finally {
            Tracer.rollForwardDelayOverride = null;
        }
    }

    private CountDownLatch makeLatch(final String targetKey) {
        final SDataLib dataLib = storeSetup.getDataLib();
        final CountDownLatch latch = new CountDownLatch(1);
        Tracer.register(new Function<Object, Object>() {
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
        Tracer.registerTransaction(new Function<Long, Object>() {
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
            TransactionId t1 = transactor.beginTransaction(true, false, false);
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
            TransactionId t2 = transactor.beginTransaction(false, false, false);
            if (commit) {
                Assert.assertEquals(testRow + " age=20 job=null", read(t2, testRow));
            } else {
                Assert.assertEquals(testRow + " age=null job=null", read(t2, testRow));
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
        SGet get = dataLib.newGet(key, null, null, null);
        if (allversions) {
            dataLib.setReadMaxVersions(get);
        }
        STable testSTable = reader.open(storeSetup.getPersonTableName());
        try {
            return reader.get(testSTable, get);
        } finally {
            reader.close(testSTable);
        }
    }

    @Test
    public void transactionTimeout() throws IOException, InterruptedException {
        TransactionId t1 = transactor.beginTransaction(true, false, false);
        insertAge(t1, "joe63", 20);
        sleep();
        TransactionId t2 = transactor.beginTransaction(true, false, false);
        insertAge(t2, "joe63", 21);
        transactor.commit(t2);
    }

    @Test
    public void transactionNoTimeoutWithKeepAlive() throws IOException, InterruptedException {
        TransactionId t1 = transactor.beginTransaction(true, false, false);
        insertAge(t1, "joe64", 20);
        sleep();
        transactor.keepAlive(t1);
        TransactionId t2 = transactor.beginTransaction(true, false, false);
        try {
            insertAge(t2, "joe64", 21);
            Assert.fail();
        } catch (WriteConflict e) {
        } catch (RetriesExhaustedWithDetailsException e) {
            assertWriteConflict(e);
        }
    }

    @Test
    public void transactionTimeoutAfterKeepAlive() throws IOException, InterruptedException {
        TransactionId t1 = transactor.beginTransaction(true, false, false);
        insertAge(t1, "joe65", 20);
        sleep();
        transactor.keepAlive(t1);
        sleep();
        TransactionId t2 = transactor.beginTransaction(true, false, false);
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
        final TransactionId t1 = transactor.beginTransaction(true, false, false);
        insertAge(t1, "joe66", 20);
        final TransactionId t2 = transactor.beginTransaction(true, false, false);
        try {
            insertAge(t2, "joe66", 22);
        } catch (WriteConflict e) {
        } catch (RetriesExhaustedWithDetailsException e) {
            assertWriteConflict(e);
        }
        try {
            insertAge(t2, "joe66", 23);
        } catch (WriteConflict e) {
        } catch (RetriesExhaustedWithDetailsException e) {
            assertWriteConflict(e);
        }
    }

    @Test
    public void transactionCommitFailRaceFailWins() throws Exception {
        final TransactionId t1 = transactor.beginTransaction(true, false, false);
        insertAge(t1, "joe67", 20);
        final TransactionId t2 = transactor.beginTransaction(true, false, false);

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
        }

        latch2.await(2, TimeUnit.SECONDS);
        Assert.assertEquals("committing failed", exception[0].getMessage());
        Assert.assertEquals(IOException.class, exception[0].getClass());
    }

    @Test
    public void transactionCommitFailRaceCommitWins() throws Exception {
        final TransactionId t1 = transactor.beginTransaction(true, false, false);
        insertAge(t1, "joe68", 20);
        final TransactionId t2 = transactor.beginTransaction(true, false, false);

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
            TransactionId tx = transactor.beginTransaction(true, false, false);
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
            TransactionId tx = transactor.beginTransaction(true, false, false);
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
            final SICompactionState compactionState = transactor.newCompactionState();
            store.compact(compactionState, storeSetup.getPersonTableName());
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
            final Boolean[] waits = new Boolean[] {false, false};
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
            TransactionId t1 = transactor.beginTransaction(true, false, false);
            insertAge(t1, "joe86", 20);

            new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        waits[0] = latch.await(2, TimeUnit.SECONDS);
                        Assert.assertTrue(waits[0]);
                        TransactionId t2 = transactor.beginTransaction(true, false, false);
                        try {
                            insertAge(t2, "joe86", 21);
                            Assert.fail();
                        } catch (WriteConflict e) {
                        } catch (RetriesExhaustedWithDetailsException e) {
                            assertWriteConflict(e);
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
            final TransactionId t1 = transactor.beginTransaction(true, false, false);
            Tracer.registerCommitting(new Function<Long, Object>() {
                @Override
                public Object apply(@Nullable Long timestamp) {
                    if (timestamp.equals(t1.getId())) {
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
            final TransactionId t1a = transactor.beginChildTransaction(t1, true, true, null, null);
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
                        TransactionId t2 = transactor.beginTransaction(true, false, false);
                        try {
                            insertAge(t2, "joe88", 21);
                            Assert.fail();
                        } catch (WriteConflict e) {
                        } catch (RetriesExhaustedWithDetailsException e) {
                            assertWriteConflict(e);
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
            final TransactionId t1 = transactor.beginTransaction(true, false, false);
            Tracer.registerWaiting(new Function<Long, Object>() {
                @Override
                public Object apply(@Nullable Long aLong) {
                    try {
                        Assert.assertTrue(transactorSetup.transactionStore.recordTransactionStatusChange(t1, TransactionStatus.COMMITTING, TransactionStatus.ERROR));
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
                        TransactionId t2 = transactor.beginTransaction(true, false, false);
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
        TransactionId t1 = transactor.beginTransaction(true, false, false);
        insertAge(t1, "joe89", 20);
        transactor.commit(t1);

        TransactionId t2 = transactor.beginTransaction(true, false, false);
        insertAge(t2, "joe89", 21);

        TransactionId t3 = transactor.beginTransaction(true, false, false);
        Object result = readRaw("joe89", true);
        final FilterState filterState = transactorSetup.transactor.newFilterState(t3);
        result = transactor.filterResult(filterState, result);
        Assert.assertEquals("joe89 age=20 job=null", resultToString("joe89", result));
    }

}
