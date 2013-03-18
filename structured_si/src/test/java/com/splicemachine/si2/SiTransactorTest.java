package com.splicemachine.si2;

import com.splicemachine.si2.data.api.SDataLib;
import com.splicemachine.si2.data.api.SGet;
import com.splicemachine.si2.data.api.SScan;
import com.splicemachine.si2.data.api.STable;
import com.splicemachine.si2.data.api.STableReader;
import com.splicemachine.si2.si.api.FilterState;
import com.splicemachine.si2.si.api.TransactionId;
import com.splicemachine.si2.si.api.Transactor;
import com.splicemachine.si2.si.impl.SiTransactor;
import junit.framework.Assert;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Iterator;

public class SiTransactorTest {
    boolean useSimple = true;

    StoreSetup storeSetup;
    TransactorSetup transactorSetup;
    Transactor transactor;

    void baseSetUp() {
        transactor = transactorSetup.transactor;
    }

    @Before
    public void setUp() {
        storeSetup = new LStoreSetup();
        transactorSetup = new TransactorSetup(storeSetup);
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

    private String read(TransactionId transactionId, String name) throws IOException {
        return readAgeDirect(useSimple, transactorSetup, storeSetup, transactionId, name);
    }

    private String scan(TransactionId transactionId, String name) throws IOException {
        return scanAgeDirect(useSimple, transactorSetup, storeSetup, transactionId, name);
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
        transactorSetup.clientTransactor.initializePut(transactionId, put);

        STable testSTable = reader.open(storeSetup.getPersonTableName());
        try {
            if (useSimple) {
                try {
                    assert transactorSetup.transactor.processPut(testSTable, put);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
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
        transactorSetup.clientTransactor.initializeGet(transactionId, get);
        STable testSTable = reader.open(storeSetup.getPersonTableName());
        try {
            Object rawTuple = reader.get(testSTable, get);
            return readRawTuple(useSimple, transactorSetup, transactionId, name, dataLib, testSTable, rawTuple);
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
        transactorSetup.clientTransactor.initializeScan(transactionId, get);
        STable testSTable = reader.open(storeSetup.getPersonTableName());
        try {
            Iterator results = reader.scan(testSTable, get);
            assert results.hasNext();
            Object rawTuple = results.next();
            assert !results.hasNext();
            return readRawTuple(useSimple, transactorSetup, transactionId, name, dataLib, testSTable, rawTuple);
        } finally {
            reader.close(testSTable);
        }
    }

    private static String readRawTuple(boolean useSimple, TransactorSetup transactorSetup, TransactionId transactionId, String name, SDataLib dataLib, STable testSTable, Object rawTuple) throws IOException {
        if (rawTuple != null) {
            Object result = rawTuple;
            if (useSimple) {
                final FilterState filterState;
                try {
                    filterState = transactorSetup.transactor.newFilterState(testSTable, transactionId);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                result = transactorSetup.transactor.filterResult(filterState, rawTuple);
            }
            final Object ageValue = dataLib.getResultValue(result, transactorSetup.family, transactorSetup.ageQualifier);
            Integer age = (Integer) dataLib.decode(ageValue, Integer.class);
            final Object jobValue = dataLib.getResultValue(result, transactorSetup.family, transactorSetup.jobQualifier);
            String job = (String) dataLib.decode(jobValue, String.class);
            return name + " age=" + age + " job=" + job;
        } else {
            return name + " age=" + null + " job=" + null;
        }
    }

    private void dumpStore() {
        if (useSimple) {
            System.out.println("store=" + storeSetup.getStore());
        }
    }

    @Test
    public void writeRead() throws IOException {
        TransactionId t1 = transactor.beginTransaction();
        Assert.assertEquals("joe9 age=null job=null", read(t1, "joe9"));
        insertAge(t1, "joe9", 20);
        dumpStore();
        Assert.assertEquals("joe9 age=20 job=null", read(t1, "joe9"));
        transactor.commit(t1);

        TransactionId t2 = transactor.beginTransaction();
        Assert.assertEquals("joe9 age=20 job=null", read(t2, "joe9"));
        dumpStore();
    }

    @Test
    public void writeReadOverlap() throws IOException {
        TransactionId t1 = transactor.beginTransaction();
        Assert.assertEquals("joe8 age=null job=null", read(t1, "joe8"));
        insertAge(t1, "joe8", 20);
        Assert.assertEquals("joe8 age=20 job=null", read(t1, "joe8"));

        TransactionId t2 = transactor.beginTransaction();
        Assert.assertEquals("joe8 age=20 job=null", read(t1, "joe8"));
        Assert.assertEquals("joe8 age=null job=null", read(t2, "joe8"));
        transactor.commit(t1);
        Assert.assertEquals("joe8 age=null job=null", read(t2, "joe8"));
        dumpStore();
    }

    @Test
    public void writeWrite() throws IOException {
        TransactionId t1 = transactor.beginTransaction();
        Assert.assertEquals("joe age=null job=null", read(t1, "joe"));
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
        Assert.assertEquals("joe2 age=null job=null", read(t1, "joe2"));
        insertAge(t1, "joe2", 20);
        Assert.assertEquals("joe2 age=20 job=null", read(t1, "joe2"));

        TransactionId t2 = transactor.beginTransaction();
        Assert.assertEquals("joe2 age=20 job=null", read(t1, "joe2"));
        Assert.assertEquals("joe2 age=null job=null", read(t2, "joe2"));
        try {
            insertAge(t2, "joe2", 30);
            assert false;
        } catch (RuntimeException e) {
            // TODO: expected write/write conflict
            //DoNotRetryIOException dnrio = (DoNotRetryIOException) e.getCause();
            //Assert.assertTrue(dnrio.getMessage().indexOf("write/write conflict") >= 0);
        }
        Assert.assertEquals("joe2 age=20 job=null", read(t1, "joe2"));
        try {
            Assert.assertEquals("joe2 age=null job=null", read(t2, "joe2"));
            assert false;
        } catch (RuntimeException e) {
            DoNotRetryIOException dnrio = (DoNotRetryIOException) e.getCause();
            Assert.assertTrue(dnrio.getMessage().indexOf("transaction is not ACTIVE") >= 0);
        }
        Assert.assertEquals("joe2 age=20 job=null", read(t1, "joe2"));
        transactor.commit(t1);
        try {
            transactor.commit(t2);
            assert false;
        } catch (DoNotRetryIOException dnrio) {
            Assert.assertEquals("transaction is not ACTIVE", dnrio.getMessage());
        }
    }

    @Test
    public void noReadAfterCommit() throws IOException {
        TransactionId t1 = transactor.beginTransaction();
        insertAge(t1, "joe3", 20);
        transactor.commit(t1);
        try {
            read(t1, "joe3");
            assert false;
        } catch (RuntimeException e) {
            DoNotRetryIOException dnrio = (DoNotRetryIOException) e.getCause();
            Assert.assertTrue(dnrio.getMessage().indexOf("transaction is not ACTIVE") >= 0);
        }
    }

    @Test
    public void writeScan() throws IOException {
        TransactionId t1 = transactor.beginTransaction();
        Assert.assertEquals("joe4 age=null job=null", read(t1, "joe4"));
        insertAge(t1, "joe4", 20);
        dumpStore();
        Assert.assertEquals("joe4 age=20 job=null", read(t1, "joe4"));
        transactor.commit(t1);

        TransactionId t2 = transactor.beginTransaction();
        Assert.assertEquals("joe4 age=20 job=null", scan(t2, "joe4"));

        Assert.assertEquals("joe4 age=20 job=null", read(t2, "joe4"));
        dumpStore();
    }

    @Test
    public void writeWriteRead() throws IOException {
        TransactionId t1 = transactor.beginTransaction();
        Assert.assertEquals("joe5 age=null job=null", read(t1, "joe5"));
        insertAge(t1, "joe5", 20);
        dumpStore();
        Assert.assertEquals("joe5 age=20 job=null", read(t1, "joe5"));
        transactor.commit(t1);

        TransactionId t2 = transactor.beginTransaction();
        Assert.assertEquals("joe5 age=20 job=null", read(t2, "joe5"));
        insertJob(t2, "joe5", "baker");
        dumpStore();
        Assert.assertEquals("joe5 age=20 job=baker", read(t2, "joe5"));
        transactor.commit(t2);

        TransactionId t3 = transactor.beginTransaction();
        Assert.assertEquals("joe5 age=20 job=baker", read(t3, "joe5"));
        dumpStore();
    }

    @Test
    public void manyWritesManyRollbacksRead() throws IOException {
        TransactionId t1 = transactor.beginTransaction();
        insertAge(t1, "joe6", 20);
        dumpStore();
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
        transactor.abort(t7);

        TransactionId t8 = transactor.beginTransaction();
        insertAge(t8, "joe6", 28);
        transactor.abort(t8);

        TransactionId t9 = transactor.beginTransaction();
        insertAge(t9, "joe6", 29);
        transactor.abort(t9);

        TransactionId t10 = transactor.beginTransaction();
        insertAge(t10, "joe6", 30);
        transactor.abort(t10);

        TransactionId t11 = transactor.beginTransaction();
        Assert.assertEquals("joe6 age=20 job=farrier", read(t11, "joe6"));
        dumpStore();
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
        //System.out.println(store);
    }

    @Test
    public void readWriteMechanics() throws Exception {
        final SDataLib dataLib = storeSetup.getDataLib();
        final STableReader reader = storeSetup.getReader();

        final Object testKey = dataLib.newRowKey(new Object[]{"jim"});
        Object put = dataLib.newPut(testKey);
        Object family = dataLib.encode("attributes");
        Object ageQualifier = dataLib.encode("age");
        dataLib.addKeyValueToPut(put, family, ageQualifier, null, dataLib.encode(25));
        TransactionId t = transactor.beginTransaction();
        transactorSetup.clientTransactor.initializePut(t, put);
        Object put2 = dataLib.newPut(testKey);
        dataLib.addKeyValueToPut(put2, family, ageQualifier, null, dataLib.encode(27));
        transactorSetup.clientTransactor.initializePut(put, put2);
        Assert.assertTrue(dataLib.valuesEqual(dataLib.encode(true), dataLib.getAttribute(put2, "si-needed")));
        System.out.println("put = " + put);
        STable testSTable = reader.open(storeSetup.getPersonTableName());
        try {
            assert transactor.processPut(testSTable, put);
            assert transactor.processPut(testSTable, put2);
            SGet get1 = dataLib.newGet(testKey, null, null, null);
            transactorSetup.clientTransactor.initializeGet(t, get1);
            Object result = reader.get(testSTable, get1);
            result = transactor.filterResult(transactor.newFilterState(testSTable, t), result);
            final int ageRead = (Integer) dataLib.decode(dataLib.getResultValue(result, family, ageQualifier), Integer.class);
            Assert.assertEquals(27, ageRead);
        } finally {
            reader.close(testSTable);
        }

        TransactionId t2 = transactor.beginTransaction();
        SGet get = dataLib.newGet(testKey, null, null, null);
        testSTable = reader.open(storeSetup.getPersonTableName());
        try {
            final Object resultTuple = reader.get(testSTable, get);
            for (Object keyValue : dataLib.listResult(resultTuple)) {
                System.out.print(keyValue);
                System.out.print(" ");
                System.out.println(((SiTransactor) transactor).shouldKeep(keyValue, t2));
            }
            final FilterState filterState = transactor.newFilterState(testSTable, t2);
            transactor.filterResult(filterState, resultTuple);
        } finally {
            reader.close(testSTable);
        }

        transactor.commit(t);

        t = transactor.beginTransaction();

        dataLib.addKeyValueToPut(put, family, ageQualifier, null, dataLib.encode(35));
        transactorSetup.clientTransactor.initializePut(t, put);
        testSTable = reader.open(storeSetup.getPersonTableName());
        try {
            assert transactor.processPut(testSTable, put);
        } finally {
            reader.close(testSTable);
        }

        //System.out.println("store2 = " + store);
    }
}
