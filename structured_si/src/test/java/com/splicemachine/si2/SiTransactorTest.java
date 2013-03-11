package com.splicemachine.si2;

import com.splicemachine.si2.data.api.SDataLib;
import com.splicemachine.si2.data.api.SGet;
import com.splicemachine.si2.data.api.STable;
import com.splicemachine.si2.data.api.STableReader;
import com.splicemachine.si2.data.hbase.HGet;
import com.splicemachine.si2.data.hbase.TransactorFactory;
import com.splicemachine.si2.filters.SIFilter;
import com.splicemachine.si2.si.api.FilterState;
import com.splicemachine.si2.si.api.TransactionId;
import com.splicemachine.si2.si.api.Transactor;
import com.splicemachine.si2.si.impl.SiTransactor;
import junit.framework.Assert;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.client.Get;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class SiTransactorTest {
    final boolean useSimple = true;

    StoreSetup storeSetup;
    TransactorSetup transactorSetup;
    Transactor transactor;

    @Before
    public void setUp() {
        storeSetup = new LStoreSetup();
        if (!useSimple) {
            storeSetup = new HStoreSetup();
        }
        transactorSetup = new TransactorSetup(storeSetup);
        transactor = transactorSetup.transactor;
        if (!useSimple) {
            TransactorFactory.setTransactor(transactor);
        }
    }

    @After
    public void tearDown() throws Exception {
        if (storeSetup.getTestCluster() != null) {
            storeSetup.getTestCluster().shutdownMiniCluster();
        }
    }

    private void insertAge(TransactionId transactionId, String name, int age) {
        insertAgeDirect(transactorSetup, storeSetup, transactionId, name, age);
    }

    private String read(TransactionId transactionId, String name) throws IOException {
        return readAgeDirect(useSimple, transactorSetup, storeSetup, transactionId, name);
    }

    static void insertAgeDirect(TransactorSetup transactorSetup, StoreSetup storeSetup, TransactionId transactionId, String name, int age) {
        final SDataLib dataLib = storeSetup.getDataLib();
        final STableReader reader = storeSetup.getReader();

        Object key = dataLib.newRowKey(new Object[]{name});
        Object put = dataLib.newPut(key);
        dataLib.addKeyValueToPut(put, transactorSetup.family, transactorSetup.ageQualifier, null, dataLib.encode(age));
        List puts = Arrays.asList(put);
        transactorSetup.clientTransactor.initializePuts(puts);

        STable testSTable = reader.open("people");
        try {
            transactorSetup.transactor.processPuts(transactionId, testSTable, puts);
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
        STable testSTable = reader.open("people");
        try {
            Object rawTuple = reader.get(testSTable, get);
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
                final Object value = dataLib.getResultValue(result, transactorSetup.family, transactorSetup.ageQualifier);
                Integer age = (Integer) dataLib.decode(value, Integer.class);
                return name + " age=" + age;
            } else {
                return name + " age=" + null;
            }
        } finally {
            reader.close(testSTable);
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
        Assert.assertEquals("joe age=null", read(t1, "joe"));
        insertAge(t1, "joe", 20);
        dumpStore();
        Assert.assertEquals("joe age=20", read(t1, "joe"));
        transactor.commit(t1);

        TransactionId t2 = transactor.beginTransaction();
        Assert.assertEquals("joe age=20", read(t2, "joe"));
        dumpStore();
    }

    @Test
    public void writeReadOverlap() throws IOException {
        TransactionId t1 = transactor.beginTransaction();
        Assert.assertEquals("joe age=null", read(t1, "joe"));
        insertAge(t1, "joe", 20);
        Assert.assertEquals("joe age=20", read(t1, "joe"));

        TransactionId t2 = transactor.beginTransaction();
        Assert.assertEquals("joe age=20", read(t1, "joe"));
        Assert.assertEquals("joe age=null", read(t2, "joe"));
        transactor.commit(t1);
        Assert.assertEquals("joe age=null", read(t2, "joe"));
        dumpStore();
    }

    @Test
    public void writeWrite() throws IOException {
        TransactionId t1 = transactor.beginTransaction();
        Assert.assertEquals("joe age=null", read(t1, "joe"));
        insertAge(t1, "joe", 20);
        Assert.assertEquals("joe age=20", read(t1, "joe"));
        transactor.commit(t1);

        TransactionId t2 = transactor.beginTransaction();
        Assert.assertEquals("joe age=20", read(t2, "joe"));
        insertAge(t2, "joe", 30);
        Assert.assertEquals("joe age=30", read(t2, "joe"));
        transactor.commit(t2);
    }

    @Test
    public void writeWriteOverlap() throws IOException {
        TransactionId t1 = transactor.beginTransaction();
        Assert.assertEquals("joe age=null", read(t1, "joe"));
        insertAge(t1, "joe", 20);
        Assert.assertEquals("joe age=20", read(t1, "joe"));

        TransactionId t2 = transactor.beginTransaction();
        Assert.assertEquals("joe age=20", read(t1, "joe"));
        Assert.assertEquals("joe age=null", read(t2, "joe"));
        try {
            insertAge(t2, "joe", 30);
            assert false;
        } catch (RuntimeException e) {
            Assert.assertEquals("write/write conflict", e.getMessage());
        }
        Assert.assertEquals("joe age=20", read(t1, "joe"));
        try {
            Assert.assertEquals("joe age=null", read(t2, "joe"));
            assert false;
        } catch (RuntimeException e) {
            DoNotRetryIOException dnrio = (DoNotRetryIOException) e.getCause();
            Assert.assertTrue(dnrio.getMessage().indexOf("transaction is not ACTIVE") >= 0);
        }
        Assert.assertEquals("joe age=20", read(t1, "joe"));
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
        insertAge(t1, "joe", 20);
        transactor.commit(t1);
        try {
            read(t1, "joe");
            assert false;
        } catch (RuntimeException e) {
            DoNotRetryIOException dnrio = (DoNotRetryIOException) e.getCause();
            Assert.assertTrue(dnrio.getMessage().indexOf("transaction is not ACTIVE") >= 0);
        }
    }

    @Test
    public void fourTransactions() throws Exception {
        TransactionId t1 = transactor.beginTransaction();
        insertAge(t1, "joe", 20);
        transactor.commit(t1);

        TransactionId t2 = transactor.beginTransaction();
        Assert.assertEquals("joe age=20", read(t2, "joe"));
        insertAge(t2, "joe", 30);
        Assert.assertEquals("joe age=30", read(t2, "joe"));

        TransactionId t3 = transactor.beginTransaction();
        Assert.assertEquals("joe age=20", read(t3, "joe"));

        transactor.commit(t2);

        TransactionId t4 = transactor.beginTransaction();
        Assert.assertEquals("joe age=30", read(t4, "joe"));
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
        List tuples = Arrays.asList(put);
        transactorSetup.clientTransactor.initializePuts(tuples);
        System.out.println("put = " + put);
        TransactionId t = transactor.beginTransaction();
        STable testSTable = reader.open("people");
        try {
            transactor.processPuts(t, testSTable, tuples);
        } finally {
            reader.close(testSTable);
        }

        TransactionId t2 = transactor.beginTransaction();
        SGet get = dataLib.newGet(testKey, null, null, null);
        testSTable = reader.open("people");
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
        tuples = Arrays.asList(put);
        transactorSetup.clientTransactor.initializePuts(tuples);
        testSTable = reader.open("people");
        try {
            transactor.processPuts(t, testSTable, tuples);
        } finally {
            reader.close(testSTable);
        }

        //System.out.println("store2 = " + store);
    }
}
