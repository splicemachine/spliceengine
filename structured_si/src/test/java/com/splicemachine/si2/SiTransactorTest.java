package com.splicemachine.si2;

import com.splicemachine.si2.data.api.SScan;
import com.splicemachine.si2.data.hbase.HDataLibAdapter;
import com.splicemachine.si2.data.hbase.HTableReaderAdapter;
import com.splicemachine.si2.data.hbase.HTableWriterAdapter;
import com.splicemachine.si2.data.light.Clock;
import com.splicemachine.si2.data.api.SDataLib;
import com.splicemachine.si2.data.api.SGet;
import com.splicemachine.si2.data.api.STable;
import com.splicemachine.si2.data.api.STableReader;
import com.splicemachine.si2.data.api.STableWriter;
import com.splicemachine.si2.data.hbase.HDataLib;
import com.splicemachine.si2.data.hbase.HStore;
import com.splicemachine.si2.data.light.IncrementingClock;
import com.splicemachine.si2.data.light.LDataLib;
import com.splicemachine.si2.data.light.LStore;
import com.splicemachine.si2.si.api.ClientTransactor;
import com.splicemachine.si2.si.api.TransactionId;
import com.splicemachine.si2.si.api.Transactor;
import com.splicemachine.si2.si.impl.RowMetadataStore;
import com.splicemachine.si2.si.impl.SiTransactor;
import com.splicemachine.si2.si.impl.SimpleIdSource;
import com.splicemachine.si2.si.impl.TransactionSchema;
import com.splicemachine.si2.si.impl.TransactionStore;
import junit.framework.Assert;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class SiTransactorTest {
    final boolean useSimple = false;

    LStore store;
    final TransactionSchema transactionSchema = new TransactionSchema("transaction", "siFamily", "start", "end", "status");
    Object family;
    Object ageQualifier;

    SDataLib dataLib;
    STableReader reader;
    STableWriter writer;
    ClientTransactor clientTransactor;
    Transactor transactor;

    HBaseTestingUtility testCluster;

    private HStore setupHBaseStore() {
        try {
            testCluster = new HBaseTestingUtility();
            testCluster.startMiniCluster(1);
            final TestHTableSource tableSource = new TestHTableSource(testCluster, "people", new String[]{"attributes", "_si"});
            tableSource.addTable(testCluster, "transaction", new String[]{"siFamily"});
            return new HStore(tableSource);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void setupHBaseHarness() {
        dataLib = new HDataLibAdapter(new HDataLib());
        final HStore store = setupHBaseStore();
        final STableReader rawReader = new HTableReaderAdapter(store);
        reader = new STableReader() {
            @Override
            public STable open(String relationIdentifier) {
                return rawReader.open(relationIdentifier);
            }

            @Override
            public void close(STable table) {
            }

            @Override
            public Object get(STable table, SGet get) {
                return rawReader.get(table, get);
            }

            @Override
            public Iterator scan(STable table, SScan scan) {
                return rawReader.scan(table, scan);
            }
        };
        writer = new HTableWriterAdapter(store);
    }

    private void setupSimpleHarness() {
        dataLib = new LDataLib();
        Clock clock = new IncrementingClock(1000);
        store = new LStore(clock);
        reader = store;
        writer = store;
    }

    private void setupCore() {
        family = dataLib.encode("attributes");
        ageQualifier = dataLib.encode("age");

        final TransactionStore transactionStore = new TransactionStore(transactionSchema, dataLib, reader, writer);
        SiTransactor siTransactor = new SiTransactor(new SimpleIdSource(), dataLib, writer,
                new RowMetadataStore(dataLib, reader, "si-needed", "_si", "commit", 0),
                transactionStore );
        clientTransactor = siTransactor;
        transactor = siTransactor;
    }

    @Before
    public void setUp() {
        if (useSimple) {
            setupSimpleHarness();
        } else {
            setupHBaseHarness();
        }
        setupCore();
    }

    @After
    public void tearDown() throws Exception {
        if (testCluster != null) {
            testCluster.shutdownMiniCluster();
        }
    }

    private void insertAge(TransactionId transactionId, String name, int age) {
        Object key = dataLib.newRowKey(new Object[]{name});
        Object put = dataLib.newPut(key);
        dataLib.addKeyValueToPut(put, family, ageQualifier, null, dataLib.encode(age));
        List puts = Arrays.asList(put);
        clientTransactor.initializePuts(puts);

        STable testSTable = reader.open("people");
        try {
            transactor.processPuts(transactionId, testSTable, puts);
        } finally {
            reader.close(testSTable);
        }
    }

    private String read(TransactionId transactionId, String name) {
        Object key = dataLib.newRowKey(new Object[]{name});
        SGet get = dataLib.newGet(key, Arrays.asList(family), null, null);
        STable testSTable = reader.open("people");
        try {
            Object rawTuple = reader.get(testSTable, get);
            if (rawTuple != null) {
                Object tuple = transactor.filterResult(transactionId, rawTuple);
                final Object value = dataLib.getResultValue(tuple, family, ageQualifier);
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
            System.out.println("store=" + store);
        }
    }

    @Test
    public void writeRead() {
        TransactionId t1 = transactor.beginTransaction();
        Assert.assertEquals("joe age=null", read(t1, "joe"));
        insertAge(t1, "joe", 20);
        Assert.assertEquals("joe age=20", read(t1, "joe"));
        transactor.commit(t1);

        TransactionId t2 = transactor.beginTransaction();
        Assert.assertEquals("joe age=20", read(t2, "joe"));
        dumpStore();
    }

    @Test
    public void writeReadOverlap() {
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
    public void writeWrite() {
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
    public void writeWriteOverlap() {
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
            Assert.assertEquals("transaction is not ACTIVE", e.getMessage());
        }
        Assert.assertEquals("joe age=20", read(t1, "joe"));
        transactor.commit(t1);
        try {
            transactor.commit(t2);
            assert false;
        } catch (RuntimeException e) {
            Assert.assertEquals("transaction is not ACTIVE", e.getMessage());
        }
    }

    @Test
    public void noReadAfterCommit() {
        TransactionId t1 = transactor.beginTransaction();
        insertAge(t1, "joe", 20);
        transactor.commit(t1);
        try {
            read(t1, "joe");
            assert false;
        } catch (RuntimeException e) {
            Assert.assertEquals("transaction is not ACTIVE", e.getMessage());
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
        final Object testKey = dataLib.newRowKey(new Object[]{"jim"});
        Object put = dataLib.newPut(testKey);
        Object family = dataLib.encode("attributes");
        Object ageQualifier = dataLib.encode("age");
        dataLib.addKeyValueToPut(put, family, ageQualifier, null, dataLib.encode(25));
        List tuples = Arrays.asList(put);
        clientTransactor.initializePuts(tuples);
        System.out.println("tuple = " + put);
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
            for (Object cell : dataLib.listResult(resultTuple)) {
                System.out.print(cell);
                System.out.print(" ");
                System.out.println(((SiTransactor) transactor).shouldKeep(cell, t2));
            }
            transactor.filterResult(t2, resultTuple);
        } finally {
            reader.close(testSTable);
        }

        transactor.commit(t);

        t = transactor.beginTransaction();

        dataLib.addKeyValueToPut(put, family, ageQualifier, null, dataLib.encode(35));
        tuples = Arrays.asList(put);
        clientTransactor.initializePuts(tuples);
        testSTable = reader.open("people");
        try {
            transactor.processPuts(t, testSTable, tuples);
        } finally {
            reader.close(testSTable);
        }

        //System.out.println("store2 = " + store);
    }
}
