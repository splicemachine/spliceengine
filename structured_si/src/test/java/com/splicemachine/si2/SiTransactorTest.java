package com.splicemachine.si2;

import com.splicemachine.si2.relations.api.*;
import com.splicemachine.si2.relations.hbase.HBaseStore;
import com.splicemachine.si2.relations.hbase.HBaseTupleHandler;
import com.splicemachine.si2.relations.simple.IncrementingClock;
import com.splicemachine.si2.relations.simple.ManualClock;
import com.splicemachine.si2.relations.simple.SimpleStore;
import com.splicemachine.si2.relations.simple.SimpleTupleHandler;
import com.splicemachine.si2.si.api.ClientTransactor;
import com.splicemachine.si2.si.api.TransactionId;
import com.splicemachine.si2.si.api.Transactor;
import com.splicemachine.si2.si.impl.*;
import junit.framework.Assert;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class SiTransactorTest {
    final boolean useSimple = true;

    SimpleStore store;
    final TransactionSchema transactionSchema = new TransactionSchema("transaction", "siFamily", "start", "end", "status");
    Object family;
    Object ageQualifier;

    TupleHandler tupleHandler;
    RelationReader reader;
    RelationWriter writer;
    ClientTransactor clientTransactor;
    Transactor transactor;

    HBaseTestingUtility testCluster;

    private HBaseStore setupHBaseStore() {
        try {
            testCluster = new HBaseTestingUtility();
            testCluster.startMiniCluster(1);
            final TestHBaseTableSource tableSource = new TestHBaseTableSource(testCluster, "people", new String[]{"attributes", "_si"});
            tableSource.addTable(testCluster, "transaction", new String[]{"siFamily"});
            return new HBaseStore(tableSource);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void setupHBaseHarness() {
        tupleHandler = new HBaseTupleHandler();
        final HBaseStore store = setupHBaseStore();
        reader = new RelationReader() {
            @Override
            public Relation open(String relationIdentifier) {
                return store.open(relationIdentifier);
            }

            @Override
            public void close(Relation relation) {
            }

            @Override
            public Iterator read(Relation relation, TupleGet get) {
                return store.read(relation, get);
            }
        };
        writer = store;
    }

    private void setupSimpleHarness() {
        tupleHandler = new SimpleTupleHandler();
        Clock clock = new IncrementingClock(1000);
        store = new SimpleStore(clock);
        reader = store;
        writer = store;
    }

    private void setupCore() {
        family = tupleHandler.makeFamily("attributes");
        ageQualifier = tupleHandler.makeQualifier("age");

        final TransactionStore transactionStore = new TransactionStore(transactionSchema, tupleHandler, reader, writer);
        SiTransactor siTransactor = new SiTransactor(new SimpleIdSource(), tupleHandler, reader, writer,
                transactionStore, "si-needed",
                "_si", "commit", "lock", 0);
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
        Object key = tupleHandler.makeTupleKey(new Object[]{name});
        TuplePut tuple = tupleHandler.makeTuplePut(key, null);
        tupleHandler.addCellToTuple(tuple, family, ageQualifier, null, tupleHandler.makeValue(age));
        List<TuplePut> tuples = Arrays.asList(tuple);
        clientTransactor.initializeTuplePuts(tuples);

        Relation testRelation = reader.open("people");
        final List<TuplePut> modifiedTuples = transactor.processTuplePuts(transactionId, testRelation, tuples);
        try {
            writer.write(testRelation, modifiedTuples);
        } finally {
            reader.close(testRelation);
        }
    }

    private String read(TransactionId transactionId, String name) {
        Object key = tupleHandler.makeTupleKey(new Object[]{name});
        TupleGet get = tupleHandler.makeTupleGet(key, key, Arrays.asList(family), null, null);
        Relation testRelation = reader.open("people");
        try {
            Iterator results = reader.read(testRelation, get);
            Object rawTuple = results.next();
            Object tuple = transactor.filterTuple(transactionId, rawTuple);
            final Object value = tupleHandler.getLatestCellForColumn(tuple, family, ageQualifier);
            Integer age = (Integer) tupleHandler.fromValue(value, Integer.class);
            return name + " age=" + age;
        } finally {
            reader.close(testRelation);
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
        insertAge(t1, "joe", 20);
        transactor.commitTransaction(t1);

        TransactionId t2 = transactor.beginTransaction();
        Assert.assertEquals("joe age=20", read(t2, "joe"));
        dumpStore();
    }

    @Test
    public void writeReadOverlap() {
        TransactionId t1 = transactor.beginTransaction();
        insertAge(t1, "joe", 20);

        TransactionId t2 = transactor.beginTransaction();
        Assert.assertEquals("joe age=null", read(t2, "joe"));
        transactor.commitTransaction(t1);
        dumpStore();
    }

    @Test
    public void writeWrite() {
        TransactionId t1 = transactor.beginTransaction();
        insertAge(t1, "joe", 20);
        Assert.assertEquals("joe age=20", read(t1, "joe"));
        transactor.commitTransaction(t1);

        TransactionId t2 = transactor.beginTransaction();
        Assert.assertEquals("joe age=20", read(t2, "joe"));
        insertAge(t2, "joe", 30);
        Assert.assertEquals("joe age=30", read(t2, "joe"));
        transactor.commitTransaction(t2);
    }

    @Test
    public void writeWriteOverlap() {
        TransactionId t1 = transactor.beginTransaction();
        insertAge(t1, "joe", 20);
        Assert.assertEquals("joe age=20", read(t1, "joe"));

        TransactionId t2 = transactor.beginTransaction();
        Assert.assertEquals("joe age=null", read(t2, "joe"));
        try {
            insertAge(t2, "joe", 30);
        } catch (RuntimeException e) {
            Assert.assertEquals("write/write conflict", e.getMessage());
        }
        Assert.assertEquals("joe age=30", read(t2, "joe"));
        transactor.commitTransaction(t1);
        try {
            transactor.commitTransaction(t2);
        } catch (RuntimeException e) {
            Assert.assertEquals("transaction is not ACTIVE", e.getMessage());
        }
    }

    @Test
    public void fourTransactions() throws Exception {
        TransactionId t1 = transactor.beginTransaction();
        insertAge(t1, "joe", 20);
        transactor.commitTransaction(t1);

        TransactionId t2 = transactor.beginTransaction();
        Assert.assertEquals("joe age=20", read(t2, "joe"));
        insertAge(t2, "joe", 30);
        Assert.assertEquals("joe age=30", read(t2, "joe"));

        TransactionId t3 = transactor.beginTransaction();
        Assert.assertEquals("joe age=20", read(t3, "joe"));

        transactor.commitTransaction(t2);

        TransactionId t4 = transactor.beginTransaction();
        Assert.assertEquals("joe age=30", read(t4, "joe"));
        //System.out.println(store);
    }

    @Test
    public void readWriteMechanics() throws Exception {
        final Object testKey = tupleHandler.makeTupleKey(new Object[]{"jim"});
        TuplePut tuple = tupleHandler.makeTuplePut(testKey, null);
        Object family = tupleHandler.makeFamily("attributes");
        Object ageQualifier = tupleHandler.makeQualifier("age");
        tupleHandler.addCellToTuple(tuple, family, ageQualifier, null, tupleHandler.makeValue(25));
        List<TuplePut> tuples = Arrays.asList(tuple);
        clientTransactor.initializeTuplePuts(tuples);
        System.out.println("tuple = " + tuple);
        TransactionId t = transactor.beginTransaction();
        Relation testRelation = reader.open("people");
        final List<TuplePut> modifiedTuples = transactor.processTuplePuts(t, testRelation, tuples);
        try {
            writer.write(testRelation, modifiedTuples);
        } finally {
            reader.close(testRelation);
        }

        TransactionId t2 = transactor.beginTransaction();
        TupleGet get = tupleHandler.makeTupleGet(testKey, testKey, null, null, null);
        testRelation = reader.open("people");
        try {
            final Iterator results = reader.read(testRelation, get);
            Object resultTuple = results.next();
            for (Object cell : tupleHandler.getCells(resultTuple)) {
                System.out.print(cell);
                System.out.print(" ");
                System.out.println(((SiTransactor) transactor).shouldKeep(cell, (SiTransactionId) t2));
            }
            transactor.filterTuple(t2, resultTuple);
        } finally {
            reader.close(testRelation);
        }

        transactor.commitTransaction(t);

        t = transactor.beginTransaction();

        TuplePut put = tupleHandler.makeTuplePut(tupleHandler.makeTupleKey(new Object[]{"joe"}), null);


        TransactionId t3 = transactor.beginTransaction();
        TuplePut put3 = tupleHandler.makeTuplePut(testKey, null);
        tupleHandler.addCellToTuple(put, family, ageQualifier, null, tupleHandler.makeValue(35));
        tuples = Arrays.asList(tuple);
        clientTransactor.initializeTuplePuts(tuples);
        testRelation = reader.open("people");
        tuples = transactor.processTuplePuts(t, testRelation, tuples);
        try {
            writer.write(testRelation, tuples);
        } finally {
            reader.close(testRelation);
        }


        //System.out.println("store2 = " + store);
    }
}
