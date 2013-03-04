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
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class SiTransactorTest {
    SimpleStore store;
    final TransactionSchema transactionSchema = new TransactionSchema("transaction", "siFamily", "start", "end", "status");
    Object family;
    Object ageQualifier;

    TupleHandler tupleHandler;
    RelationReader reader;
    RelationWriter writer;
    ClientTransactor clientTransactor;
    Transactor transactor;

    private static HBaseStore setupHBaseStore() {
        try {
            final HBaseTestingUtility testCluster = new HBaseTestingUtility();
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
        SiTransactor siTransactor = new SiTransactor(new SimpleIdSource(), tupleHandler, transactionStore, "si-needed",
                "_si", "commit", 0);
        clientTransactor = siTransactor;
        transactor = siTransactor;
    }

    final boolean useSimple = true;

    @Before
    public void setUp() {
        if (useSimple) {
            setupSimpleHarness();
        } else {
            setupHBaseHarness();
        }
        setupCore();
    }

    private void insertAge(TransactionId transactionId, String name, int age) {
        Object key = tupleHandler.makeTupleKey(new Object[]{name});
        TuplePut tuple = tupleHandler.makeTuplePut(key, null);
        tupleHandler.addCellToTuple(tuple, family, ageQualifier, null, tupleHandler.makeValue(age));
        List<TuplePut> tuples = Arrays.asList(tuple);
        clientTransactor.initializeTuplePuts(tuples);

        final List<TuplePut> modifiedTuples = transactor.processTuplePuts(transactionId, tuples);
        Relation testRelation = reader.open("people");
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

    @Test
    public void test2() throws Exception {
        long start = System.currentTimeMillis();
        TransactionId t1 = transactor.beginTransaction();
        insertAge(t1, "joe", 20);
        transactor.commitTransaction(t1);

        TransactionId t2 = transactor.beginTransaction();
        System.out.println(read(t2, "joe"));
        insertAge(t2, "joe", 30);
        System.out.println(read(t2, "joe"));

        TransactionId t3 = transactor.beginTransaction();
        System.out.println(read(t3, "joe"));

        transactor.commitTransaction(t2);

        TransactionId t4 = transactor.beginTransaction();
        System.out.println(read(t4, "joe"));
        long end = System.currentTimeMillis();
        System.out.println("duration = " + (end - start));

        System.out.println(store);
    }

    @Test
    public void testName() throws Exception {
        final TupleHandler tupleHandler = new SimpleTupleHandler();
        final ManualClock clock = new ManualClock();
        final SimpleStore store = new SimpleStore(clock);
        final TransactionSchema transactionSchema = new TransactionSchema("transaction", "siFamily", "start", "end", "status");
        final TransactionStore transactionStore = new TransactionStore(transactionSchema, tupleHandler, store, store);

        SiTransactor siTransactor = new SiTransactor(new SimpleIdSource(), tupleHandler, transactionStore, "si-needed",
                "_si", "commit", 0);
        ClientTransactor clientTransactor = siTransactor;

        clock.setTime(100);
        final Object testKey = tupleHandler.makeTupleKey(new Object[]{"jim"});
        TuplePut tuple = tupleHandler.makeTuplePut(testKey, null);
        Object family = tupleHandler.makeFamily("foo");
        Object ageQualifier = tupleHandler.makeQualifier("age");
        tupleHandler.addCellToTuple(tuple, family, ageQualifier, null, tupleHandler.makeValue(25));
        List<TuplePut> tuples = Arrays.asList(tuple);
        clientTransactor.initializeTuplePuts(tuples);
        System.out.println("tuple = " + tuple);

        Transactor transactor = siTransactor;
        TransactionId t = transactor.beginTransaction();
        clock.setTime(101);
        final List<TuplePut> modifiedTuples = transactor.processTuplePuts(t, tuples);
        RelationReader reader = store;
        RelationWriter writer = store;
        Relation testRelation = reader.open("people");
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
                System.out.println(siTransactor.shouldKeep(cell, (SiTransactionId) t2));
            }
            transactor.filterTuple(t2, resultTuple);
        } finally {
            reader.close(testRelation);
        }

        transactor.commitTransaction(t);

        clock.setTime(102);
        t = transactor.beginTransaction();

        TuplePut put = tupleHandler.makeTuplePut(tupleHandler.makeTupleKey(new Object[]{"joe"}), null);


        TransactionId t3 = transactor.beginTransaction();
        TuplePut put3 = tupleHandler.makeTuplePut(testKey, null);
        tupleHandler.addCellToTuple(put, family, ageQualifier, null, tupleHandler.makeValue(35));
        tuples = Arrays.asList(tuple);
        clientTransactor.initializeTuplePuts(tuples);
        tuples = transactor.processTuplePuts(t, tuples);
        testRelation = reader.open("people");
        try {
            writer.write(testRelation, tuples);
        } finally {
            reader.close(testRelation);
        }


        //System.out.println("store2 = " + store);
    }
}
