package com.splicemachine.si2;

import com.splicemachine.si2.relations.api.Relation;
import com.splicemachine.si2.relations.api.RelationReader;
import com.splicemachine.si2.relations.api.RelationWriter;
import com.splicemachine.si2.relations.api.TupleGet;
import com.splicemachine.si2.relations.api.TupleHandler;
import com.splicemachine.si2.relations.api.TuplePut;
import com.splicemachine.si2.relations.simple.ManualClock;
import com.splicemachine.si2.relations.simple.SimpleStore;
import com.splicemachine.si2.relations.simple.SimpleTupleHandler;
import com.splicemachine.si2.si.api.ClientTransactor;
import com.splicemachine.si2.si.api.TransactionId;
import com.splicemachine.si2.si.api.Transactor;
import com.splicemachine.si2.si.impl.SiTransactionId;
import com.splicemachine.si2.si.impl.SiTransactor;
import com.splicemachine.si2.si.impl.SimpleIdSource;
import com.splicemachine.si2.si.impl.TransactionSchema;
import com.splicemachine.si2.si.impl.TransactionStore;
import org.junit.Test;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class SiTransactorTest {
	@Test
	public void testName() throws Exception {
		final TupleHandler tupleHandler = new SimpleTupleHandler();
		final ManualClock clock = new ManualClock();
		final SimpleStore store = new SimpleStore(clock);
		final TransactionSchema transactionSchema = new TransactionSchema("transaction", "siFamily", "start", "end", "status");
		final TransactionStore transactionStore = new TransactionStore(transactionSchema, tupleHandler, store, store);

		SiTransactor siTransactor = new SiTransactor(new SimpleIdSource(), tupleHandler, transactionStore, "si-needed");
		ClientTransactor clientTransactor = siTransactor;

		clock.setTime(100);
		final Object testKey = tupleHandler.makeTupleKey(new Object[]{"jim"});
		TuplePut tuple = tupleHandler.makeTuplePut(testKey, null);
		Object family = tupleHandler.makeFamily("foo");
		Object ageQualifier = tupleHandler.makeQualifier("age");
		tupleHandler.addCellToTuple(tuple, family, ageQualifier, null, tupleHandler.makeValue(25));
		final List<TuplePut> tuples = Arrays.asList(tuple);
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
				System.out.print( " ");
				System.out.println(siTransactor.shouldKeep(cell, (SiTransactionId) t2));
			}
			transactor.filterTuple(t2, resultTuple);
		} finally {
			reader.close(testRelation);
		}

		transactor.commitTransaction(t);

		clock.setTime(102);
		t = transactor.beginTransaction();

		TuplePut put = tupleHandler.makeTuplePut(tupleHandler.makeTupleKey(new Object[] {"joe"}), null);


		System.out.println("store2 = " + store);
	}
}
