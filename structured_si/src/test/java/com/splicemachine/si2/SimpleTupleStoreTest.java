package com.splicemachine.si2;

import com.splicemachine.si2.relations.api.Relation;
import com.splicemachine.si2.relations.api.TupleGet;
import com.splicemachine.si2.relations.helper.RelationHelper;
import com.splicemachine.si2.relations.simple.ManualClock;
import com.splicemachine.si2.relations.simple.SimpleStore;
import com.splicemachine.si2.relations.api.TupleHandler;
import com.splicemachine.si2.relations.simple.SimpleTupleHandler;
import junit.framework.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class SimpleTupleStoreTest {
	@Test
	public void test1() throws Exception {
		SimpleStore store = new SimpleStore(new ManualClock());

		Relation relation = store.open("table1");
		TupleHandler tupleHandler = new SimpleTupleHandler();
		final Object testKey = tupleHandler.makeTupleKey(new Object[] {"joe"});
		Object tuple = tupleHandler.makeTuplePut(testKey, null);
		tupleHandler.addCellToTuple(tuple, tupleHandler.makeFamily("foo"), tupleHandler.makeQualifier("age"), 1L, tupleHandler.makeValue(23));
		store.write(relation, Arrays.asList(tuple));
		TupleGet get = tupleHandler.makeTupleGet(testKey, testKey, null, null, null);
		final Iterator results = store.read(relation, get);
		Assert.assertTrue(results.hasNext());
		final Object outputTuple = results.next();
		Assert.assertEquals("joe", tupleHandler.getKey(outputTuple));
		final List outputCells = tupleHandler.getCells(outputTuple);
		Assert.assertEquals(1, outputCells.size());
		final Object outputCell = outputCells.get(0);
		Assert.assertEquals("foo", tupleHandler.getCellFamily(outputCell));
		Assert.assertEquals("age", tupleHandler.getCellQualifier(outputCell));
		Assert.assertEquals(23, tupleHandler.getCellValue(outputCell));
		Assert.assertEquals(1L, tupleHandler.getCellTimestamp(outputCell));
	}

	@Test
	public void testUsingRelationAPI() throws Exception {
		SimpleStore store = new SimpleStore( new ManualClock() );
		TupleHandler tupleHandler = new SimpleTupleHandler();
		RelationHelper api = new RelationHelper(tupleHandler, store, store);
		api.open("table1");
		api.write(new Object[] {"joe"}, "foo", "age", 21, 0L);

		Object testKey = tupleHandler.makeTupleKey(new Object[]{"joe"});
		TupleGet get = tupleHandler.makeTupleGet(testKey, testKey, null, null, null);
		final Iterator results = store.read(store.open("table1"), get);
		Assert.assertTrue(results.hasNext());
		final Object outputTuple = results.next();
		Assert.assertEquals("joe", tupleHandler.getKey(outputTuple));
		final List outputCells = tupleHandler.getCells(outputTuple);
		Assert.assertEquals(1, outputCells.size());
		final Object outputCell = outputCells.get(0);
		Assert.assertEquals("foo", tupleHandler.getCellFamily(outputCell));
		Assert.assertEquals("age", tupleHandler.getCellQualifier(outputCell));
		Assert.assertEquals(21, tupleHandler.getCellValue(outputCell));
		Assert.assertEquals(0L, tupleHandler.getCellTimestamp(outputCell));
	}
}
