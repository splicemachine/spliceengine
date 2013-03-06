package com.splicemachine.si2;

import com.splicemachine.si2.data.api.SDataLib;
import com.splicemachine.si2.data.api.SGet;
import com.splicemachine.si2.data.api.STable;
import com.splicemachine.si2.data.helper.RelationHelper;
import com.splicemachine.si2.data.light.LDataLib;
import com.splicemachine.si2.data.light.LStore;
import com.splicemachine.si2.data.light.ManualClock;
import junit.framework.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class LStoreTest {
	@Test
	public void test1() throws Exception {
		LStore store = new LStore(new ManualClock());

		STable table = store.open("table1");
		SDataLib SDataLib = new LDataLib();
		final Object testKey = SDataLib.newRowKey(new Object[]{"joe"});
		Object tuple = SDataLib.newPut(testKey);
		SDataLib.addKeyValueToPut(tuple, SDataLib.encode("foo"), SDataLib.encode("age"), 1L, SDataLib.encode(23));
		store.write(table, Arrays.asList(tuple));
		SGet get = SDataLib.newGet(testKey, null, null, null);
		final Object outputTuple = store.get(table, get);
		Assert.assertEquals("joe", SDataLib.getResultKey(outputTuple));
		final List outputCells = SDataLib.listResult(outputTuple);
		Assert.assertEquals(1, outputCells.size());
		final Object outputCell = outputCells.get(0);
		Assert.assertEquals("foo", SDataLib.getKeyValueFamily(outputCell));
		Assert.assertEquals("age", SDataLib.getKeyValueQualifier(outputCell));
		Assert.assertEquals(23, SDataLib.getKeyValueValue(outputCell));
		Assert.assertEquals(1L, SDataLib.getKeyValueTimestamp(outputCell));
	}

	@Test
	public void testUsingRelationAPI() throws Exception {
		LStore store = new LStore( new ManualClock() );
		SDataLib SDataLib = new LDataLib();
		RelationHelper api = new RelationHelper(SDataLib, store, store);
		api.open("table1");
		api.write(new Object[] {"joe"}, "foo", "age", 21, 0L);

		Object testKey = SDataLib.newRowKey(new Object[]{"joe"});
		SGet get = SDataLib.newGet(testKey, null, null, null);
		final Object outputTuple = store.get(store.open("table1"), get);
		Assert.assertEquals("joe", SDataLib.getResultKey(outputTuple));
		final List outputCells = SDataLib.listResult(outputTuple);
		Assert.assertEquals(1, outputCells.size());
		final Object outputCell = outputCells.get(0);
		Assert.assertEquals("foo", SDataLib.getKeyValueFamily(outputCell));
		Assert.assertEquals("age", SDataLib.getKeyValueQualifier(outputCell));
		Assert.assertEquals(21, SDataLib.getKeyValueValue(outputCell));
		Assert.assertEquals(0L, SDataLib.getKeyValueTimestamp(outputCell));
	}
}
