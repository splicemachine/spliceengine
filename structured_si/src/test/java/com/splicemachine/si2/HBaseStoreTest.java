package com.splicemachine.si2;

import com.splicemachine.si2.relations.api.RelationReader;
import com.splicemachine.si2.relations.api.RelationWriter;
import com.splicemachine.si2.relations.api.TupleGet;
import com.splicemachine.si2.relations.api.TupleHandler;
import com.splicemachine.si2.relations.helper.RelationHelper;
import com.splicemachine.si2.relations.hbase.HBaseStore;
import com.splicemachine.si2.relations.hbase.HBaseTupleHandler;
import junit.framework.Assert;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.util.Iterator;
import java.util.List;

public class HBaseStoreTest {

	@Test
	public void testName() throws Exception {
		final HBaseTestingUtility testCluster = new HBaseTestingUtility();
		testCluster.startMiniCluster(1);

		final TestHBaseTableSource tableSource = new TestHBaseTableSource(testCluster, "table1", new String[]{"foo"});
		final HBaseStore store = new HBaseStore(tableSource);
		TupleHandler tupleHandler = new HBaseTupleHandler();
		RelationReader reader = store;
		RelationWriter writer = store;

		RelationHelper api = new RelationHelper(tupleHandler, store, store);
		api.open("table1");
		api.write(new Object[] {"joe"}, "foo", "age", 21, 0L);

		Object testKey = tupleHandler.makeTupleKey(new Object[]{"joe"});
		TupleGet get = tupleHandler.makeTupleGet(testKey, testKey, null, null, null);
		final Iterator results = store.read(store.open("table1"), get);
		Assert.assertTrue(results.hasNext());
		final Object outputTuple = results.next();
		Assert.assertEquals("joe", Bytes.toString((byte[]) tupleHandler.getKey(outputTuple)));
		final List outputCells = tupleHandler.getCells(outputTuple);
		Assert.assertEquals(1, outputCells.size());
		final Object outputCell = outputCells.get(0);
		Assert.assertEquals("foo", Bytes.toString((byte[]) tupleHandler.getCellFamily(outputCell)));
		Assert.assertEquals("age", Bytes.toString((byte[]) tupleHandler.getCellQualifier(outputCell)));
		Assert.assertEquals(21, Bytes.toInt((byte[]) tupleHandler.getCellValue(outputCell)));
		Assert.assertEquals(0L, tupleHandler.getCellTimestamp(outputCell));
	}
}
