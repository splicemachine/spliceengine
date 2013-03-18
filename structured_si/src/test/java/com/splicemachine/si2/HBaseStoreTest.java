package com.splicemachine.si2;

import com.splicemachine.si2.data.api.SDataLib;
import com.splicemachine.si2.data.api.SGet;
import com.splicemachine.si2.data.api.STableReader;
import com.splicemachine.si2.data.api.STableWriter;
import com.splicemachine.si2.data.hbase.HDataLib;
import com.splicemachine.si2.data.hbase.HDataLibAdapter;
import com.splicemachine.si2.data.hbase.HStore;
import com.splicemachine.si2.data.hbase.HTableReaderAdapter;
import com.splicemachine.si2.data.hbase.HTableWriterAdapter;
import com.splicemachine.si2.data.helper.RelationHelper;
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

        try {
            final TestHTableSource tableSource = new TestHTableSource(testCluster, "table1", new String[]{"foo"});
            final HStore store = new HStore(tableSource);
            SDataLib SDataLib = new HDataLibAdapter(new HDataLib());
            STableReader reader = new HTableReaderAdapter(store);
            STableWriter writer = new HTableWriterAdapter(store);

            RelationHelper api = new RelationHelper(SDataLib, reader, writer);
            api.open("table1");
            api.write(new Object[] {"joe"}, "foo", "age", 21, 0L);

            Object testKey = SDataLib.newRowKey(new Object[]{"joe"});
            SGet get = SDataLib.newGet(testKey, null, null, null);
            final Object outputTuple = reader.get(reader.open("table1"), get);
            Assert.assertEquals("joe", Bytes.toString((byte[]) SDataLib.getResultKey(outputTuple)));
            final List outputCells = SDataLib.listResult(outputTuple);
            Assert.assertEquals(1, outputCells.size());
            final Object outputCell = outputCells.get(0);
            Assert.assertEquals("foo", Bytes.toString((byte[]) SDataLib.getKeyValueFamily(outputCell)));
            Assert.assertEquals("age", Bytes.toString((byte[]) SDataLib.getKeyValueQualifier(outputCell)));
            Assert.assertEquals(21, Bytes.toInt((byte[]) SDataLib.getKeyValueValue(outputCell)));
            Assert.assertEquals(0L, SDataLib.getKeyValueTimestamp(outputCell));
        } finally {
            testCluster.shutdownMiniCluster();
        }
    }
}
