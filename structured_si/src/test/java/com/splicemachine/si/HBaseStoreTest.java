package com.splicemachine.si;

import com.splicemachine.si.data.api.SDataLib;
import com.splicemachine.si.data.api.SGet;
import com.splicemachine.si.data.api.STableReader;
import com.splicemachine.si.data.api.STableWriter;
import com.splicemachine.si.data.hbase.HDataLib;
import com.splicemachine.si.data.hbase.HTableReader;
import com.splicemachine.si.data.hbase.HTableWriter;
import org.junit.Assert;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.util.List;

public class HBaseStoreTest {

	@Test
	public void testName() throws Exception {
		final HBaseTestingUtility testCluster = new HBaseTestingUtility();
		testCluster.startMiniCluster(1);

        try {
            final TestHTableSource tableSource = new TestHTableSource(testCluster, "table1", new String[]{"foo"});
            SDataLib SDataLib = new HDataLib();
            STableReader reader = new HTableReader(tableSource);
            STableWriter writer = new HTableWriter();

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
