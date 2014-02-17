package com.splicemachine.si;

import com.splicemachine.si.data.api.SDataLib;
import com.splicemachine.si.data.api.STableReader;
import com.splicemachine.si.data.api.STableWriter;
import com.splicemachine.si.data.hbase.HDataLib;
import com.splicemachine.si.data.hbase.HTableReader;
import com.splicemachine.si.data.hbase.HTableWriter;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.junit.Assert;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.util.List;

public class HBaseStoreTest {

	@Test
	public void testName() throws Exception {
		final HBaseTestingUtility testCluster = new HBaseTestingUtility();
        HStoreSetup.setTestingUtilityPorts(testCluster, HStoreSetup.getNextBasePort());
        testCluster.startMiniCluster(1);

        try {
            final TestHTableSource tableSource = new TestHTableSource(testCluster, new String[]{"foo"});
            SDataLib dataLib = new HDataLib();
            STableReader reader = new HTableReader(tableSource);
            STableWriter writer = new HTableWriter();

            RelationHelper api = new RelationHelper(dataLib, reader, writer);
            api.open("table1");
            api.write(new Object[] {"joe"}, "foo", "age", 21, 0L);

            byte[] testKey = dataLib.newRowKey(new Object[]{"joe"});
            Object get = dataLib.newGet(testKey, null, null, null);
            final Result outputTuple = reader.get(reader.open("table1"), get);
            Assert.assertEquals("joe", Bytes.toString(outputTuple.getRow()));
            final List<KeyValue> outputCells = dataLib.listResult(outputTuple);
            Assert.assertEquals(1, outputCells.size());
            final KeyValue outputCell = outputCells.get(0);
            Assert.assertEquals("foo", Bytes.toString(outputCell.getFamily()));
            Assert.assertEquals("age", Bytes.toString(outputCell.getQualifier()));
            Assert.assertEquals(21, Bytes.toInt(outputCell.getValue()));
            Assert.assertEquals(0L, outputCell.getTimestamp());
        } finally {
            testCluster.shutdownMiniCluster();
        }
    }
}
