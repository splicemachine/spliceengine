package com.splicemachine.si;

import com.splicemachine.si.data.api.SDataLib;
import com.splicemachine.si.data.light.*;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class LStoreTest {
    @Test
    public void test1() throws Exception {
        LStore store = new LStore(new ManualClock());

        LTable table = store.open("table1");
        SDataLib<LTuple, LTuple, LGet, LGet> dataLib = new LDataLib();
        byte[] testKey = dataLib.newRowKey(new Object[]{"joe"});
        LTuple tuple = dataLib.newPut(testKey);
        dataLib.addKeyValueToPut(tuple, dataLib.encode("foo"), dataLib.encode("age"), 1L, dataLib.encode(23));
        store.write(table, tuple);
        LGet get = dataLib.newGet(testKey, null, null, null);
        final Result outputTuple = store.get(table, get);
        Assert.assertEquals("joe", Bytes.toString(outputTuple.getRow()));
        final List<KeyValue> outputCells = dataLib.listResult(outputTuple);
        Assert.assertEquals(1, outputCells.size());
        final KeyValue outputCell = outputCells.get(0);
        Assert.assertEquals("foo", Bytes.toString(outputCell.getFamily()));
        Assert.assertEquals("age", Bytes.toString(outputCell.getQualifier()));
        Assert.assertEquals(23, Bytes.toInt(outputCell.getValue()));
        Assert.assertEquals(1L, outputCell.getTimestamp());
    }

    @Test
    public void testUsingRelationAPI() throws Exception {
        LStore store = new LStore(new ManualClock());
        SDataLib<LTuple, LTuple, LGet, LGet> dataLib = new LDataLib();
        RelationHelper api = new RelationHelper(dataLib, store, store);
        api.open("table1");
        api.write(new Object[]{"joe"}, "foo", "age", 21, 0L);

        byte[] testKey = dataLib.newRowKey(new Object[]{"joe"});
        LGet get = dataLib.newGet(testKey, null, null, null);
        final Result outputTuple = store.get(store.open("table1"), get);
        Assert.assertEquals("joe", Bytes.toString(outputTuple.getRow()));
        final List<KeyValue> outputCells = dataLib.listResult(outputTuple);
        Assert.assertEquals(1, outputCells.size());
        final KeyValue outputCell = outputCells.get(0);
        Assert.assertEquals("foo", Bytes.toString(outputCell.getFamily()));
        Assert.assertEquals("age", Bytes.toString(outputCell.getQualifier()));
        Assert.assertEquals(21, Bytes.toInt(outputCell.getValue()));
        Assert.assertEquals(0L, outputCell.getTimestamp());
    }
}
