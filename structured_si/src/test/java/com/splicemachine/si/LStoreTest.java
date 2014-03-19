package com.splicemachine.si;

import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;

import com.splicemachine.si.data.api.SDataLib;
import com.splicemachine.si.data.light.LDataLib;
import com.splicemachine.si.data.light.LGet;
import com.splicemachine.si.data.light.LStore;
import com.splicemachine.si.data.light.LTable;
import com.splicemachine.si.data.light.LTuple;
import com.splicemachine.si.data.light.ManualClock;

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
        final List<Cell> outputCells = dataLib.listResult(outputTuple);
        Assert.assertEquals(1, outputCells.size());
        final Cell outputCell = outputCells.get(0);
        Assert.assertEquals("foo", Bytes.toString(CellUtil.cloneFamily(outputCell)));
        Assert.assertEquals("age", Bytes.toString(CellUtil.cloneQualifier(outputCell)));
        Assert.assertEquals(23, Bytes.toInt(CellUtil.cloneValue(outputCell)));
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
        final List<Cell> outputCells = dataLib.listResult(outputTuple);
        Assert.assertEquals(1, outputCells.size());
        final Cell outputCell = outputCells.get(0);
        Assert.assertEquals("foo", Bytes.toString(CellUtil.cloneFamily(outputCell)));
        Assert.assertEquals("age", Bytes.toString(CellUtil.cloneQualifier(outputCell)));
        Assert.assertEquals(21, Bytes.toInt(CellUtil.cloneValue(outputCell)));
        Assert.assertEquals(0L, outputCell.getTimestamp());
    }
}
