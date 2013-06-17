package com.splicemachine.si;

import com.splicemachine.si.data.api.SDataLib;
import com.splicemachine.si.data.light.LDataLib;
import com.splicemachine.si.data.light.LGet;
import com.splicemachine.si.data.light.LKeyValue;
import com.splicemachine.si.data.light.LRowLock;
import com.splicemachine.si.data.light.LStore;
import com.splicemachine.si.data.light.LTable;
import com.splicemachine.si.data.light.LTuple;
import com.splicemachine.si.data.light.ManualClock;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class LStoreTest {
    @Test
    public void test1() throws Exception {
        LStore store = new LStore(new ManualClock());

        LTable table = store.open("table1");
        SDataLib<Object, LTuple, LKeyValue, Object, LTuple, LTuple, LGet, LGet, LRowLock> dataLib = new LDataLib();
        final Object testKey = dataLib.newRowKey(new Object[]{"joe"});
        LTuple tuple = dataLib.newPut(testKey);
        dataLib.addKeyValueToPut(tuple, dataLib.encode("foo"), dataLib.encode("age"), 1L, dataLib.encode(23));
        store.write(table, tuple);
        LGet get = dataLib.newGet(testKey, null, null, null);
        final LTuple outputTuple = store.get(table, get);
        Assert.assertEquals("joe", dataLib.getResultKey(outputTuple));
        final List<LKeyValue> outputCells = dataLib.listResult(outputTuple);
        Assert.assertEquals(1, outputCells.size());
        final LKeyValue outputCell = outputCells.get(0);
        Assert.assertEquals("foo", dataLib.getKeyValueFamily(outputCell));
        Assert.assertEquals("age", dataLib.getKeyValueQualifier(outputCell));
        Assert.assertEquals(23, dataLib.getKeyValueValue(outputCell));
        Assert.assertEquals(1L, dataLib.getKeyValueTimestamp(outputCell));
    }

    @Test
    public void testUsingRelationAPI() throws Exception {
        LStore store = new LStore(new ManualClock());
        SDataLib<Object, LTuple, LKeyValue, Object, LTuple, LTuple, LGet, LGet, LRowLock> dataLib = new LDataLib();
        RelationHelper api = new RelationHelper(dataLib, store, store);
        api.open("table1");
        api.write(new Object[]{"joe"}, "foo", "age", 21, 0L);

        Object testKey = dataLib.newRowKey(new Object[]{"joe"});
        LGet get = dataLib.newGet(testKey, null, null, null);
        final LTuple outputTuple = store.get(store.open("table1"), get);
        Assert.assertEquals("joe", dataLib.getResultKey(outputTuple));
        final List<LKeyValue> outputCells = dataLib.listResult(outputTuple);
        Assert.assertEquals(1, outputCells.size());
        final LKeyValue outputCell = outputCells.get(0);
        Assert.assertEquals("foo", dataLib.getKeyValueFamily(outputCell));
        Assert.assertEquals("age", dataLib.getKeyValueQualifier(outputCell));
        Assert.assertEquals(21, dataLib.getKeyValueValue(outputCell));
        Assert.assertEquals(0L, dataLib.getKeyValueTimestamp(outputCell));
    }
}
