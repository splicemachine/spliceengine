package com.splicemachine.si.impl.translate;

import java.io.IOException;
import java.util.List;

import com.google.common.collect.Lists;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import com.splicemachine.si.api.Clock;
import com.splicemachine.si.data.light.IncrementingClock;
import com.splicemachine.si.data.light.LDataLib;
import com.splicemachine.si.data.light.LGet;
import com.splicemachine.si.data.light.LStore;
import com.splicemachine.si.data.light.LTable;
import com.splicemachine.si.data.light.LTuple;

public class TranslatorTest {
    @Test
    @Ignore
    public void translate() throws IOException {
        final LDataLib dataLib1 = new LDataLib();
        final Clock clock1 = new IncrementingClock(1000);
        final LStore store1 = new LStore(clock1);

        final LDataLib dataLib2 = new LDataLib();
        final Clock clock2 = new IncrementingClock(1000);
        final LStore store2 = new LStore(clock2);

        final LTuple put = dataLib1.newPut(dataLib1.encode("joe"));
        dataLib1.addKeyValueToPut(put, dataLib1.encode("family1"), dataLib1.encode("age"), 100L, dataLib1.encode(20));
        final LTable table = store1.open("people");
        store1.write(table, put);

        final Transcoder transcoder = new Transcoder() {
            @Override
            public Object transcode(Object data) {
                return data;
            }

            @Override
            public Object transcodeKey(Object key) {
                throw new RuntimeException();
            }

            @Override
            public Object transcodeFamily(Object family) {
                return null;
            }

            @Override
            public Object transcodeQualifier(Object qualifier) {
                return null;
            }
        };
        final Translator translator = new Translator(dataLib1, store1, dataLib2, store2, store2, transcoder, transcoder);

        translator.translate("people");

        final LGet get = dataLib2.newGet(dataLib2.encode("joe"), null, null, null);
        final LTable table2 = store2.open("people");
        final Result result = store2.get(table2, get);
        Assert.assertNotNull(result);
        final List<Cell> results = Lists.newArrayList(result.rawCells());
        Assert.assertEquals(1, results.size());
        final Cell kv = results.get(0);
        Assert.assertEquals("joe", Bytes.toString(CellUtil.cloneRow(kv)));
        Assert.assertEquals("family1", Bytes.toString(CellUtil.cloneFamily(kv)));
        Assert.assertEquals("age", Bytes.toString(CellUtil.cloneQualifier(kv)));
        Assert.assertEquals(100L, kv.getTimestamp());
        Assert.assertEquals(20, Bytes.toInt(CellUtil.cloneValue(kv)));
    }
}
