package com.splicemachine.si.impl.translate;

import com.splicemachine.si.api.Clock;
import com.splicemachine.si.impl.translate.Transcoder;
import com.splicemachine.si.impl.translate.Translator;
import com.splicemachine.si.data.light.IncrementingClock;
import com.splicemachine.si.data.light.LDataLib;
import com.splicemachine.si.data.light.LGet;
import com.splicemachine.si.data.light.LKeyValue;
import com.splicemachine.si.data.light.LStore;
import com.splicemachine.si.data.light.LTable;
import com.splicemachine.si.data.light.LTuple;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

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
        final LTuple result = store2.get(table2, get);
        Assert.assertNotNull(result);
        final List<LKeyValue> results = dataLib2.listResult(result);
        Assert.assertEquals(1, results.size());
        final LKeyValue kv = results.get(0);
        Assert.assertEquals("joe", dataLib2.getKeyValueRow(kv));
        Assert.assertEquals("family1", dataLib2.getKeyValueFamily(kv));
        Assert.assertEquals("age", dataLib2.getKeyValueQualifier(kv));
        Assert.assertEquals(100L, dataLib2.getKeyValueTimestamp(kv));
        Assert.assertEquals(20, dataLib2.getKeyValueValue(kv));
    }
}
