package com.splicemachine.si.impl.translate;

import com.splicemachine.si.HStoreSetup;
import com.splicemachine.si.impl.translate.MemoryTableFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
@Ignore
public class MemoryTableFactoryTest {
    @Test
    public void foo() throws IOException {
        final HStoreSetup storeSetup = HStoreSetup.create();
        final Object table0 = storeSetup.getReader().open("999");
        try {
            final OperationWithAttributes put = storeSetup.getDataLib().newPut(Bytes.toBytes("joe"));
            storeSetup.getDataLib().addKeyValueToPut(put, Bytes.toBytes("V"), Bytes.toBytes("qualifier1"), 100L, Bytes.toBytes(20));
            storeSetup.getWriter().write(table0, put);
        } finally {
            storeSetup.getReader().close(table0);
        }
        Set<String> memoryTableNames = new HashSet<String>();
        memoryTableNames.add("999");
        new HTableInterfaceFactory() {
            @Override
            public HTableInterface createHTableInterface(Configuration config, byte[] tableName) {
                try {
                    return (HTableInterface) storeSetup.getReader().open(Bytes.toString(tableName));
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public void releaseHTableInterface(HTableInterface table) throws IOException {
            }
        };
        HTableInterfaceFactory factory = new MemoryTableFactory(storeSetup.getDataLib(), storeSetup.getReader(), memoryTableNames, new HTableFactory());
        final HTableInterface table = factory.createHTableInterface(null, Bytes.toBytes("999"));
        Get get = new Get(Bytes.toBytes("joe"));
        final Result result = table.get(get);
        final KeyValue kv = result.getColumnLatest(Bytes.toBytes("V"), Bytes.toBytes("qualifier1"));
        Assert.assertEquals(20, Bytes.toInt(kv.getValue()));
    }

}
