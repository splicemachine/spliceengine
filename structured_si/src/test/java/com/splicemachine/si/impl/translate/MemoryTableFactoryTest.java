package com.splicemachine.si.impl.translate;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableFactory;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTableInterfaceFactory;
import org.apache.hadoop.hbase.client.OperationWithAttributes;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import com.splicemachine.si.HStoreSetup;
@Ignore
public class MemoryTableFactoryTest {
    @Test
    public void foo() throws IOException {
        final HStoreSetup storeSetup = new HStoreSetup(false);
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
        final Cell kv = result.getColumnLatestCell(Bytes.toBytes("V"), Bytes.toBytes("qualifier1"));
        Assert.assertEquals(20, Bytes.toInt(CellUtil.cloneValue(kv)));
    }

}
