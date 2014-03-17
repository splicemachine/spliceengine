package com.splicemachine.si;

import java.util.Arrays;
import java.util.List;

import com.google.common.collect.Lists;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;

import com.splicemachine.si.data.api.SDataLib;
import com.splicemachine.si.data.light.LDataLib;

public class LDataLibTest {

		@Test
    public void testMultiValuesManyColumns() throws Exception {
				byte[] rowKey = Bytes.toBytes("fred");
				byte[] columnFamily = Bytes.toBytes("foo");
				byte[] ageQualifier = Bytes.toBytes("age");
				byte[] jobQualifier = Bytes.toBytes("jobQualifier");
				byte[] aliasQualifier = Bytes.toBytes("aliasQualifier");
				List<Cell> values = Arrays.asList(
                        (Cell)new KeyValue(rowKey, columnFamily, ageQualifier, 5L, Bytes.toBytes(21)),
                        (Cell)new KeyValue(rowKey, columnFamily, ageQualifier, 3L, Bytes.toBytes(11)),
                        (Cell)new KeyValue(rowKey, columnFamily, jobQualifier, 11L, Bytes.toBytes("baker")),
                        (Cell)new KeyValue(rowKey, columnFamily, aliasQualifier, 8L, Bytes.toBytes("joey")));
				Result tuple = Result.create(values);
        SDataLib reader = new LDataLib();

        @SuppressWarnings("unchecked") List<Cell> keyValues = reader.listResult(tuple);
        List<Cell> results = Lists.newArrayList();
				for (Cell kv : keyValues) {
            results.add(kv);
        }
        Assert.assertArrayEquals(new Object[]{
                new KeyValue(rowKey, columnFamily, ageQualifier, 5L, Bytes.toBytes(21)),
                new KeyValue(rowKey, columnFamily, ageQualifier, 3L, Bytes.toBytes(11)),
                new KeyValue(rowKey, columnFamily, aliasQualifier, 8L, Bytes.toBytes("joey")),
                new KeyValue(rowKey, columnFamily, jobQualifier, 11L, Bytes.toBytes("baker"))},
                results.toArray());
    }
}
