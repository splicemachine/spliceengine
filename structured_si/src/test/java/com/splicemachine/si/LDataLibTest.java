package com.splicemachine.si;

import com.google.common.collect.Lists;
import com.google.common.primitives.Longs;
import com.splicemachine.si.data.api.SDataLib;
import com.splicemachine.si.data.light.LDataLib;
import com.splicemachine.si.data.light.LTuple;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class LDataLibTest {

		@Test
    public void testMultiValuesManyColumns() throws Exception {
				byte[] rowKey = Bytes.toBytes("fred");
				byte[] columnFamily = Bytes.toBytes("foo");
				byte[] ageQualifier = Bytes.toBytes("age");
				byte[] jobQualifier = Bytes.toBytes("jobQualifier");
				byte[] aliasQualifier = Bytes.toBytes("aliasQualifier");
				List<KeyValue> values = Arrays.asList(
                new KeyValue(rowKey, columnFamily, ageQualifier, 5L, Bytes.toBytes(21)),
                new KeyValue(rowKey, columnFamily, ageQualifier, 3L, Bytes.toBytes(11)),
                new KeyValue(rowKey, columnFamily, jobQualifier, 11L, Bytes.toBytes("baker")),
                new KeyValue(rowKey, columnFamily, aliasQualifier, 8L, Bytes.toBytes("joey")));
				Result tuple = new Result(values);
        SDataLib reader = new LDataLib();

        @SuppressWarnings("unchecked") List<KeyValue> keyValues = reader.listResult(tuple);
        List<KeyValue> results = Lists.newArrayList();
				for (KeyValue kv : keyValues) {
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
