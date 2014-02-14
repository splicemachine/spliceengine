package com.splicemachine.si;

import com.google.common.collect.Lists;
import com.splicemachine.si.data.api.SDataLib;
import com.splicemachine.si.data.light.LDataLib;
import com.splicemachine.si.data.light.LTuple;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class LDataLibTest {

    @SuppressWarnings("unchecked")
		@Test
    public void testSingleVal() throws Exception {
				byte[] value = Bytes.toBytes(21);
				List<KeyValue> values = Arrays.asList(new KeyValue(Bytes.toBytes("fred"), Bytes.toBytes("foo"), Bytes.toBytes("age"), 5L, value));
				Result tuple = new Result(values);
//        LTuple tuple = new LTuple(Bytes.toBytes("fred"), values);
        SDataLib reader = new LDataLib();
        Assert.assertEquals("key is wrong", "fred", Bytes.toString(tuple.getRow()));
        List<KeyValue> result = tuple.getColumn(Bytes.toBytes("foo"), Bytes.toBytes("age"));
        Assert.assertEquals(1, result.size());
        Assert.assertArrayEquals(value, result.get(0).getValue());
    }

    @SuppressWarnings("unchecked")
		@Test
    public void testMultiValuesForOneColumn() throws Exception {
				byte[] key = Bytes.toBytes("fred");
				byte[] columnFamily = Bytes.toBytes("foo");
				byte[] qualifier = Bytes.toBytes("age");
				List<KeyValue> values = Arrays.asList(
                new KeyValue(key, columnFamily, qualifier, 5L, Bytes.toBytes(21)),
                new KeyValue(key, columnFamily, qualifier, 3L, Bytes.toBytes(11)),
                new KeyValue(key, columnFamily, qualifier, 11L, Bytes.toBytes(41)),
                new KeyValue(key, columnFamily, qualifier, 8L, Bytes.toBytes(31)));
				Result tuple = new Result(values);
        SDataLib reader = new LDataLib();
        Assert.assertEquals("key is wrong", "fred", Bytes.toString(tuple.getRow()));
				List<KeyValue> result = tuple.getColumn(columnFamily,qualifier);
        Assert.assertEquals(4, result.size());
        Assert.assertEquals(41,Bytes.toInt(result.get(0).getValue()));
        Assert.assertEquals(31, Bytes.toInt(result.get(1).getValue()));
        Assert.assertEquals(21,Bytes.toInt(result.get(2).getValue()));
        Assert.assertEquals(11, Bytes.toInt(result.get(3).getValue()));

        byte[] latestValue = tuple.getValue(columnFamily, qualifier);
        Assert.assertEquals(41, Bytes.toInt(latestValue));
    }

    @SuppressWarnings("unchecked")
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

        List<KeyValue> keyValues = reader.listResult(tuple);
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
