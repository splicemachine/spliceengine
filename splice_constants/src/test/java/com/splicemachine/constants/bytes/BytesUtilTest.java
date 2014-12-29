package com.splicemachine.constants.bytes;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class BytesUtilTest {

    byte[] A = Bytes.toBytes("AAA");
    byte[] B = Bytes.toBytes("BBB");
    byte[] C = Bytes.toBytes("CCC");
    byte[] D = Bytes.toBytes("DDD");
    byte[] E = Bytes.toBytes("EEE");

    @Test
    public void isKeyValueInRange() {
        Pair<byte[], byte[]> range = new Pair<>(B, D);
        assertTrue(BytesUtil.isKeyValueInRange(new KeyValue(B, 1L), range));
        assertTrue(BytesUtil.isKeyValueInRange(new KeyValue(C, 1L), range));
        assertTrue(BytesUtil.isKeyValueInRange(new KeyValue(D, 1L), range));
        assertFalse(BytesUtil.isKeyValueInRange(new KeyValue(A, 1L), range));
        assertFalse(BytesUtil.isKeyValueInRange(new KeyValue(E, 1L), range));
    }

    @Test
    public void isKeyValueInRange_unbounded() {
        Pair<byte[], byte[]> range = new Pair<>(new byte[]{}, new byte[]{});
        assertTrue(BytesUtil.isKeyValueInRange(new KeyValue(A, 1L), range));
        assertTrue(BytesUtil.isKeyValueInRange(new KeyValue(B, 1L), range));
        assertTrue(BytesUtil.isKeyValueInRange(new KeyValue(C, 1L), range));
        assertTrue(BytesUtil.isKeyValueInRange(new KeyValue(D, 1L), range));
        assertTrue(BytesUtil.isKeyValueInRange(new KeyValue(E, 1L), range));
    }

    @Test
    public void isKeyValueInRange_unboundedUpper() {
        Pair<byte[], byte[]> range = new Pair<>(C, new byte[]{});
        assertFalse(BytesUtil.isKeyValueInRange(new KeyValue(A, 1L), range));
        assertFalse(BytesUtil.isKeyValueInRange(new KeyValue(B, 1L), range));
        assertTrue(BytesUtil.isKeyValueInRange(new KeyValue(C, 1L), range));
        assertTrue(BytesUtil.isKeyValueInRange(new KeyValue(D, 1L), range));
        assertTrue(BytesUtil.isKeyValueInRange(new KeyValue(E, 1L), range));
    }

    @Test
    public void isKeyValueInRange_unboundedLower() {
        Pair<byte[], byte[]> range = new Pair<>(new byte[]{}, C);
        assertTrue(BytesUtil.isKeyValueInRange(new KeyValue(A, 1L), range));
        assertTrue(BytesUtil.isKeyValueInRange(new KeyValue(B, 1L), range));
        assertTrue(BytesUtil.isKeyValueInRange(new KeyValue(C, 1L), range));
        assertFalse(BytesUtil.isKeyValueInRange(new KeyValue(D, 1L), range));
        assertFalse(BytesUtil.isKeyValueInRange(new KeyValue(E, 1L), range));
    }

    @Test
    public void toHex_fromHex()  {
        byte[] encoded = Bytes.toBytes("test");

        String hex = BytesUtil.toHex(encoded);
        byte[] decoded = BytesUtil.fromHex(hex);

        assertEquals("test", Bytes.toString(decoded));
    }

}
