package com.splicemachine.constants.bytes;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.Test;

import java.util.Random;

import static org.junit.Assert.*;

public class BytesUtilTest {

    byte[] A = Bytes.toBytes("AAA");
    byte[] B = Bytes.toBytes("BBB");
    byte[] C = Bytes.toBytes("CCC");
    byte[] D = Bytes.toBytes("DDD");
    byte[] E = Bytes.toBytes("EEE");

    @Test
    public void isKeyValueInRange() {
        Pair<byte[], byte[]> range = new Pair<byte[], byte[]>(B, D);
        assertTrue(BytesUtil.isKeyValueInRange(new KeyValue(B, 1L), range));
        assertTrue(BytesUtil.isKeyValueInRange(new KeyValue(C, 1L), range));
        assertTrue(BytesUtil.isKeyValueInRange(new KeyValue(D, 1L), range));
        assertFalse(BytesUtil.isKeyValueInRange(new KeyValue(A, 1L), range));
        assertFalse(BytesUtil.isKeyValueInRange(new KeyValue(E, 1L), range));
    }

    @Test
    public void isKeyValueInRange_unbounded() {
        Pair<byte[], byte[]> range = new Pair<byte[], byte[]>(new byte[]{}, new byte[]{});
        assertTrue(BytesUtil.isKeyValueInRange(new KeyValue(A, 1L), range));
        assertTrue(BytesUtil.isKeyValueInRange(new KeyValue(B, 1L), range));
        assertTrue(BytesUtil.isKeyValueInRange(new KeyValue(C, 1L), range));
        assertTrue(BytesUtil.isKeyValueInRange(new KeyValue(D, 1L), range));
        assertTrue(BytesUtil.isKeyValueInRange(new KeyValue(E, 1L), range));
    }

    @Test
    public void isKeyValueInRange_unboundedUpper() {
        Pair<byte[], byte[]> range = new Pair<byte[], byte[]>(C, new byte[]{});
        assertFalse(BytesUtil.isKeyValueInRange(new KeyValue(A, 1L), range));
        assertFalse(BytesUtil.isKeyValueInRange(new KeyValue(B, 1L), range));
        assertTrue(BytesUtil.isKeyValueInRange(new KeyValue(C, 1L), range));
        assertTrue(BytesUtil.isKeyValueInRange(new KeyValue(D, 1L), range));
        assertTrue(BytesUtil.isKeyValueInRange(new KeyValue(E, 1L), range));
    }

    @Test
    public void isKeyValueInRange_unboundedLower() {
        Pair<byte[], byte[]> range = new Pair<byte[], byte[]>(new byte[]{}, C);
        assertTrue(BytesUtil.isKeyValueInRange(new KeyValue(A, 1L), range));
        assertTrue(BytesUtil.isKeyValueInRange(new KeyValue(B, 1L), range));
        assertTrue(BytesUtil.isKeyValueInRange(new KeyValue(C, 1L), range));
        assertFalse(BytesUtil.isKeyValueInRange(new KeyValue(D, 1L), range));
        assertFalse(BytesUtil.isKeyValueInRange(new KeyValue(E, 1L), range));
    }

    @Test
    public void toHex_fromHex() {
        byte[] bytesIn = new byte[1024];
        new Random().nextBytes(bytesIn);

        String hex = BytesUtil.toHex(bytesIn);
        byte[] bytesOut = BytesUtil.fromHex(hex);

        assertArrayEquals(bytesIn, bytesOut);
    }

}
