package com.splicemachine.constants.bytes;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class HashableBytesTest {

    @Test
    public void equality() {
        Assert.assertTrue(new HashableBytes(new byte[]{0}).equals(new HashableBytes(new byte[]{0})));
        Assert.assertTrue(new HashableBytes(new byte[]{0, 1, 2}).equals(new HashableBytes(new byte[]{0, 1, 2})));

        Assert.assertFalse(new HashableBytes(new byte[]{0}).equals(new HashableBytes(new byte[]{1})));
        Assert.assertFalse(new HashableBytes(new byte[]{0, 1, 2}).equals(new HashableBytes(new byte[]{0, 1})));
        Assert.assertFalse(new HashableBytes(new byte[]{0, 1, 2}).equals(new HashableBytes(new byte[]{0, 1, 9})));
    }

    @Test
    public void hash() {
        Assert.assertEquals(new HashableBytes(new byte[]{0}).hashCode(), new HashableBytes(new byte[]{0}).hashCode());
        Assert.assertNotEquals(new HashableBytes(new byte[]{0}).hashCode(), new HashableBytes(new byte[]{1}).hashCode());
        Assert.assertNotEquals(new HashableBytes(new byte[]{0, 1, 2}).hashCode(), new HashableBytes(new byte[]{0, 1, 3}).hashCode());
        Assert.assertNotEquals(new HashableBytes(new byte[]{0}).hashCode(), new HashableBytes(new byte[]{0, 1}).hashCode());
    }

    @Test
    public void inSet() {
        Set set = new HashSet();
        set.add(new HashableBytes(new byte[] {0, 1}));
        set.add(new HashableBytes(new byte[] {0, 1}));
        Assert.assertEquals(1, set.size());
        Assert.assertTrue(set.contains(new HashableBytes(new byte[]{0, 1})));
        Assert.assertFalse(set.contains(new HashableBytes(new byte[]{0, 2})));
        set.add(new HashableBytes(new byte[]{0, 2}));
        Assert.assertEquals(2, set.size());
        Assert.assertTrue(set.contains(new HashableBytes(new byte[]{0, 2})));
    }

    @Test
    public void getBytes() {
        Assert.assertTrue(Arrays.equals(new byte[] {0, 1}, new HashableBytes(new byte[] {0, 1}).getBytes()));
    }
}
