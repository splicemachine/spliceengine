package com.splicemachine.qpt;

import org.junit.Assert;
import org.junit.Test;

public class CommonTest {
    public static String repeat(String s, int n) {
        return new String(new char[n]).replace("\0", s);
    }

    @Test
    public void testEncoding() {
        Assert.assertEquals("XjOWk700", Encoding.makeId("X", 123456789));
        Assert.assertEquals("U0000000", Encoding.makeId("U", 0));
        Assert.assertEquals("AZZZZZZZ", Encoding.makeId("A", -1));
    }
}
