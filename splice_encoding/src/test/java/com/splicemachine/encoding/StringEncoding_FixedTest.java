package com.splicemachine.encoding;

import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;

public class StringEncoding_FixedTest {

    @Test
    public void testDecodeSYS() throws Exception {
        byte[] testBytes = StringEncoding.toBytes("SYS", false);
        assertEquals("[85, 91, 85]", Arrays.toString(testBytes));
    }

}
