package com.splicemachine.encoding;

import org.junit.Test;

import java.util.Arrays;

/**
 * Tests special case encoding scenarios (regression tests, etc.)
 *
 * @author Scott Fines
 * Created on: 6/13/13
 */
public class SpecialEncodingTest {

    @Test
    public void testDecodeSYS() throws Exception {
        byte[] testBytes = StringEncoding.toBytes("SYS",false);
        System.out.println(Arrays.toString(testBytes));

    }
}
