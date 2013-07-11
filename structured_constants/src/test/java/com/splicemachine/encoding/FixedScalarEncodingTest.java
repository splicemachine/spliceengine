package com.splicemachine.encoding;

import org.junit.Test;

/**
 * @author Scott Fines
 *         Created on: 7/11/13
 */
public class FixedScalarEncodingTest {

    @Test
    public void testEncodeDecodeInteger() throws Exception {
        byte[] data = Encoding.encode(1280);
        int val = Encoding.decodeInt(data);
        System.out.println(val);
    }
}
