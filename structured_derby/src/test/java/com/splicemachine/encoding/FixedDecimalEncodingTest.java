package com.splicemachine.encoding;

import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;
import java.nio.ByteBuffer;

/**
 * @author Scott Fines
 *         Created on: 6/18/13
 */
public class FixedDecimalEncodingTest {

    @Test
    public void testCanEncodeDecodeByteSpecificByteBuffersCorrectly() throws Exception {
        BigDecimal value = new BigDecimal("37661026");
        byte[] encoding = DecimalEncoding.toBytes(value,false);
        BigDecimal decoded = DecimalEncoding.toBigDecimal(encoding,false);
        Assert.assertTrue("Incorrect decoding. Expected <"+value+">, Actual <"+ value+">",value.compareTo(decoded)==0);

        ByteBuffer encodeBuffer = ByteBuffer.wrap(encoding);
        decoded = DecimalEncoding.toBigDecimal(encodeBuffer,false);
        Assert.assertTrue("Incorrect decoding. Expected <"+value+">, Actual <"+ value+">",value.compareTo(decoded)==0);
    }
}
