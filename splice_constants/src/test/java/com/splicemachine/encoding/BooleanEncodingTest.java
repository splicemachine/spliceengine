package com.splicemachine.encoding;

import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author Scott Fines
 *         Created on: 6/8/13
 */
public class BooleanEncodingTest {


    @Test
    public void testSerializesAndDeserializesCorrectly() throws Exception {
        byte[] trueBytes = ScalarEncoding.toBytes(true, false);
        boolean shouldBeTrue = ScalarEncoding.toBoolean(trueBytes, false);

        Assert.assertTrue("Incorrect serialization of true!",shouldBeTrue);

        byte[] falseBytes = ScalarEncoding.toBytes(false, false);
        boolean shouldBeFalse = ScalarEncoding.toBoolean(falseBytes, false);

        Assert.assertFalse("Incorrect serialization of false!", shouldBeFalse);

        byte[] trueBytesDesc = ScalarEncoding.toBytes(true, true);
        boolean shouldBeTrueDesc = ScalarEncoding.toBoolean(trueBytesDesc, true);

        Assert.assertTrue("Incorrect serialization of true!", shouldBeTrueDesc);

        byte[] falseBytesDesc = ScalarEncoding.toBytes(false, true);
        boolean shouldBeFalseDesc = ScalarEncoding.toBoolean(falseBytesDesc, true);

        Assert.assertFalse("Incorrect serialization of false!",shouldBeFalseDesc);
    }

    @Test
    public void testSortsCorrectly() throws Exception {
        byte[] trueBytes = ScalarEncoding.toBytes(true, false);
        byte[] falseBytes = ScalarEncoding.toBytes(false, false);

        Assert.assertTrue("Incorrect sort--false before true!", Bytes.compareTo(trueBytes,falseBytes)<0);
        trueBytes = ScalarEncoding.toBytes(true, true);
        falseBytes = ScalarEncoding.toBytes(false, true);

        Assert.assertTrue("Incorrect sort--false after true!", Bytes.compareTo(trueBytes,falseBytes)>0);
    }
}
