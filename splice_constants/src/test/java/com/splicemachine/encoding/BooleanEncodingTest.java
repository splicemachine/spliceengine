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
        byte[] trueBytes = ScalarEncoding.writeBoolean(true,false);
        boolean shouldBeTrue = ScalarEncoding.readBoolean(trueBytes,false);

        Assert.assertTrue("Incorrect serialization of true!",shouldBeTrue);

        byte[] falseBytes = ScalarEncoding.writeBoolean(false,false);
        boolean shouldBeFalse = ScalarEncoding.readBoolean(falseBytes,false);

        Assert.assertFalse("Incorrect serialization of false!", shouldBeFalse);

        byte[] trueBytesDesc = ScalarEncoding.writeBoolean(true,true);
        boolean shouldBeTrueDesc = ScalarEncoding.readBoolean(trueBytesDesc,true);

        Assert.assertTrue("Incorrect serialization of true!", shouldBeTrueDesc);

        byte[] falseBytesDesc = ScalarEncoding.writeBoolean(false,true);
        boolean shouldBeFalseDesc = ScalarEncoding.readBoolean(falseBytesDesc,true);

        Assert.assertFalse("Incorrect serialization of false!",shouldBeFalseDesc);
    }

    @Test
    public void testSortsCorrectly() throws Exception {
        byte[] trueBytes = ScalarEncoding.writeBoolean(true,false);
        byte[] falseBytes = ScalarEncoding.writeBoolean(false,false);

        Assert.assertTrue("Incorrect sort--false before true!", Bytes.compareTo(trueBytes,falseBytes)<0);
        trueBytes = ScalarEncoding.writeBoolean(true,true);
        falseBytes = ScalarEncoding.writeBoolean(false,true);

        Assert.assertTrue("Incorrect sort--false after true!", Bytes.compareTo(trueBytes,falseBytes)>0);
    }
}
