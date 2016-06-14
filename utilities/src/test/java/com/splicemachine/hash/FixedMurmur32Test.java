package com.splicemachine.hash;

import com.splicemachine.primitives.Bytes;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author Scott Fines
 *         Date: 4/18/15
 */
public class FixedMurmur32Test{

    private final Murmur32 murmur32 = new Murmur32(0);
    @Test
    public void testIntSameAsByteArray() throws Exception {
        for(int i=0;i<3000;i++){
            byte[] bytes=Bytes.toBytes(i);
            int correct=murmur32.hash(bytes,0,bytes.length);

            int actual=murmur32.hash(i);

            Assert.assertEquals("Incorrect int hash!",correct,actual);
        }
    }
}
