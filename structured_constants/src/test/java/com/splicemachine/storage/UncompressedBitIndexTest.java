package com.splicemachine.storage;

import com.google.common.collect.Lists;
import com.splicemachine.storage.index.BitIndex;
import com.splicemachine.storage.index.UncompressedBitIndex;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.BitSet;
import java.util.Collection;
import java.util.Random;

/**
 * @author Scott Fines
 *         Created on: 7/5/13
 */
@RunWith(Parameterized.class)
public class UncompressedBitIndexTest {
    private static final int bitSetSize=100;

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        Random random = new Random();
        Collection<Object[]> data = Lists.newArrayListWithCapacity(bitSetSize);
        for(int i=0;i<bitSetSize;i++){
            BitSet bitSet  = new BitSet(i);
            for(int j=0;j<=i;j++){
                bitSet.set(j,random.nextBoolean());
            }
            data.add(new Object[]{bitSet});
        }
        return data;
    }

    private final BitSet bitSet;

    public UncompressedBitIndexTest(BitSet bitSet) {
        this.bitSet = bitSet;
    }

    @Test
    public void testCanEncodeAndDecodeProperly() throws Exception {
        BitIndex bitIndex = UncompressedBitIndex.create(bitSet);
        byte[] encode = bitIndex.encode();

        BitIndex decoded = UncompressedBitIndex.wrap(encode,0,encode.length);
        Assert.assertEquals(bitIndex,decoded);

    }
}
