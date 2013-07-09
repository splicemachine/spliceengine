package com.splicemachine.storage.index;

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
 * Created on: 7/5/13
 */
@RunWith(Parameterized.class)
public class IterativeBitIndexTest {
    private static final int bitSetSize=2000;

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

    public IterativeBitIndexTest(BitSet bitSet) {
        this.bitSet = bitSet;
    }

    @Test
    public void testCanEncodeAndDecodeDenseUncompressedProperly() throws Exception {
        BitIndex bitIndex = UncompressedBitIndex.create(bitSet);
        byte[] encode = bitIndex.encode();

        BitIndex decoded = UncompressedBitIndex.wrap(encode,0,encode.length);
        Assert.assertEquals(bitIndex,decoded);

    }

    @Test
    public void testCanEncodeAndDecodeSparseProperly() throws Exception {
        BitIndex bitIndex = SparseBitIndex.create(bitSet);
        System.out.println(bitIndex);

        byte[] encode = bitIndex.encode();

        BitIndex decoded = SparseBitIndex.wrap(encode,0,encode.length);
        Assert.assertEquals(bitIndex,decoded);
    }

    @Test
    public void testCanEncodeAndDecodeDenseCompressedProperly() throws Exception {
        BitIndex bitIndex = DenseCompressedBitIndex.compress(bitSet);
//        System.out.println(bitIndex);

        byte[] encode = bitIndex.encode();

        BitIndex decoded = DenseCompressedBitIndex.wrap(encode,0,encode.length);
        Assert.assertEquals(bitIndex,decoded);

    }

    public static void main(String... args) throws Exception{
        BitSet comparisonSet = new BitSet(4000);
        comparisonSet.set(0);
        comparisonSet.set(2);
        comparisonSet.set(4);
//        comparisonSet.set(6);
//        comparisonSet.set(8);
        comparisonSet.set(10);
//        comparisonSet.set(12);
        comparisonSet.set(14);
//        comparisonSet.set(16);
//        comparisonSet.set(18);
        comparisonSet.set(20);
        comparisonSet.set(22);

        BitIndex uncompressed = BitIndexing.uncompressedBitMap(comparisonSet);
        BitIndex compressed = BitIndexing.compressedBitMap(comparisonSet);
        BitIndex sparse = BitIndexing.sparseBitMap(comparisonSet);

        System.out.printf("uncompressed size=%d%n",uncompressed.encodedSize());
        System.out.printf("compressed size=%d%n",compressed.encodedSize());
        System.out.printf("sparse size=%d%n",sparse.encodedSize());

    }
}
