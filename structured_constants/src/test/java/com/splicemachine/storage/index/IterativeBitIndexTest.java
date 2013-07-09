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
    public void testCanEncodeAndDecodeDenseUncompressedLazyProperly() throws Exception {
        BitIndex bitIndex = UncompressedBitIndex.create(bitSet);
        byte[] encode = bitIndex.encode();

        BitIndex decoded = BitIndexing.uncompressedBitMap(encode,0,encode.length);

        //equality is defined as the same bits set in each index
        for(int i=bitIndex.nextSetBit(0);i>=0;i=bitIndex.nextSetBit(i+1)){
            Assert.assertTrue(decoded.isSet(i));
        }
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
    public void testCanEncodeAndDecodeSparseLazyProperly() throws Exception {
        BitIndex bitIndex = SparseBitIndex.create(bitSet);
        System.out.println(bitIndex);

        byte[] encode = bitIndex.encode();

        BitIndex decoded = BitIndexing.sparseBitMap(encode,0,encode.length);
        System.out.println(bitIndex);
        //equality is defined as the same bits set in each index
        for(int i=bitIndex.nextSetBit(0);i>=0;i=bitIndex.nextSetBit(i+1)){
            Assert.assertTrue(decoded.isSet(i));
        }
    }

    @Test
    public void testCanEncodeAndDecodeDenseCompressedProperly() throws Exception {
        BitIndex bitIndex = DenseCompressedBitIndex.compress(bitSet);
//        System.out.println(bitIndex);

        byte[] encode = bitIndex.encode();

        BitIndex decoded = DenseCompressedBitIndex.wrap(encode,0,encode.length);
        Assert.assertEquals(bitIndex,decoded);

    }

    @Test
    public void testCanEncodeAndDecodeDenseCompressedLazyProperly() throws Exception {
        BitIndex bitIndex =BitIndexing.compressedBitMap(bitSet);
        byte[] encode = bitIndex.encode();

        BitIndex decoded = BitIndexing.compressedBitMap(encode,0,encode.length);

        //equality is defined as the same bits set in each index
        for(int i=bitIndex.nextSetBit(0);i>=0;i=bitIndex.nextSetBit(i+1)){
            Assert.assertTrue(decoded.isSet(i));
        }
    }

    public static void main(String... args) throws Exception{
        BitSet comparisonSet = new BitSet(4000);
        comparisonSet.set(0);
        comparisonSet.set(1);
        comparisonSet.set(2);
        comparisonSet.set(3);
        comparisonSet.set(4);
        comparisonSet.set(5);


        BitIndex uncompressed = BitIndexing.uncompressedBitMap(comparisonSet);

        byte[] encoded = uncompressed.encode();
        BitIndex lazy = BitIndexing.uncompressedBitMap(encoded,0,encoded.length);

        for(int i=uncompressed.nextSetBit(0);i>=0;i=uncompressed.nextSetBit(i+1)){
            if(!lazy.isSet(i)){
                System.out.println(i);
            }
        }
//        System.out.printf("uncompressed size=%d%n",uncompressed.encodedSize());
//        System.out.printf("compressed size=%d%n",compressed.encodedSize());
//        System.out.printf("sparse size=%d%n",sparse.encodedSize());

    }
}
