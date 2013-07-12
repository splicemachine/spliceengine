package com.splicemachine.storage.index;

import com.google.common.collect.Lists;
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
    private static final int bitSetSize=100;

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        Random random = new Random();
        Collection<Object[]> data = Lists.newArrayListWithCapacity(bitSetSize);
        for(int i=0;i<bitSetSize;i++){
            BitSet bitSet  = new BitSet(i);
            BitSet lengthDelimitedBits = new BitSet(i);
            BitSet floatFields = new BitSet(i);
            BitSet doubleFields = new BitSet(i);
            for(int j=0;j<=i;j++){
                bitSet.set(j,random.nextBoolean());
                if(bitSet.get(j)){
                    lengthDelimitedBits.set(j,random.nextBoolean());
                    if(!lengthDelimitedBits.get(j)){
                        floatFields.set(j,random.nextBoolean());
                        if(!floatFields.get(j)){
                            doubleFields.set(j,random.nextBoolean());
                        }
                    }
                }
            }
            data.add(new Object[]{bitSet,lengthDelimitedBits,floatFields,doubleFields});
        }
        return data;
    }

    private final BitSet bitSet;
    private final BitSet lengthDelimitedBits;
    private final BitSet floatFields;
    private final BitSet doubleFields;

    public IterativeBitIndexTest(BitSet bitSet,BitSet lengthDelimitedBits,BitSet floatFields,BitSet doubleFields) {
        this.bitSet = bitSet;
        this.lengthDelimitedBits =lengthDelimitedBits;
        this.floatFields = floatFields;
        this.doubleFields = doubleFields;
    }

    @Test
    public void testCanEncodeAndDecodeDenseUncompressedProperly() throws Exception {
        BitIndex bitIndex = UncompressedBitIndex.create(bitSet,lengthDelimitedBits,floatFields,doubleFields);
        byte[] encode = bitIndex.encode();

        BitIndex decoded = UncompressedBitIndex.wrap(encode, 0, encode.length);
        Assert.assertEquals("Incorrect encode-decode of bitmap "+ bitSet,bitIndex,decoded);
    }

    @Test
    public void testCanEncodeAndDecodeDenseUncompressedLazyProperly() throws Exception {
        BitIndex bitIndex = UncompressedBitIndex.create(bitSet,lengthDelimitedBits,floatFields,doubleFields);
        byte[] encode = bitIndex.encode();

        BitIndex decoded = BitIndexing.uncompressedBitMap(encode,0,encode.length);

        for(int i=decoded.nextSetBit(0);i>=0;i=bitIndex.nextSetBit(i+1))
            Assert.assertTrue("Incorrect encode-decode of bitmap "+ bitSet,bitIndex.isSet(i));

        //equality is defined as the same bits set in each index
        for(int i=bitIndex.nextSetBit(0);i>=0;i=bitIndex.nextSetBit(i+1)){
            Assert.assertTrue("Incorrect encode-decode of bitmap "+ bitSet,decoded.isSet(i));
        }
    }

    @Test
    public void testCanEncodeAndDecodeIntersectsDenseUncompressedLazyProperly() throws Exception {
        BitIndex bitIndex = UncompressedBitIndex.create(bitSet,lengthDelimitedBits,floatFields,doubleFields);
        byte[] encode = bitIndex.encode();

        BitIndex decoded = BitIndexing.uncompressedBitMap(encode,0,encode.length);

        if(bitSet.isEmpty())
            Assert.assertTrue("Incorrect decoding of bitset "+ bitSet,decoded.isEmpty());
        else
            Assert.assertTrue("Intersection incorrect with bitset "+ bitSet,decoded.intersects(bitSet));
    }

    @Test
    public void testCanEncodeAndDecodeSparseProperly() throws Exception {
        BitIndex bitIndex = SparseBitIndex.create(bitSet,lengthDelimitedBits,floatFields,doubleFields);

        byte[] encode = bitIndex.encode();

        BitIndex decoded = SparseBitIndex.wrap(encode,0,encode.length);
        Assert.assertEquals("Incorrect encode-ecode of bitmap "+ bitSet,bitIndex,decoded);
    }

    @Test
    public void testCanEncodeAndDecodeSparseLazyProperly() throws Exception {
        BitIndex bitIndex = BitIndexing.sparseBitMap(bitSet,lengthDelimitedBits,floatFields,doubleFields);

        byte[] encode = bitIndex.encode();

        BitIndex decoded = BitIndexing.sparseBitMap(encode,0,encode.length);

        for(int i=decoded.nextSetBit(0);i>=0;i=bitIndex.nextSetBit(i+1))
            Assert.assertTrue("Incorrect encode-decode of bitmap "+ bitSet,bitIndex.isSet(i));

        //equality is defined as the same bits set in each index
        for(int i=bitIndex.nextSetBit(0);i>=0;i=bitIndex.nextSetBit(i+1)){
            Assert.assertTrue("Incorrect encode/decode of bitmap "+ bitSet,decoded.isSet(i));
        }
    }

    @Test
    public void testCanEncodeAndDecodeIntersectsSparseLazyProperly() throws Exception {
        BitIndex bitIndex = SparseBitIndex.create(bitSet,lengthDelimitedBits,floatFields,doubleFields);

        byte[] encode = bitIndex.encode();

        BitIndex decoded = BitIndexing.sparseBitMap(encode,0,encode.length);
        //equality is defined as the same bits set in each index
        if(bitSet.isEmpty())
            Assert.assertTrue("Incorrect decoding of bitset "+ bitSet,decoded.isEmpty());
        else
            Assert.assertTrue("Intersection incorrect with bitset "+ bitSet,decoded.intersects(bitSet));
    }

    @Test
    public void testCanEncodeAndDecodeDenseCompressedProperly() throws Exception {
        BitIndex bitIndex = BitIndexing.compressedBitMap(bitSet,lengthDelimitedBits,floatFields,doubleFields);
        byte[] encode = bitIndex.encode();

        BitIndex decoded = DenseCompressedBitIndex.wrap(encode,0,encode.length);
        Assert.assertEquals("Incorrect encode/decode of bitmap "+ bitSet,bitIndex,decoded);
    }

    @Test
    public void testIntersectsLazyCompressedProperly() throws Exception {
        BitIndex bitIndex =BitIndexing.compressedBitMap(bitSet,lengthDelimitedBits,floatFields,doubleFields);
        byte[] encode = bitIndex.encode();

        BitIndex decoded = BitIndexing.compressedBitMap(encode, 0, encode.length);

        //equality is defined as the same bits set in each index
        if(bitSet.isEmpty())
            Assert.assertTrue("Incorrect decoding of bitset "+ bitSet,decoded.isEmpty());
        else
            Assert.assertTrue("Intersection incorrect with bitset "+ bitSet,decoded.intersects(bitSet));
    }

    @Test
    public void testCanEncodeAndDecodeDenseCompressedLazyProperly() throws Exception {
        BitIndex bitIndex =BitIndexing.compressedBitMap(bitSet,lengthDelimitedBits,floatFields,doubleFields);
        byte[] encode = bitIndex.encode();

        BitIndex decoded = BitIndexing.compressedBitMap(encode, 0, encode.length);

        for(int i=decoded.nextSetBit(0);i>=0;i=bitIndex.nextSetBit(i+1))
            Assert.assertTrue("Incorrect encode-decode of bitmap "+ bitSet,bitIndex.isSet(i));

        //equality is defined as the same bits set in each index
        for(int i=bitIndex.nextSetBit(0);i>=0;i=bitIndex.nextSetBit(i+1)){
            Assert.assertTrue("Incorrect encode/decode of bitmap "+ bitSet,decoded.isSet(i));
        }
    }
}
