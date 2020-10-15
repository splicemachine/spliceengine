/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.storage.index;

import com.carrotsearch.hppc.BitSet;
import splice.com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

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
                if (random.nextBoolean())
                    bitSet.set(j);
                if (random.nextBoolean())
                    lengthDelimitedBits.set(j);
                if (!lengthDelimitedBits.get(j)) {
                    if (random.nextBoolean())
                        floatFields.set(j);
                    if (!floatFields.get(j)) {
                        if (random.nextBoolean())
                            doubleFields.set(j);
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
        Assert.assertEquals("Incorrect encode-decode of bitmap "+ bitSet,compactTypeSets(bitIndex),decoded);
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
        Assert.assertEquals("Incorrect encode-ecode of bitmap "+ bitSet,compactTypeSets(bitIndex),decoded);
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
        Assert.assertEquals("Incorrect encode/decode of bitmap "+ bitSet,compactTypeSets(bitIndex),decoded);
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

    @Test
    public void testThatIndexesTreatBitSetsAsIfImmutable() throws Exception {
        /* Other code assumes that new BitIndexes can be created from existing indexes by reusing the
         * type-specific bitsets the index contains. Therefore it's unsafe for a BitIndex to modify
         * those bitsets. This tests verifies that no modification takes place.
         */
        BitIndex compressed = BitIndexing.compressedBitMap(bitSet, lengthDelimitedBits, floatFields,doubleFields);
        BitIndex uncompressed = BitIndexing.uncompressedBitMap(bitSet, lengthDelimitedBits, floatFields,doubleFields);
        BitIndex sparse = BitIndexing.sparseBitMap(bitSet, lengthDelimitedBits, floatFields,doubleFields);

        Assert.assertTrue("BitIndex construction should not modify type bitsets",
                             naryEquals(compressed.getScalarFields(), uncompressed.getScalarFields(),
                                           sparse.getScalarFields(), lengthDelimitedBits));
        Assert.assertTrue("BitIndex construction should not modify type bitsets",
                             naryEquals(compressed.getFloatFields(), uncompressed.getFloatFields(),
                                           sparse.getFloatFields(), floatFields));
        Assert.assertTrue("BitIndex construction should not modify type bitsets",
                             naryEquals(compressed.getDoubleFields(), uncompressed.getDoubleFields(),
                                           sparse.getDoubleFields(), doubleFields));
    }

    public static boolean naryEquals(Object first, Object ... args) {
        Object prev = first;
        for (Object arg: args){
            if (!prev.equals(arg)){
                return false;
            }
            prev = arg;
        }
        return true;
    }

    /*
     * Returns new BitIndex whose type BitSets (scalar, double, float)
     * are subsets of the main BitSet. This is useful for performing equality
     * comparisons on roundtripped BitIndexes, as encoding to bytes compacts
     * the type BitSets like this method does.
     */
    public static BitIndex compactTypeSets(BitIndex bIdx){
        BitSet set = new BitSet();
        BitSet scalars = new BitSet();
        BitSet floats = new BitSet();
        BitSet doubles = new BitSet();
        for (int pos = bIdx.nextSetBit(0); pos >= 0; pos = bIdx.nextSetBit(pos + 1)){
            set.set(pos);
            if (bIdx.isScalarType(pos)){ scalars.set(pos); }
            else if (bIdx.isFloatType(pos)){ floats.set(pos); }
            else if (bIdx.isDoubleType(pos)){ doubles.set(pos); }
        }
        BitIndex compactedIdx;
        if (bIdx instanceof SparseBitIndex){
            compactedIdx = BitIndexing.sparseBitMap(set, scalars, floats, doubles);
        } else if (bIdx instanceof DenseCompressedBitIndex){
            compactedIdx = BitIndexing.compressedBitMap(set, scalars, floats, doubles);
        } else if (bIdx instanceof UncompressedBitIndex){
            compactedIdx = BitIndexing.uncompressedBitMap(set, scalars, floats, doubles);
        } else {
            throw new RuntimeException("Don't know, bro");
        }
        return compactedIdx;
    }

    public static void main(String... args) throws Exception{
        BitSet comparisonSet = new BitSet(4000);
        comparisonSet.set(0);
        comparisonSet.set(1);
        comparisonSet.set(2);
        comparisonSet.set(3);
        comparisonSet.set(4);
        comparisonSet.set(5);


        BitIndex uncompressed = BitIndexing.uncompressedBitMap(comparisonSet,null,null,null);

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
