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

package com.splicemachine.storage;

import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.encoding.MultiFieldEncoder;
import com.splicemachine.encoding.TestType;
import com.splicemachine.utils.Pair;
import com.splicemachine.utils.kryo.KryoPool;
import org.junit.Assert;
import org.junit.Test;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.carrotsearch.hppc.BitSet;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

/**
 * @author Scott Fines
 * Created on: 7/5/13
 */
public class EntryEncoderTest {
    private static KryoPool defaultPool = new KryoPool(100);
    @Test
    public void testCanEncodeAndDecodeCorrectlyCompressed() throws Exception {
        BitSet setBits = new BitSet(10);
        setBits.set(1);
        setBits.set(3);
        setBits.set(8);
        EntryEncoder encoder = EntryEncoder.create(defaultPool,10, setBits, null, null, null);

        String longTest = "hello this is a tale of woe and sadness from which we will never returnaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaabbbbbbbbbbbbbbbbbbbbbbbbbbbbeeeeeeeeeeeeeeeeeeeeeeeeeeeeea";
        BigDecimal correct = new BigDecimal("22.456789012345667890230456677890192348576");
        MultiFieldEncoder entryEncoder = encoder.getEntryEncoder();
        entryEncoder.encodeNext(1);
        entryEncoder.encodeNext(longTest);
        entryEncoder.encodeNext(correct);

        byte[] encode = encoder.encode();

        EntryDecoder decoder = new EntryDecoder();
        decoder.set(encode);

        Assert.assertTrue(decoder.isSet(1));
        Assert.assertTrue(decoder.isSet(3));
        Assert.assertTrue(decoder.isSet(8));
        Assert.assertFalse(decoder.isSet(4));


        MultiFieldDecoder fieldDecoder = decoder.getEntryDecoder();

        Assert.assertEquals(1,fieldDecoder.decodeNextInt());
        Assert.assertEquals(longTest,fieldDecoder.decodeNextString());
        BigDecimal next = fieldDecoder.decodeNextBigDecimal();
        Assert.assertTrue("expected: "+ correct+", actual: "+ next,correct.compareTo(next)==0);

    }

    @Test
    public void testCanEncodeAndDecodeCorrectlyUncompressed() throws Exception {
        BitSet setBits = new BitSet(10);
        setBits.set(1);
        setBits.set(3);
        setBits.set(8);
        EntryEncoder encoder = EntryEncoder.create(defaultPool,10,setBits,null,null,null);

        String longTest = "hello this is a tale of woe and sadness from which we will never return";
        BigDecimal correct = new BigDecimal("22.456789012345667890230456677890192348576");
        MultiFieldEncoder entryEncoder = encoder.getEntryEncoder();
        entryEncoder.encodeNext(1);
        entryEncoder.encodeNext(longTest);
        entryEncoder.encodeNext(correct);

        byte[] encode = encoder.encode();

        EntryDecoder decoder = new EntryDecoder();
        decoder.set(encode);

        Assert.assertTrue(decoder.isSet(1));
        Assert.assertTrue(decoder.isSet(3));
        Assert.assertTrue(decoder.isSet(8));
        Assert.assertFalse(decoder.isSet(4));


        MultiFieldDecoder fieldDecoder = decoder.getEntryDecoder();

        Assert.assertEquals(1,fieldDecoder.decodeNextInt());
        Assert.assertEquals(longTest,fieldDecoder.decodeNextString());
        BigDecimal next = fieldDecoder.decodeNextBigDecimal();
        Assert.assertTrue("expected: "+ correct+", actual: "+ next,correct.compareTo(next)==0);

    }

    @Test
    public void testEncodeAllColumnsSafely() throws Exception {
        BitSet setCols = new BitSet();
        setCols.set(0);
        setCols.set(1);
        setCols.set(2);

        BitSet scalarFields = new BitSet();
        scalarFields.set(1);
        EntryEncoder encoder = EntryEncoder.create(defaultPool,3, setCols,scalarFields,null,null);
        String longTest = "hello this is a tale of woe and sadness from which we will never return";
        BigDecimal correct = new BigDecimal("22.456789012345667890230456677890192348576");
        MultiFieldEncoder entryEncoder = encoder.getEntryEncoder();
        entryEncoder.encodeNext(1);
        entryEncoder.encodeNext(longTest);
        entryEncoder.encodeNext(correct);

        byte[] encode = encoder.encode();

        EntryDecoder decoder = new EntryDecoder();
        decoder.set(encode);

        Assert.assertTrue(decoder.isSet(0));
        Assert.assertTrue(decoder.isSet(1));
        Assert.assertTrue(decoder.isSet(2));


        MultiFieldDecoder fieldDecoder = decoder.getEntryDecoder();

        Assert.assertEquals(1,fieldDecoder.decodeNextInt());
        Assert.assertEquals(longTest,fieldDecoder.decodeNextString());
        BigDecimal next = fieldDecoder.decodeNextBigDecimal();
        Assert.assertTrue("expected: "+ correct+", actual: "+ next,correct.compareTo(next)==0);

    }

    @Test
    public void testMelding() throws Exception {
        meldTest(asList(TestType.BOOLEAN, TestType.INTEGER), asList(Boolean.TRUE, 100),
                 singletonList(TestType.INTEGER), singletonList(42), singletonList(1),
                 asList(TestType.BOOLEAN, TestType.INTEGER), asList(Boolean.TRUE, 42));
    }

    @Test
    public void testMeldingRandomData() throws Exception {
        for(int i = 0; i < 100; i++) {
            List<Pair<TestType, Object>> orig = generateRandomEncoder(200);
            Pair<List<Pair<TestType, Object>>, List<Integer>> blindUpdate = generateBlindUpdate(orig, 100);
            List<Pair<TestType, Object>> expected = generateExpectedResult(orig,blindUpdate);
            meldTest(orig, blindUpdate.getFirst(), blindUpdate.getSecond(), expected);
        }
    }

    ////////////////////////////// helper functions //////////////////////////////////////////////////

    /**
     * Generates a random list of {@link TestType} and values for simulating an encoded row
     * with {@code colCount} columns.
     * @param colCount The number of columns.
     * @return randomly generated list of {@link TestType} and values.
     */
    private List<Pair<TestType, Object>> generateRandomEncoder(int colCount) {
        assert colCount > 0;

        List<Pair<TestType, Object>> result = new ArrayList<>(colCount);
        Random random = new Random(0L);
        for(int i = 0; i < colCount; ++i) {
            TestType type = TestType.values()[random.nextInt(TestType.values().length)];
            Object value = type.generateRandom(random);
            result.add(new Pair<>(type, value));
        }
        return result;
    }

    /**
     * Simulates a blind update where some columns from the passed {@code orig} are updated. The updated columns are
     * randomly picked.
     *
     * @param orig the list of {@link TestType} and values representing the row which we want to blindly update.
     * @param updatedColCount the desired number of updated columns.
     *
     * @return a {@link Pair} of randomly picked update columns and a list of the updated column indices.
     *
     * Example:
     *    orig = { {INTEGER, 42}, {STRING, "foo"}, {Byte, 0x14}, {FLOAT, 3.14} }
     *    updatedColCount = 2
     *    possible output:
     *    < {{STRING, "bar"}, {FLOAT, 6.28}}, {1,3}  >
     */
    private Pair<List<Pair<TestType, Object>>, List<Integer>> generateBlindUpdate(List<Pair<TestType, Object>> orig,
                                                                          int updatedColCount) {
        List<Pair<TestType, Object>> list = new ArrayList<>(updatedColCount);
        List<Integer> updatedColIndices = new ArrayList<>(updatedColCount);
        Random random = new Random(0L);
        for(int i = 0; i < updatedColCount; ++i) {
            int pickedColIndex = random.nextInt(orig.size());
            while(updatedColIndices.contains(pickedColIndex)) {
                pickedColIndex = random.nextInt(orig.size());
            }
            updatedColIndices.add(pickedColIndex);
        }
        updatedColIndices.sort(Integer::compareTo);

        for(int i = 0; i < updatedColCount; i++) {
            int pickedColIndex = updatedColIndices.get(i);
            Pair<TestType, Object> pickedCol = orig.get(pickedColIndex);
            Object newValue = pickedCol.getFirst().generateRandom(random);
            list.add(new Pair<>(pickedCol.getFirst(), newValue));
        }
        return new Pair<>(list, updatedColIndices);
    }

    /**
     * Generates a list of expected melding result.
     *
     * @param orig the list of {@link TestType} and values representing the row which we want to blindly update.
     * @param blindUpdate the blind update, i.e. list of randomly chosen columns' {@link TestType} and values and their
     *                    indices.
     * @return the list of {@link TestType} and values representing the resulting melded row.
     *
     * Example:
     *    orig = { {INTEGER, 42}, {STRING, "foo"}, {Byte, 0x14}, {FLOAT, 3.14} }
     *    blinkUpdate = < {{STRING, "bar"}, {FLOAT, 6.28}}, {1,3}  >
     *    result:
     *   { {INTEGER, 42}, {STRING, "bar"}, {Byte, 0x14}, {FLOAT, 6.28} }
     */
    private static List<Pair<TestType, Object>> generateExpectedResult(List<Pair<TestType, Object>> orig,
                                                                       Pair<List<Pair<TestType, Object>>, List<Integer>> blindUpdate) {
        assert orig != null;
        assert blindUpdate != null;
        assert blindUpdate.getFirst() != null && blindUpdate.getFirst().size() < orig.size();
        assert blindUpdate.getSecond() != null;

        List<Pair<TestType, Object>> result = new ArrayList<>(orig);

        int cnt = 0;
        for(int updateColIndex : blindUpdate.getSecond()) {
            result.set(updateColIndex, blindUpdate.getFirst().get(cnt++));
        }

        return result;
    }

    private static void meldTest(List<TestType> toTypes, List<Object> toValues,
                                 List<TestType> fromTypes, List<Object> fromValues, List<Integer> fromSetBits,
                                 List<TestType> expectedTypes, List<Object> expectedValues) {
        assert toTypes != null && toValues != null && toTypes.size() == toValues.size();
        assert fromTypes != null && fromValues != null && fromTypes.size() == fromValues.size();
        assert expectedTypes != null && expectedValues != null && expectedTypes.size() == expectedValues.size();
        assert fromTypes.size() <= toTypes.size();
        assert fromSetBits != null && fromSetBits.size() <= fromTypes.size();

        meldTest(zip(toTypes, toValues), zip(fromTypes, fromValues),fromSetBits, zip(expectedTypes, expectedValues));
    }

    private static void meldTest(List<Pair<TestType, Object>> toDecoderData,
                                 List<Pair<TestType, Object>> fromDecoderData, List<Integer> fromSetBits,
                                 List<Pair<TestType, Object>> expectedData ) {

        BitSet bitSet = new BitSet();
        for(Integer setBit : fromSetBits) {
            bitSet.set(setBit);
        }

        EntryDecoder toDecoder = generateDecoder(toDecoderData, toDecoderData.size());
        EntryDecoder fromDecoder = generateDecoder(fromDecoderData, fromDecoderData.size(), bitSet);

        EntryEncoder resultEncoder = EntryEncoder.create(defaultPool, toDecoder.getCurrentIndex());

        Utils.meld(toDecoder, fromDecoder, resultEncoder);

        valuesShouldBe(expectedData,resultEncoder);
    }

    public static <A, B> List<Pair<A, B>> zip(List<A> as, List<B> bs) {
        assert as != null && bs != null && as.size() == bs.size();
        return IntStream.range(0, as.size())
                .mapToObj(i -> new Pair<>(as.get(i), bs.get(i)))
                .collect(Collectors.toList());
    }

    private static EntryDecoder generateDecoder(List<Pair<TestType, Object>> data,
                                                int colCount) {
        return generateDecoder(data, colCount, null);
    }

    /**
     * Generates an {@link EntryDecoder} from a list of given {@link TestType} and values.
     *
     * @param data The {@link EntryDecoder}'s data.
     * @param colCount The number of columns, note that it could be different from the data size.
     * @param setBits A bitset of all set columns from pair-wise data.
     * @return a {@link EntryDecoder} with all {@code data} encoded properly and bitset indexes generated correctly.
     */
    private static EntryDecoder generateDecoder(List<Pair<TestType, Object>> data,
                                                int colCount,
                                                BitSet setBits) {
         boolean withSetBits = false;
        if(setBits == null) {
            setBits = new BitSet();
            withSetBits = true;
        }
        BitSet scalarBitSet = new BitSet();
        BitSet floatBitSet = new BitSet();
        BitSet doubleBitSet = new BitSet();
        int cnt = 0;
        if(!withSetBits) {
            for (int i = setBits.nextSetBit(0); i >= 0; i = setBits.nextSetBit(i + 1)) {
                TestType testType = data.get(cnt++).getFirst();
                if(testType.isScalarType()) {
                    scalarBitSet.set(i);
                } else if(testType.isDoubleType()) {
                    doubleBitSet.set(i);
                } else if(testType.isFloatType()) {
                    floatBitSet.set(i);
                }
            }
        } else {
            for (int i = 0; i < data.size(); ++i) {
                TestType testType = data.get(i).getFirst();
                if (testType.isScalarType()) {
                    scalarBitSet.set(i);
                } else if (testType.isDoubleType()) {
                    doubleBitSet.set(i);
                } else if (testType.isFloatType()) {
                    floatBitSet.set(i);
                }
                setBits.set(i);
            }
        }
        EntryEncoder entryEncoder = EntryEncoder.create(defaultPool, data.size(), setBits, scalarBitSet, floatBitSet, doubleBitSet);
        MultiFieldEncoder encoder = entryEncoder.getEntryEncoder();
        for (Pair<TestType, Object> datum : data) {
            datum.getFirst().load(encoder, datum.getSecond());
        }
        EntryDecoder result = new EntryDecoder();
        result.set(entryEncoder.encode());
        return result;
    }

    private static void valuesShouldBe(List<Pair<TestType, Object>> data,
                                       EntryEncoder encoder) {
        EntryDecoder decoder = new EntryDecoder();
        decoder.set(encoder.encode());
        MultiFieldDecoder multiFieldDecoder = decoder.getEntryDecoder();
        for(Pair<TestType, Object> datum : data) {
            datum.getFirst().check(multiFieldDecoder,datum.getSecond(), false);
        }
    }
}
