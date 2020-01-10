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

import com.carrotsearch.hppc.BitSet;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.storage.index.BitIndex;
import com.splicemachine.storage.index.BitIndexing;
import com.splicemachine.utils.kryo.KryoPool;
import org.junit.Assert;
import org.junit.Test;
import java.math.BigDecimal;

/**
 * @author Scott Fines
 * Created on: 10/3/13
 */
public class EntryPredicateFilterTest {
    private static KryoPool defaultPool = new KryoPool(100);

    @Test
    public void testReturnsAllColumnsForEmptyPredicate() throws Exception {
        EntryPredicateFilter predicateFilter = new EntryPredicateFilter(new BitSet(),true);

        BitSet setCols = new BitSet(2);
        setCols.set(0);
        setCols.set(1);

        BitSet scalarFields = new BitSet(2);
        BitSet floatFields = new BitSet(2);
        BitSet doubleFields = new BitSet(2);
        BitIndex index = BitIndexing.getBestIndex(setCols,scalarFields,floatFields,doubleFields);
        EntryEncoder encoder = EntryEncoder.create(defaultPool,index);

        String testType1 = "test";
        BigDecimal testType2 = new BigDecimal("2.345");
        encoder.getEntryEncoder().encodeNext(testType1).encodeNext(testType2);
        byte[] data = encoder.encode();

        EntryDecoder decoder = new EntryDecoder();
        decoder.set(data);

        EntryAccumulator accumulator = predicateFilter.newAccumulator();

        boolean match = predicateFilter.match(decoder, accumulator);
        Assert.assertTrue("did not match!",match);

        //make sure this row only returns back testTyp1
        byte[] retBytes = accumulator.finish();
        int i;
        //noinspection StatementWithEmptyBody
        for(i=0;i<retBytes.length&&retBytes[i]!=0x00;i++);
        BitIndex returnedIndex = BitIndexing.wrap(retBytes,0,i);

        //make sure that it only returns the zero field
        Assert.assertTrue("Index returned incorrectly!",returnedIndex.isSet(0));
        Assert.assertTrue("Index has incorrect fields!",returnedIndex.isSet(1));

        //make sure index is correct type
        Assert.assertFalse("got scalar type, when expected untyped",returnedIndex.isScalarType(0));
        Assert.assertFalse("got float type, when expected untyped",returnedIndex.isFloatType(0));
        Assert.assertFalse("got double type, when expected untyped",returnedIndex.isDoubleType(0));
        Assert.assertFalse("got scalar type, when expected untyped",returnedIndex.isScalarType(1));
        Assert.assertFalse("got float type, when expected untyped",returnedIndex.isFloatType(1));
        Assert.assertFalse("got double type, when expected untyped",returnedIndex.isDoubleType(1));

        //make sure returned data is correct
        MultiFieldDecoder fieldDecoder = MultiFieldDecoder.wrap(retBytes, i + 1, retBytes.length - (i + 1));

        String decodedField = fieldDecoder.decodeNextString();
        Assert.assertEquals("Incorrect string returned!",testType1,decodedField);
        BigDecimal t = fieldDecoder.decodeNextBigDecimal();
        Assert.assertTrue("Incorrect Big decimal returned! Expected "+ testType2+", but got "+ t,testType2.compareTo(t)==0);
        Assert.assertTrue("more than two fields available in field decoder!",fieldDecoder.nextIsNull());
    }

    @Test
    public void testEntryPredicateFilterReturnsOnlyMatchingColumnsFromRow() throws Exception {
        BitSet fieldsToReturn = new BitSet(2);
        fieldsToReturn.set(0);

        EntryPredicateFilter predicateFilter = new EntryPredicateFilter(fieldsToReturn,true);

        BitSet setCols = new BitSet(2);
        setCols.set(0);
        setCols.set(1);

        BitSet scalarFields = new BitSet(2);
        BitSet floatFields = new BitSet(2);
        BitSet doubleFields = new BitSet(2);
        BitIndex index = BitIndexing.getBestIndex(setCols,scalarFields,floatFields,doubleFields);
        EntryEncoder encoder = EntryEncoder.create(defaultPool,index);

        String testType1 = "test";
        BigDecimal testType2 = new BigDecimal("2.345");
        encoder.getEntryEncoder().encodeNext(testType1).encodeNext(testType2);
        byte[] data = encoder.encode();

        EntryDecoder decoder = new EntryDecoder();
        decoder.set(data);

        EntryAccumulator accumulator = predicateFilter.newAccumulator();

        boolean match = predicateFilter.match(decoder, accumulator);
        Assert.assertTrue("did not match!",match);

        //make sure this row only returns back testTyp1
        byte[] retBytes = accumulator.finish();
        int i;
        //noinspection StatementWithEmptyBody
        for(i=0;i<retBytes.length&&retBytes[i]!=0x00;i++);
        BitIndex returnedIndex = BitIndexing.wrap(retBytes,0,i);

        //make sure that it only returns the zero field
        Assert.assertTrue("Index returned incorrectly!",returnedIndex.isSet(0));
        Assert.assertFalse("Index has too many fields!",returnedIndex.isSet(1));

        //make sure index is correct type
        Assert.assertFalse("got scalar type, when expected untyped",returnedIndex.isScalarType(0));
        Assert.assertFalse("got float type, when expected untyped",returnedIndex.isFloatType(0));
        Assert.assertFalse("got double type, when expected untyped",returnedIndex.isDoubleType(0));

        //make sure returned data is correct
        MultiFieldDecoder fieldDecoder = MultiFieldDecoder.wrap(retBytes, i + 1, retBytes.length - (i + 1));

        String decodedField = fieldDecoder.decodeNextString();
        Assert.assertEquals("Incorrect string returned!",testType1,decodedField);
        Assert.assertTrue("more than one field available in field decoder!",fieldDecoder.nextIsNull());
    }
}
