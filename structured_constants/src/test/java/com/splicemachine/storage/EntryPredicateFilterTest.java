package com.splicemachine.storage;

import com.splicemachine.encoding.Encoding;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.storage.index.BitIndex;
import com.splicemachine.storage.index.BitIndexing;
import com.splicemachine.utils.kryo.KryoPool;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.BitSet;
import java.util.Collections;

/**
 * @author Scott Fines
 * Created on: 10/3/13
 */
public class EntryPredicateFilterTest {

    @Test
    public void testFiltersOutRowsWithMissingColumnsToRemove() throws Exception {
        /*
         * Test that if we have a NullPredicate that is never touched explicitly, that
         * it will still filter out values. This is the situation where a sparse encoding
         * is just missing the entry
         */

        BitSet setCols = new BitSet(2);
        setCols.set(0);
        setCols.set(1);

        BitSet scalarFields = new BitSet(2);
        BitSet floatFields = new BitSet(2);
        BitSet doubleFields = new BitSet(2);
        BitIndex index = BitIndexing.getBestIndex(setCols,scalarFields,floatFields,doubleFields);
        EntryEncoder encoder = EntryEncoder.create(KryoPool.defaultPool(),index);

        String testType1 = "test";
        BigDecimal testType2 = new BigDecimal("2.345");

        Predicate pred = new NullPredicate(true,true,2,false,false);
        EntryPredicateFilter predicateFilter = new EntryPredicateFilter(new BitSet(),
                Collections.singletonList(pred),true);

        encoder.getEntryEncoder().encodeNext(testType1).encodeNext(testType2);
        byte[] data = encoder.encode();

        EntryDecoder decoder = new EntryDecoder(KryoPool.defaultPool());
        decoder.set(data);

        EntryAccumulator accumulator = predicateFilter.newAccumulator();

        boolean match = predicateFilter.match(decoder, accumulator);
        Assert.assertTrue("matched!",match);

        Assert.assertNull("bytes returned even though predicate filter should have filtered it out",accumulator.finish());
    }

    @Test
    public void testEntryPredicateFilterFiltersRowsWithDontMatchPredicate() throws Exception {
        BitSet fieldsToReturn = new BitSet(2);
        fieldsToReturn.set(1);

        BitSet setCols = new BitSet(2);
        setCols.set(0);
        setCols.set(1);

        BitSet scalarFields = new BitSet(2);
        BitSet floatFields = new BitSet(2);
        BitSet doubleFields = new BitSet(2);
        BitIndex index = BitIndexing.getBestIndex(setCols,scalarFields,floatFields,doubleFields);
        EntryEncoder encoder = EntryEncoder.create(KryoPool.defaultPool(),index);

        String testType1 = "test";
        BigDecimal testType2 = new BigDecimal("2.345");

        Predicate pred = new ValuePredicate(CompareFilter.CompareOp.EQUAL,1, Encoding.encode("test2"),true);
        EntryPredicateFilter predicateFilter = new EntryPredicateFilter(fieldsToReturn,
                Collections.singletonList(pred),true);

        encoder.getEntryEncoder().encodeNext(testType1).encodeNext(testType2);
        byte[] data = encoder.encode();

        EntryDecoder decoder = new EntryDecoder(KryoPool.defaultPool());
        decoder.set(data);

        EntryAccumulator accumulator = predicateFilter.newAccumulator();

        boolean match = predicateFilter.match(decoder, accumulator);
        Assert.assertFalse("matched!",match);
    }

    @Test
    public void testReturnsEverythingThatMatchesPredicate() throws Exception {
     String testType1 = "test";
     BigDecimal testType2 = new BigDecimal("2.345");
     Predicate pred = new ValuePredicate(CompareFilter.CompareOp.EQUAL,0, Encoding.encode(testType1),true);
        EntryPredicateFilter predicateFilter = new EntryPredicateFilter(new BitSet(),
                Collections.singletonList(pred),true);

        BitSet setCols = new BitSet(2);
        setCols.set(0);
        setCols.set(1);

        BitSet scalarFields = new BitSet(2);
        BitSet floatFields = new BitSet(2);
        BitSet doubleFields = new BitSet(2);
        BitIndex index = BitIndexing.getBestIndex(setCols,scalarFields,floatFields,doubleFields);
        EntryEncoder encoder = EntryEncoder.create(KryoPool.defaultPool(),index);

        encoder.getEntryEncoder().encodeNext(testType1).encodeNext(testType2);
        byte[] data = encoder.encode();

        EntryDecoder decoder = new EntryDecoder(KryoPool.defaultPool());
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
        MultiFieldDecoder fieldDecoder = MultiFieldDecoder.wrap(retBytes, i + 1, retBytes.length - (i + 1), KryoPool.defaultPool());

        String decodedField = fieldDecoder.decodeNextString();
        Assert.assertEquals("Incorrect string returned!",testType1,decodedField);
        BigDecimal t = fieldDecoder.decodeNextBigDecimal();
        Assert.assertTrue("Incorrect Big decimal returned! Expected "+ testType2+", but got "+ t,testType2.compareTo(t)==0);
        Assert.assertTrue("more than two fields available in field decoder!",fieldDecoder.nextIsNull());
    }

    @Test
    public void testReturnsAllColumnsForEmptyPredicate() throws Exception {
        EntryPredicateFilter predicateFilter = new EntryPredicateFilter(new BitSet(),
                Collections.<Predicate>emptyList(),true);

        BitSet setCols = new BitSet(2);
        setCols.set(0);
        setCols.set(1);

        BitSet scalarFields = new BitSet(2);
        BitSet floatFields = new BitSet(2);
        BitSet doubleFields = new BitSet(2);
        BitIndex index = BitIndexing.getBestIndex(setCols,scalarFields,floatFields,doubleFields);
        EntryEncoder encoder = EntryEncoder.create(KryoPool.defaultPool(),index);

        String testType1 = "test";
        BigDecimal testType2 = new BigDecimal("2.345");
        encoder.getEntryEncoder().encodeNext(testType1).encodeNext(testType2);
        byte[] data = encoder.encode();

        EntryDecoder decoder = new EntryDecoder(KryoPool.defaultPool());
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
        MultiFieldDecoder fieldDecoder = MultiFieldDecoder.wrap(retBytes, i + 1, retBytes.length - (i + 1), KryoPool.defaultPool());

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

        EntryPredicateFilter predicateFilter = new EntryPredicateFilter(fieldsToReturn,
                Collections.<Predicate>emptyList(),true);

        BitSet setCols = new BitSet(2);
        setCols.set(0);
        setCols.set(1);

        BitSet scalarFields = new BitSet(2);
        BitSet floatFields = new BitSet(2);
        BitSet doubleFields = new BitSet(2);
        BitIndex index = BitIndexing.getBestIndex(setCols,scalarFields,floatFields,doubleFields);
        EntryEncoder encoder = EntryEncoder.create(KryoPool.defaultPool(),index);

        String testType1 = "test";
        BigDecimal testType2 = new BigDecimal("2.345");
        encoder.getEntryEncoder().encodeNext(testType1).encodeNext(testType2);
        byte[] data = encoder.encode();

        EntryDecoder decoder = new EntryDecoder(KryoPool.defaultPool());
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
        MultiFieldDecoder fieldDecoder = MultiFieldDecoder.wrap(retBytes, i + 1, retBytes.length - (i + 1), KryoPool.defaultPool());

        String decodedField = fieldDecoder.decodeNextString();
        Assert.assertEquals("Incorrect string returned!",testType1,decodedField);
        Assert.assertTrue("more than one field available in field decoder!",fieldDecoder.nextIsNull());
    }
}
