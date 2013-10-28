package com.splicemachine.storage;

import com.google.common.collect.Lists;
import com.splicemachine.encoding.Encoding;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.storage.index.BitIndex;
import com.splicemachine.storage.index.BitIndexing;
import com.splicemachine.utils.kryo.KryoPool;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;

/**
 * @author Scott Fines
 * Created on: 10/3/13
 */
public class EntryPredicateFilterTest {

    @Test
    public void testPredicateWorksOnMultiKeyAndPredicate() throws Exception {
        /*
         * Regression for observed error in TPCC data set
         */
        String binary = "\\xCE\\xD8\\xF5\\xFA\\xA3\\xDF\\x88\\x00\\xC4\\xE2\\x00\\x83\\x00\\xC4\\x83\\x00\\xDFY\\x98\\x00IE\\x00CPVKDCTRTK\\x00|ttkqytfcejlimg\\x00\\xE4`\\x00a \\x00\\xC0$\\x00\\x00\\x00\\x00\\x00\\x01\\x00\\x81\\x00\\x80\\x00sprsckzju\\x00fsn|dlzmq{hhvfisfcr\\x00mysvwrw|jnozwihhr{\\x00YP\\x00:72:33333\\x00327:7867;75785;:\\x00\\xEDA\\xBB\\xA2\\x87\\xC8\\x00QG\\x00kvrmxikqjixsggeo{xhhgshhuptzlljsgnnzotyppxmvzvmfld{xqotdikqxrk{trglyolwdipxsqhrtspzpxey{y|xtkiwi|vvfj{kusfgi|ikurrittyqnv|xtyjqe{mdxhswzykydsvlgiqsgp{mjgrivtrkdpny{xhdxlvpr|kyzhodnjm{qdqrsqp|l|pqjfeiktf|uhelf{dvqd{j{wofqlvcllhdf|ypqh|kgjodcfcqduqhvqyudo{isxeneqdykowgqyeupud{vm|sylyf|{lg|{feuqcvnvxu|y|gffjhulnzsd||{dqxuj|p";

        byte[] bytes = Bytes.toBytesBinary(binary);

        Predicate p1 = new ValuePredicate(CompareFilter.CompareOp.EQUAL,0,Encoding.encode(1250),true);
        Predicate p2 = new ValuePredicate(CompareFilter.CompareOp.EQUAL,1,Encoding.encode(3),true);
        Predicate p3 = new ValuePredicate(CompareFilter.CompareOp.EQUAL,2,Encoding.encode(1155),true);

        Predicate and = new AndPredicate(Lists.newArrayList(p1,p2,p3));

        BitSet fieldsToReturn = new BitSet(1);
        fieldsToReturn.set(0,3);

        EntryPredicateFilter epf = new EntryPredicateFilter(fieldsToReturn,Collections.<Predicate>singletonList(and),true);

        EntryAccumulator accumulator = epf.newAccumulator();

        EntryDecoder decoder = new EntryDecoder(KryoPool.defaultPool());
        decoder.set(bytes);

        boolean match = epf.match(decoder, accumulator);
        Assert.assertTrue("Doesn't match!",match);

        byte[] accumulatedBytes = accumulator.finish();

        //make sure the decoder works correctly
        decoder.set(accumulatedBytes);

        MultiFieldDecoder fieldDecoder = decoder.getEntryDecoder();
        Assert.assertFalse("Field is incorrectly null!",fieldDecoder.nextIsNull());
        Assert.assertEquals("Incorrect field value!",1250,fieldDecoder.decodeNextInt());
        Assert.assertFalse("Field is incorrectly null!",fieldDecoder.nextIsNull());
        Assert.assertEquals("Incorrect field value!",3,fieldDecoder.decodeNextInt());
        Assert.assertFalse("Field is incorrectly null!",fieldDecoder.nextIsNull());
        Assert.assertEquals("Incorrect field value!",1155,fieldDecoder.decodeNextInt());
    }

    @Test
    public void testOrPredicateAppliesWithImplicitlyMissingField() throws Exception {
        BitSet setCols = new BitSet(3);
        setCols.set(0);
        setCols.set(2);

        BitSet scalarFields = new BitSet(3);
        scalarFields.set(0,3);
        scalarFields.and(setCols);

        BitIndex index = BitIndexing.getBestIndex(setCols,scalarFields,new BitSet(),new BitSet());
        EntryEncoder encoder = EntryEncoder.create(KryoPool.defaultPool(), index);

        short testField1 = 10030;
        long testField3 = 91968292l;

        encoder.getEntryEncoder().encodeNext(testField1).encodeNext(testField3);

        byte[] bytes = encoder.encode();

        Predicate pred1 = new ValuePredicate(CompareFilter.CompareOp.LESS,0,Encoding.encode(testField1),true);
        Predicate pred2 = new NullPredicate(false,false,1,false,false);

        Predicate finalPred = new AndPredicate(Arrays.<Predicate>asList(new OrPredicate(Arrays.asList(pred1,pred2))));

        BitSet retCols = new BitSet(2);
        retCols.set(0,2);
        EntryPredicateFilter epf = new EntryPredicateFilter(retCols,Arrays.asList(finalPred));

        EntryDecoder decoder = new EntryDecoder(KryoPool.defaultPool());
        decoder.set(bytes);

        EntryAccumulator accumulator = new SparseEntryAccumulator(epf,retCols);

        Assert.assertTrue("Incorrectly does not match row!",epf.match(decoder,accumulator));

        byte[] finished = accumulator.finish();
        Assert.assertNotNull("No row returned from finish!",finished);
    }

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
