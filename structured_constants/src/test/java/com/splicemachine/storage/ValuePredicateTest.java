package com.splicemachine.storage;

import com.splicemachine.encoding.Encoding;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.encoding.MultiFieldEncoder;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author Scott Fines
 *         Created on: 7/9/13
 */
public class ValuePredicateTest {
    @Test
    public void testMatchIsCorrect() throws Exception {
        byte[] correctVal = Encoding.encode(1);

        ValuePredicate predicate = new ValuePredicate(CompareFilter.CompareOp.EQUAL,0,correctVal,true,false);

        byte[] testVal = Encoding.encode(1);

        Assert.assertTrue(predicate.match(0, testVal, 0, testVal.length));
    }

    @Test
    public void testMatchIsCorrectNotMatching() throws Exception {
        byte[] correctVal = Encoding.encode(2);

        ValuePredicate predicate = new ValuePredicate(CompareFilter.CompareOp.EQUAL,0,correctVal,true,false);

        byte[] testVal = Encoding.encode(1);

        Assert.assertFalse(predicate.match(0, testVal, 0, testVal.length));
    }

    @Test
    public void testMatchIsCorrectNotMatchingNull() throws Exception {
        byte[] correctVal = Encoding.encode(2);

        ValuePredicate predicate = new ValuePredicate(CompareFilter.CompareOp.EQUAL,0,correctVal,true,false);


        Assert.assertFalse(predicate.match(0, null, 0, 0));
    }

    @Test
    public void testMatchIsCorrectNotMatchingNullWithoutRemovingNulls() throws Exception {
        byte[] correctVal = Encoding.encode(2);

        ValuePredicate predicate = new ValuePredicate(CompareFilter.CompareOp.EQUAL,0,correctVal,false,false);


        Assert.assertFalse(predicate.match(0, null, 0, 0));
    }

    @Test
    public void testCanMatchNull() throws Exception {

        ValuePredicate predicate = new ValuePredicate(CompareFilter.CompareOp.EQUAL,0,null,true,false);

        Assert.assertTrue("does not match null!",predicate.match(0, null, 0, 0));
        Assert.assertTrue("does not match empty byte[]!",predicate.match(0, new byte[]{}, 0, 0));
    }

    @Test
    public void testMatchSlicesCorrectly() throws Exception {
        byte[] correctVal = Encoding.encode(2);

        ValuePredicate predicate = new ValuePredicate(CompareFilter.CompareOp.EQUAL,0,correctVal,true,false);

        MultiFieldEncoder encoder = MultiFieldEncoder.create(2);
        encoder.encodeNext(2);
        encoder.encodeNext(1);

        byte[] testVal = encoder.build();

        MultiFieldDecoder decoder = MultiFieldDecoder.wrap(testVal);
        int offset = decoder.offset();
        decoder.skip();
        int length = decoder.offset()-1;
        Assert.assertTrue(predicate.match(0,decoder.array(),offset,length));

        offset = decoder.offset();
        decoder.skip();
        length = decoder.offset()-1;

        Assert.assertFalse(predicate.match(0,decoder.array(),offset,length));

    }

    @Test
    public void testMatchIsCorrectNotEquals() throws Exception {
        byte[] correctVal = Encoding.encode(1);

        ValuePredicate predicate = new ValuePredicate(CompareFilter.CompareOp.NOT_EQUAL,0,correctVal,true,false);

        byte[] testVal = Encoding.encode(2);

        Assert.assertTrue(predicate.match(0, testVal, 0, testVal.length));

    }

    @Test
    public void testMatchIsCorrectNotMatchingNotEquals() throws Exception {
        byte[] correctVal = Encoding.encode(2);

        ValuePredicate predicate = new ValuePredicate(CompareFilter.CompareOp.NOT_EQUAL,0,correctVal,true,false);

        byte[] testVal = Encoding.encode(2);

        Assert.assertFalse(predicate.match(0, testVal, 0, testVal.length));
    }

    @Test
    public void testMatchSlicesCorrectlyNotEquals() throws Exception {
        byte[] correctVal = Encoding.encode(2);

        ValuePredicate predicate = new ValuePredicate(CompareFilter.CompareOp.NOT_EQUAL,0,correctVal,true,false);

        MultiFieldEncoder encoder = MultiFieldEncoder.create(2);
        encoder.encodeNext(2);
        encoder.encodeNext(1);

        byte[] testVal = encoder.build();

        MultiFieldDecoder decoder = MultiFieldDecoder.wrap(testVal);
        int offset = decoder.offset();
        decoder.skip();
        int length = decoder.offset()-1;
        Assert.assertFalse(predicate.match(0,decoder.array(),offset,length));

        offset = decoder.offset();
        decoder.skip();
        length = decoder.offset()-1;

        Assert.assertTrue(predicate.match(0,decoder.array(),offset,length));

    }

    @Test
    public void testMatchIsCorrectGreaterThan() throws Exception {
        byte[] correctVal = Encoding.encode(2);

        ValuePredicate predicate = new ValuePredicate(CompareFilter.CompareOp.GREATER,0,correctVal,true,false);

        byte[] testVal = Encoding.encode(3);
        Assert.assertTrue(predicate.match(0, testVal, 0, testVal.length));
    }

    @Test
    public void testMatchIsCorrectNotMatchingGreaterThan() throws Exception {
        byte[] correctVal = Encoding.encode(2);

        ValuePredicate predicate = new ValuePredicate(CompareFilter.CompareOp.GREATER,0,correctVal,true,false);

        byte[] testVal = Encoding.encode(1);
        Assert.assertFalse(predicate.match(0, testVal, 0, testVal.length));
    }

    @Test
    public void testMatchSlicesCorrectlyGreaterThan() throws Exception {
        byte[] correctVal = Encoding.encode(2);

        ValuePredicate predicate = new ValuePredicate(CompareFilter.CompareOp.GREATER,0,correctVal,true,false);

        MultiFieldEncoder encoder = MultiFieldEncoder.create(3);
        encoder.encodeNext(2);
        encoder.encodeNext(1);
        encoder.encodeNext(3);

        byte[] testVal = encoder.build();

        MultiFieldDecoder decoder = MultiFieldDecoder.wrap(testVal);
        int offset = decoder.offset();
        decoder.skip();
        int length = decoder.offset()-1;
        Assert.assertFalse(predicate.match(0,decoder.array(),offset,length));

        offset = decoder.offset();
        decoder.skip();
        length = decoder.offset()-1;
        Assert.assertFalse(predicate.match(0,decoder.array(),offset,length));

        offset = decoder.offset();
        decoder.skip();
        length = decoder.offset()-1;
        Assert.assertTrue(predicate.match(0,decoder.array(),offset,length));

    }


    @Test
    public void testMatchIsCorrectGreaterThanEqualThan() throws Exception {
        byte[] correctVal = Encoding.encode(2);

        ValuePredicate predicate = new ValuePredicate(CompareFilter.CompareOp.GREATER_OR_EQUAL,0,correctVal,true,false);

        byte[] testVal = Encoding.encode(3);
        Assert.assertTrue(predicate.match(0, testVal, 0, testVal.length));

        testVal = Encoding.encode(2);
        Assert.assertTrue(predicate.match(0, testVal, 0, testVal.length));
    }

    @Test
    public void testMatchIsCorrectNotMatchingGreaterThanEqualThan() throws Exception {
        byte[] correctVal = Encoding.encode(2);

        ValuePredicate predicate = new ValuePredicate(CompareFilter.CompareOp.GREATER_OR_EQUAL,0,correctVal,true,false);

        byte[] testVal = Encoding.encode(1);
        Assert.assertFalse(predicate.match(0, testVal, 0, testVal.length));
    }

    @Test
    public void testMatchSlicesCorrectlyGreaterThanEqualThan() throws Exception {
        byte[] correctVal = Encoding.encode(2);

        ValuePredicate predicate = new ValuePredicate(CompareFilter.CompareOp.GREATER_OR_EQUAL,0,correctVal,true,false);

        MultiFieldEncoder encoder = MultiFieldEncoder.create(3);
        encoder.encodeNext(2);
        encoder.encodeNext(1);
        encoder.encodeNext(3);

        byte[] testVal = encoder.build();

        MultiFieldDecoder decoder = MultiFieldDecoder.wrap(testVal);
        int offset = decoder.offset();
        decoder.skip();
        int length = decoder.offset()-1;
        Assert.assertTrue(predicate.match(0,decoder.array(),offset,length));

        offset = decoder.offset();
        decoder.skip();
        length = decoder.offset()-1;
        Assert.assertFalse(predicate.match(0,decoder.array(),offset,length));

        offset = decoder.offset();
        decoder.skip();
        length = decoder.offset()-1;
        Assert.assertTrue(predicate.match(0,decoder.array(),offset,length));

    }

    @Test
    public void testMatchIsCorrectLessThanEqualThan() throws Exception {
        byte[] correctVal = Encoding.encode(2);

        ValuePredicate predicate = new ValuePredicate(CompareFilter.CompareOp.LESS_OR_EQUAL,0,correctVal,true,false);

        byte[] testVal = Encoding.encode(1);
        Assert.assertTrue(predicate.match(0, testVal, 0, testVal.length));

        testVal = Encoding.encode(2);
        Assert.assertTrue(predicate.match(0, testVal, 0, testVal.length));
    }

    @Test
    public void testMatchIsCorrectNotMatchingLessThanEqualThan() throws Exception {
        byte[] correctVal = Encoding.encode(2);

        ValuePredicate predicate = new ValuePredicate(CompareFilter.CompareOp.LESS_OR_EQUAL,0,correctVal,true,false);

        byte[] testVal = Encoding.encode(3);
        Assert.assertFalse(predicate.match(0, testVal, 0, testVal.length));
    }

    @Test
    public void testMatchSlicesCorrectlyLessThanEqualThan() throws Exception {
        byte[] correctVal = Encoding.encode(2);

        ValuePredicate predicate = new ValuePredicate(CompareFilter.CompareOp.LESS_OR_EQUAL,0,correctVal,true,false);

        MultiFieldEncoder encoder = MultiFieldEncoder.create(3);
        encoder.encodeNext(2);
        encoder.encodeNext(1);
        encoder.encodeNext(3);

        byte[] testVal = encoder.build();

        MultiFieldDecoder decoder = MultiFieldDecoder.wrap(testVal);
        int offset = decoder.offset();
        decoder.skip();
        int length = decoder.offset()-1;
        Assert.assertTrue(predicate.match(0,decoder.array(),offset,length));

        offset = decoder.offset();
        decoder.skip();
        length = decoder.offset()-1;
        Assert.assertTrue(predicate.match(0,decoder.array(),offset,length));

        offset = decoder.offset();
        decoder.skip();
        length = decoder.offset()-1;
        Assert.assertFalse(predicate.match(0,decoder.array(),offset,length));

    }

    @Test
    public void testMatchIsCorrectLessThan() throws Exception {
        byte[] correctVal = Encoding.encode(2);

        ValuePredicate predicate = new ValuePredicate(CompareFilter.CompareOp.LESS,0,correctVal,true,false);

        byte[] testVal = Encoding.encode(1);

        Assert.assertTrue(predicate.match(0, testVal, 0, testVal.length));

    }

    @Test
    public void testMatchIsCorrectNotMatchingLessThan() throws Exception {
        byte[] correctVal = Encoding.encode(2);

        ValuePredicate predicate = new ValuePredicate(CompareFilter.CompareOp.LESS,0,correctVal,true,false);

        byte[] testVal = Encoding.encode(2);

        Assert.assertFalse(predicate.match(0, testVal, 0, testVal.length));
    }

    @Test
    public void testMatchSlicesCorrectlyLessThan() throws Exception {
        byte[] correctVal = Encoding.encode(2);

        ValuePredicate predicate = new ValuePredicate(CompareFilter.CompareOp.LESS,0,correctVal,true,false);

        MultiFieldEncoder encoder = MultiFieldEncoder.create(2);
        encoder.encodeNext(2);
        encoder.encodeNext(1);

        byte[] testVal = encoder.build();

        MultiFieldDecoder decoder = MultiFieldDecoder.wrap(testVal);
        int offset = decoder.offset();
        decoder.skip();
        int length = decoder.offset()-1;
        Assert.assertFalse(predicate.match(0,decoder.array(),offset,length));

        offset = decoder.offset();
        decoder.skip();
        length = decoder.offset()-1;

        Assert.assertTrue(predicate.match(0,decoder.array(),offset,length));

    }
}
