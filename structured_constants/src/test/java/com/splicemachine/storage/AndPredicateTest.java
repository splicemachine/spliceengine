package com.splicemachine.storage;

import com.splicemachine.encoding.Encoding;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

/**
 * @author Scott Fines
 * Created on: 10/3/13
 */
public class AndPredicateTest {

    @Test
    public void testAndPredicateMatchesTwoColumns() throws Exception {
        byte[] compareValue = Encoding.encode(10);
        byte[] compareValue2 = Encoding.encode(0);
        ValuePredicate pred1 = new ValuePredicate(CompareFilter.CompareOp.LESS_OR_EQUAL,0,compareValue,true);
        ValuePredicate pred2 = new ValuePredicate(CompareFilter.CompareOp.GREATER,0,compareValue2,true);

        AndPredicate andPredicate = new AndPredicate(Arrays.<Predicate>asList(pred1,pred2));

        byte[] testValue = Encoding.encode(5);
        Assert.assertTrue("Does not match value!",andPredicate.match(0,testValue,0,testValue.length));
    }

    @Test
    public void testOnePredicateFailsFailsAndPredicate() throws Exception {
            byte[] compareValue = Encoding.encode(10);
            byte[] compareValue2 = Encoding.encode(0);
            ValuePredicate pred1 = new ValuePredicate(CompareFilter.CompareOp.GREATER_OR_EQUAL,0,compareValue,true);
            ValuePredicate pred2 = new ValuePredicate(CompareFilter.CompareOp.GREATER,0,compareValue2,true);

            AndPredicate andPredicate = new AndPredicate(Arrays.<Predicate>asList(pred1,pred2));

            byte[] testValue = Encoding.encode(5);
            Assert.assertFalse("Erroneously matches value!", andPredicate.match(0, testValue, 0, testValue.length));
    }

    @Test
    public void testNoPredicateApplyCausesNoApplies() throws Exception {
        byte[] compareValue = Encoding.encode(10);
        byte[] compareValue2 = Encoding.encode(0);
        ValuePredicate pred1 = new ValuePredicate(CompareFilter.CompareOp.LESS_OR_EQUAL,0,compareValue,true);
        ValuePredicate pred2 = new ValuePredicate(CompareFilter.CompareOp.GREATER,1,compareValue2,true);

        AndPredicate andPredicate = new AndPredicate(Arrays.<Predicate>asList(pred1,pred2));

        byte[] testValue = Encoding.encode(5);
        Assert.assertFalse("Erroneously applies!",andPredicate.applies(2));
    }

    @Test
    public void testOnlyOnePredicateAppliesWhenDifferentColumnsApplied() throws Exception {
        byte[] compareValue = Encoding.encode(10);
        byte[] compareValue2 = Encoding.encode(0);
        ValuePredicate pred1 = new ValuePredicate(CompareFilter.CompareOp.LESS_OR_EQUAL,0,compareValue,true);
        ValuePredicate pred2 = new ValuePredicate(CompareFilter.CompareOp.GREATER,1,compareValue2,true);

        AndPredicate andPredicate = new AndPredicate(Arrays.<Predicate>asList(pred1,pred2));

        byte[] testValue = Encoding.encode(5);
        Assert.assertTrue("does not match value!", andPredicate.match(0, testValue, 0, testValue.length));
    }

    @Test
    public void testDoesNotApplyWhenNoPredicatesApply() throws Exception {
        byte[] compareValue = Encoding.encode(10);
        byte[] compareValue2 = Encoding.encode(0);
        ValuePredicate pred1 = new ValuePredicate(CompareFilter.CompareOp.GREATER_OR_EQUAL,0,compareValue,true);
        ValuePredicate pred2 = new ValuePredicate(CompareFilter.CompareOp.GREATER,0,compareValue2,true);

        AndPredicate andPredicate = new AndPredicate(Arrays.<Predicate>asList(pred1,pred2));

        byte[] testValue = Encoding.encode(5);
        Assert.assertTrue("does not match value!", andPredicate.match(1, testValue, 0, testValue.length));
    }

    @Test
    public void testCheckAfterAppliesIfOnePredicateChecksAfter() throws Exception {
        byte[] compareValue = Encoding.encode(10);
        byte[] compareValue2 = Encoding.encode(0);
        ValuePredicate pred1 = new ValuePredicate(CompareFilter.CompareOp.LESS_OR_EQUAL,0,compareValue,true);
        ValuePredicate pred2 = new ValuePredicate(CompareFilter.CompareOp.GREATER,1,compareValue2,false);

        AndPredicate andPredicate = new AndPredicate(Arrays.<Predicate>asList(pred1,pred2));

        Assert.assertTrue(andPredicate.checkAfter());
    }

    @Test
    public void testDoesNotCheckAfterIfNoPredicatesCheckAfter() throws Exception {
        byte[] compareValue = Encoding.encode(10);
        byte[] compareValue2 = Encoding.encode(0);
        ValuePredicate pred1 = new ValuePredicate(CompareFilter.CompareOp.LESS_OR_EQUAL,0,compareValue,false);
        ValuePredicate pred2 = new ValuePredicate(CompareFilter.CompareOp.GREATER,1,compareValue2,false);

        AndPredicate andPredicate = new AndPredicate(Arrays.<Predicate>asList(pred1,pred2));

        Assert.assertFalse(andPredicate.checkAfter());
    }
}
