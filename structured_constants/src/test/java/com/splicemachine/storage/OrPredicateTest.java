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
public class OrPredicateTest {

    @Test
    public void testMatchesOneColumnNull() throws Exception {
        byte[] encoded1 = Encoding.encode(10030);
        Predicate pred1 = new ValuePredicate(CompareFilter.CompareOp.LESS,0,encoded1,true);
        Predicate pred2 = new NullPredicate(false,false,0,false,false);

        OrPredicate orPred = new OrPredicate(Arrays.asList(pred1,pred2));

        Assert.assertTrue("Does not match null!",orPred.match(0,null,0,0));
        Assert.assertTrue("Does not match empty byte[]!",orPred.match(0,new byte[]{},0,0));
    }

    @Test
    public void testMatchesOneColumnNullAnotherWrongColumn() throws Exception {
        byte[] encoded1 = Encoding.encode(10030);
        Predicate pred1 = new ValuePredicate(CompareFilter.CompareOp.LESS,1,encoded1,true);
        Predicate pred2 = new NullPredicate(false,false,0,false,false);

        OrPredicate orPred = new OrPredicate(Arrays.asList(pred1,pred2));

        Assert.assertTrue("Does not match null!",orPred.match(0,null,0,0));
        orPred.reset();
        Assert.assertTrue("Does not match empty byte[]!",orPred.match(0,new byte[]{},0,0));
    }


    @Test
    public void testFailsIfNoPredicatesPassDifferentColumn() throws Exception {
        byte[] encoded1 = Encoding.encode(10030);
        Predicate pred1 = new ValuePredicate(CompareFilter.CompareOp.LESS,1,encoded1,true);
        Predicate pred2 = new NullPredicate(true,false,0,false,false);

        OrPredicate orPred = new OrPredicate(Arrays.asList(pred1,pred2));

        Assert.assertTrue("Does not match null!",orPred.match(0,null,0,0));

        byte[] testValue = Encoding.encode(11000);
        Assert.assertFalse("Erroneously matches field!",orPred.match(1,testValue,0,testValue.length));

        /*
         * We simulate here passing in an additional column to an OrPredicate that MIGHT match,
         * but that shouldn't, because the Ors are exhausted
         */
        byte[] testVal = Encoding.encode(8000);
        Assert.assertFalse("Erroneously matches!",orPred.match(2,testVal,0,testVal.length));
    }

    @Test
    public void testMatchesNoColumnsFails() throws Exception {
        byte[] encoded1 = Encoding.encode(10030);
        Predicate pred1 = new ValuePredicate(CompareFilter.CompareOp.LESS,0,encoded1,true);
        Predicate pred2 = new NullPredicate(false,false,0,false,false);

        OrPredicate orPred = new OrPredicate(Arrays.asList(pred1,pred2));

        byte[] testValue = Encoding.encode(11000);
        Assert.assertFalse("Erroneously matches value!", orPred.match(0, testValue, 0, testValue.length));
    }

    @Test
    public void testOnePredicateChecksAfterCausesOrToCheckAfter() throws Exception {
        byte[] encoded1 = Encoding.encode(10030);
        Predicate pred1 = new ValuePredicate(CompareFilter.CompareOp.LESS,0,encoded1,true);
        Predicate pred2 = new NullPredicate(false,false,0,false,false);

        OrPredicate orPred = new OrPredicate(Arrays.asList(pred1,pred2));

        Assert.assertTrue("Check after incorrect!",orPred.checkAfter());
    }

    @Test
    public void testNoPredicatesCheckAfterCausesOrNotToCheckAfter() throws Exception {
        byte[] encoded1 = Encoding.encode(10030);
        Predicate pred1 = new ValuePredicate(CompareFilter.CompareOp.LESS_OR_EQUAL,0,encoded1,false);
        Predicate pred2 = new ValuePredicate(CompareFilter.CompareOp.GREATER_OR_EQUAL,0,encoded1,false);

        OrPredicate orPred = new OrPredicate(Arrays.asList(pred1,pred2));

        Assert.assertFalse("Check after incorrect!",orPred.checkAfter());
    }

    @Test
    public void testAppliesIfOnePredicateApplies() throws Exception {
        byte[] encoded1 = Encoding.encode(10030);
        Predicate pred1 = new ValuePredicate(CompareFilter.CompareOp.LESS,1,encoded1,true);
        Predicate pred2 = new NullPredicate(false,false,0,false,false);

        OrPredicate orPred = new OrPredicate(Arrays.asList(pred1,pred2));

        Assert.assertTrue("Application incorrect!",orPred.applies(0));
    }

    @Test
    public void testDoesNotApplyIfNoPredicatesApply() throws Exception {
        byte[] encoded1 = Encoding.encode(10030);
        Predicate pred1 = new ValuePredicate(CompareFilter.CompareOp.LESS,1,encoded1,true);
        Predicate pred2 = new NullPredicate(false,false,2,false,false);

        OrPredicate orPred = new OrPredicate(Arrays.asList(pred1,pred2));

        Assert.assertFalse("Application incorrect!",orPred.applies(0));
    }
}
