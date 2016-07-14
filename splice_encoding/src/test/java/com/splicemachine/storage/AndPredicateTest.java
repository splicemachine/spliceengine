/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.splicemachine.storage;

import com.carrotsearch.hppc.ObjectArrayList;
import com.splicemachine.encoding.Encoding;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author Scott Fines
 * Created on: 10/3/13
 */
public class AndPredicateTest {

    @Test
    public void testAndPredicateMatchesTwoColumns() throws Exception {
        byte[] compareValue = Encoding.encode(10);
        byte[] compareValue2 = Encoding.encode(0);
        Predicate pred1 = new ValuePredicate(CompareOp.LESS_OR_EQUAL,0,compareValue,true,false);
        Predicate pred2 = new ValuePredicate(CompareOp.GREATER,0,compareValue2,true,false);
        AndPredicate andPredicate = new AndPredicate(ObjectArrayList.from(pred1,pred2));
        byte[] testValue = Encoding.encode(5);
        Assert.assertTrue("Does not match value!",andPredicate.match(0,testValue,0,testValue.length));
    }

    @Test
    public void testOnePredicateFailsFailsAndPredicate() throws Exception {
            byte[] compareValue = Encoding.encode(10);
            byte[] compareValue2 = Encoding.encode(0);
            Predicate pred1 = new ValuePredicate(CompareOp.GREATER_OR_EQUAL,0,compareValue,true,false);
            Predicate pred2 = new ValuePredicate(CompareOp.GREATER,0,compareValue2,true,false);
            AndPredicate andPredicate = new AndPredicate(ObjectArrayList.from(pred1,pred2));
            byte[] testValue = Encoding.encode(5);
            Assert.assertFalse("Erroneously matches value!", andPredicate.match(0, testValue, 0, testValue.length));
    }

    @Test
    public void testNoPredicateApplyCausesNoApplies() throws Exception {
        byte[] compareValue = Encoding.encode(10);
        byte[] compareValue2 = Encoding.encode(0);
        Predicate pred1 = new ValuePredicate(CompareOp.LESS_OR_EQUAL,0,compareValue,true,false);
        Predicate pred2 = new ValuePredicate(CompareOp.GREATER,1,compareValue2,true,false);
        AndPredicate andPredicate = new AndPredicate(ObjectArrayList.from(pred1,pred2));
        byte[] testValue = Encoding.encode(5);
        Assert.assertFalse("Erroneously applies!",andPredicate.applies(2));
    }

    @Test
    public void testOnlyOnePredicateAppliesWhenDifferentColumnsApplied() throws Exception {
        byte[] compareValue = Encoding.encode(10);
        byte[] compareValue2 = Encoding.encode(0);
        Predicate pred1 = new ValuePredicate(CompareOp.LESS_OR_EQUAL,0,compareValue,true,false);
        Predicate pred2 = new ValuePredicate(CompareOp.GREATER,1,compareValue2,true,false);
        AndPredicate andPredicate = new AndPredicate(ObjectArrayList.from(pred1,pred2));
        byte[] testValue = Encoding.encode(5);
        Assert.assertTrue("does not match value!", andPredicate.match(0, testValue, 0, testValue.length));
    }

    @Test
    public void testDoesNotApplyWhenNoPredicatesApply() throws Exception {
        byte[] compareValue = Encoding.encode(10);
        byte[] compareValue2 = Encoding.encode(0);
        Predicate pred1 = new ValuePredicate(CompareOp.GREATER_OR_EQUAL,0,compareValue,true,false);
        Predicate pred2 = new ValuePredicate(CompareOp.GREATER,0,compareValue2,true,false);
        AndPredicate andPredicate = new AndPredicate(ObjectArrayList.from(pred1,pred2));
        byte[] testValue = Encoding.encode(5);
        Assert.assertTrue("does not match value!", andPredicate.match(1, testValue, 0, testValue.length));
    }

    @Test
    public void testCheckAfterAppliesIfOnePredicateChecksAfter() throws Exception {
        byte[] compareValue = Encoding.encode(10);
        byte[] compareValue2 = Encoding.encode(0);
        Predicate pred1 = new ValuePredicate(CompareOp.LESS_OR_EQUAL,0,compareValue,true,false);
        Predicate pred2 = new ValuePredicate(CompareOp.GREATER,1,compareValue2,false,false);
        AndPredicate andPredicate = new AndPredicate(ObjectArrayList.from(pred1,pred2));
        Assert.assertTrue(andPredicate.checkAfter());
    }

    @Test
    public void testDoesNotCheckAfterIfNoPredicatesCheckAfter() throws Exception {
        byte[] compareValue = Encoding.encode(10);
        byte[] compareValue2 = Encoding.encode(0);
        Predicate pred1 = new ValuePredicate(CompareOp.LESS_OR_EQUAL,0,compareValue,false,false);
        Predicate pred2 = new ValuePredicate(CompareOp.GREATER,1,compareValue2,false,false);
        AndPredicate andPredicate = new AndPredicate(ObjectArrayList.from(pred1,pred2));
        Assert.assertFalse(andPredicate.checkAfter());
    }
}
