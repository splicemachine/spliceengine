package com.splicemachine.storage;

import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/**
 * @author Scott Fines
 *         Created on: 8/12/13
 */
public class PredicatesTest {

    @Test
    public void testCanEncodeDecodeNullPredicateList() throws Exception {
        List<Predicate> nullPreds = Arrays.asList(
                (Predicate) new NullPredicate(true,false,0,false,false),
                (Predicate) new NullPredicate(true,false,0,false,false)
        );

        List<Predicate> preds = Arrays.asList((Predicate)new AndPredicate(nullPreds));

        byte[] data  = Predicates.toBytes(preds);

        Pair<List<Predicate>,Integer> decodedPred = Predicates.allFromBytes(data,0);

    }

    @Test
    public void testCanEncodeDecodePredicatesList() throws Exception {
        List<Predicate> firstPreds = Arrays.asList(
                (Predicate)new ValuePredicate(CompareFilter.CompareOp.EQUAL,1,new byte[]{0x02,0x01},false)
        );
        List<Predicate> secondPreds = Arrays.asList(
                (Predicate)new ValuePredicate(CompareFilter.CompareOp.GREATER_OR_EQUAL,0,new byte[]{0x01,0x03},true),
                (Predicate)new ValuePredicate(CompareFilter.CompareOp.GREATER_OR_EQUAL,1,new byte[]{0x01,0x04},false)
        );
        List<Predicate> predicates = Arrays.asList(
                (Predicate)new AndPredicate(firstPreds),
                new AndPredicate(secondPreds)
        );

        byte[] data = Predicates.toBytes(predicates);

        Pair<List<Predicate>,Integer> decoded = Predicates.allFromBytes(data,0);

        System.out.println(decoded);
    }
}
