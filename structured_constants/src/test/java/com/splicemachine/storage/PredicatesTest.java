package com.splicemachine.storage;

import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.Test;
import com.carrotsearch.hppc.ObjectArrayList;
/**
 * @author Scott Fines
 *         Created on: 8/12/13
 */
public class PredicatesTest {

    @Test
    public void testCanEncodeDecodeNullPredicateList() throws Exception {
        ObjectArrayList<Predicate> nullPreds = ObjectArrayList.from(
                (Predicate) new NullPredicate(true,false,0,false,false),
                (Predicate) new NullPredicate(true,false,0,false,false)
        );

        ObjectArrayList<Predicate> preds = ObjectArrayList.from((Predicate)new AndPredicate(nullPreds));

        byte[] data  = Predicates.toBytes(preds);

        Pair<ObjectArrayList<Predicate>,Integer> decodedPred = Predicates.allFromBytes(data,0);

    }

    @Test
    public void testCanEncodeDecodePredicatesList() throws Exception {
        ObjectArrayList<Predicate> firstPreds = ObjectArrayList.from(
                (Predicate)new ValuePredicate(CompareFilter.CompareOp.EQUAL,1,new byte[]{0x02,0x01},false)
        );
        ObjectArrayList<Predicate> secondPreds = ObjectArrayList.from(
                (Predicate)new ValuePredicate(CompareFilter.CompareOp.GREATER_OR_EQUAL,0,new byte[]{0x01,0x03},true),
                (Predicate)new ValuePredicate(CompareFilter.CompareOp.GREATER_OR_EQUAL,1,new byte[]{0x01,0x04},false)
        );
        ObjectArrayList<Predicate> predicates = ObjectArrayList.from(
                (Predicate)new AndPredicate(firstPreds),
                new AndPredicate(secondPreds)
        );

        byte[] data = Predicates.toBytes(predicates);

        Pair<ObjectArrayList<Predicate>,Integer> decoded = Predicates.allFromBytes(data,0);

        System.out.println(decoded);
    }
}
