package com.splicemachine.storage;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.hadoop.hbase.util.Pair;

import java.io.IOException;
import java.util.*;

/**
 * @author Scott Fines
 * Created on: 7/9/13
 */
public class AndPredicate implements Predicate{
    private List<Predicate> ands;


    public AndPredicate(List<Predicate> ands) {
        this.ands = Collections.unmodifiableList(new LinkedList<Predicate>(ands));
    }

    @Override
    public boolean applies(int column) {
        for(Predicate predicate:ands){
            if(predicate.applies(column)) return true;
        }
        return false;
    }

    @Override
    public boolean match(int column, byte[] data, int offset, int length) {
        if(ands != null){
            for(Predicate predicate : ands){
                if(!predicate.applies(column)) continue; //skip non-applicable columns

                if(!predicate.match(column, data, offset, length)){
                    return false;
                }
            }
        }
        return true;
    }

    @Override
    public boolean checkAfter() {
        for(Predicate and:ands){
            if(and.checkAfter()) return true;
        }
        return false;
    }

    @Override
    public void setCheckedColumns(BitSet checkedColumns) {
        for(Predicate predicate:ands){
            predicate.setCheckedColumns(checkedColumns);
        }
    }

    @Override
    public void reset() {
        //reset children
        for(Predicate predicate:ands){
            predicate.reset();
        }
    }

    @Override
    public byte[] toBytes() {
        /*
         * Format is
         *
         * 1-byte type (PredicateType.AND)
         * 4-byte length field
         * n-byte predicates
         */
        byte[] listData = Predicates.toBytes(ands);
        byte[] data  = new byte[listData.length+1];
        data[0] = PredicateType.AND.byteValue();
        System.arraycopy(listData,0,data,1,listData.length);
        return data;
    }

    public static Pair<AndPredicate,Integer> fromBytes(byte[] data, int offset) throws IOException {
        Pair<List<Predicate>,Integer> predicates = Predicates.allFromBytes(data,offset);
        return Pair.newPair(new AndPredicate(predicates.getFirst()),predicates.getSecond()-offset+1);
    }

}
