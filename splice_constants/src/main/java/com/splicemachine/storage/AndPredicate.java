package com.splicemachine.storage;

import org.apache.hadoop.hbase.util.Pair;

import com.carrotsearch.hppc.BitSet;
import com.carrotsearch.hppc.ObjectArrayList;

import java.io.IOException;

/**
 * @author Scott Fines
 * Created on: 7/9/13
 */
public class AndPredicate implements Predicate{
    private ObjectArrayList<Predicate> ands;
    private int matchedCount = 0;
    private boolean failed = false;

    public static Predicate newAndPredicate(Predicate...preds) {
        if(preds.length==1)
            return preds[0];
        return new AndPredicate(ObjectArrayList.from(preds));
    }

    public static Predicate newAndPredicate(ObjectArrayList<Predicate> ands){
        if(ands.size()==1){
            return ands.get(0);
        }
        return new AndPredicate(ands);
    }

    public AndPredicate(ObjectArrayList<Predicate> ands) {
        this.ands = new ObjectArrayList<>(ands);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof AndPredicate)) return false;

        AndPredicate that = (AndPredicate) o;

        if (ands != null ? !ands.equals(that.ands) : that.ands != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return ands != null ? ands.hashCode() : 0;
    }

//    @Override
//    public boolean isFinished(){
//        return matchedCount==ands.size();
//    }
//
//    @Override
//    public boolean isFailed(){
//        return failed;
//    }

    @Override
    public boolean applies(int column) {
    	Object[] buffer = ands.buffer;
    	int iBuffer = ands.size();
    	for (int i = 0; i < iBuffer; i++) {
            if( ((Predicate)buffer[i]).applies(column)) 
            	return true;    		
    	}
        return false;
    }

    @Override
    public boolean match(int column, byte[] data, int offset, int length) {
        if(failed) return false; //once we've failed, keep failing until we reset
        if(ands != null){
        	Object[] buffer = ands.buffer;
        	int iBuffer = ands.size();
        	for (int i = 0; i < iBuffer; i++) {
        		Predicate predicate = (Predicate) buffer[i];
                if(!predicate.applies(column)) 
                	continue; //skip non-applicable columns
                if(!predicate.match(column, data, offset, length)){
                    failed = true;
                    return false;
                }
            }
        }
        matchedCount++;
        return true;
    }

    @Override
    public boolean checkAfter() {
    	Object[] buffer = ands.buffer;
    	int iBuffer = ands.size();
    	for (int i = 0; i < iBuffer; i++) {
    		Predicate and = (Predicate) buffer[i];
            if(and.checkAfter()) return true;
        }
        return false;
    }

    @Override
    public void setCheckedColumns(BitSet checkedColumns) {
    	Object[] buffer = ands.buffer;
    	int iBuffer = ands.size();
    	for (int i = 0; i < iBuffer; i++) {
    		Predicate predicate = (Predicate) buffer[i];
            predicate.setCheckedColumns(checkedColumns);
        }
    }

    @Override
    public void reset() {
        //reset children
        Object[] buffer = ands.buffer;
        int iBuffer = ands.size();
        for (int i = 0; i < iBuffer; i++) {
            Predicate predicate = (Predicate) buffer[i];
            predicate.reset();
        }
        failed =false;
        matchedCount = 0;
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
        Pair<ObjectArrayList<Predicate>,Integer> predicates = Predicates.allFromBytes(data,offset);
        return Pair.newPair(new AndPredicate(predicates.getFirst()),predicates.getSecond()-offset+1);
    }

    @Override
    public String toString() {
        return "AndPredicate{" +
                "ands=" + ands +
                ", failed=" + failed +
                '}';
    }
}
