package com.splicemachine.storage;

import com.carrotsearch.hppc.BitSet;
import com.carrotsearch.hppc.ObjectArrayList;
import com.splicemachine.constants.bytes.BytesUtil;

import org.apache.hadoop.hbase.util.Pair;

import java.io.IOException;

/**
 * @author Scott Fines
 * Created on: 7/9/13
 */
public class OrPredicate implements Predicate {
    private ObjectArrayList<Predicate> ors;

    /**
     * Once matched, we should return true until reset
     */
    private boolean matched;
    private int visitedCount;

    public OrPredicate(ObjectArrayList<Predicate> ors) {
        this.ors = ors;
    }

    @Override
    public boolean applies(int column) {
    	Object[] buffer = ors.buffer;
    	int iBuffer = ors.size();
    	for (int i = 0; i < iBuffer; i++) {
    		Predicate predicate = (Predicate) buffer[i];
    		if(predicate.applies(column)) 
    			return true;
        }
        return false;
    }

    @Override
    public boolean match(int column,byte[] data, int offset, int length) {
        if(matched) return true;
        if(visitedCount>=ors.size()) return false; //we've visited all of our fields, and none matched

        
    	Object[] buffer = ors.buffer;
    	int iBuffer = ors.size();
    	for (int i = 0; i < iBuffer; i++) {
    		Predicate predicate = (Predicate) buffer[i];
            if(!predicate.applies(column))
                continue;

            if(predicate.match(column, data, offset, length)){
                matched=true;
                return true;
            }
            else
                visitedCount++;
        }
        return visitedCount<ors.size();
    }

    @Override
    public boolean checkAfter() {
    	Object[] buffer = ors.buffer;
    	int iBuffer = ors.size();
    	for (int i = 0; i < iBuffer; i++) {
    		Predicate predicate = (Predicate) buffer[i];
            if(predicate.checkAfter()) return true;
        }
        return false;
    }

    @Override
    public void setCheckedColumns(BitSet checkedColumns) {
    	Object[] buffer = ors.buffer;
    	int iBuffer = ors.size();
    	for (int i = 0; i < iBuffer; i++) {
    		Predicate predicate = (Predicate) buffer[i];
    		predicate.setCheckedColumns(checkedColumns);
        }
    }

    @Override
    public void reset() {
        matched=false;
        visitedCount=0;
        //reset children
    	Object[] buffer = ors.buffer;
    	int iBuffer = ors.size();
    	for (int i = 0; i < iBuffer; i++) {
    		Predicate predicate = (Predicate) buffer[i];
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
        byte[] listData = Predicates.toBytes(ors);
        byte[] data  = new byte[listData.length+1];
        data[0] = PredicateType.OR.byteValue();
        System.arraycopy(listData,0,data,1,listData.length);

        return data;
    }

    public static Pair<OrPredicate,Integer> fromBytes(byte[] data, int offset) throws IOException {
        int size = BytesUtil.bytesToInt(data,offset);
        Pair<ObjectArrayList<Predicate>,Integer> predicates = Predicates.fromBytes(data,offset+4,size);
        return Pair.newPair(new OrPredicate(predicates.getFirst()),predicates.getSecond()-offset+1);
    }
}
