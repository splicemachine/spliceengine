package com.splicemachine.storage;

import com.google.common.collect.Lists;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.BitSet;
import java.util.List;

/**
 * @author Scott Fines
 * Created on: 7/9/13
 */
public class OrPredicate implements Predicate {
    private static final long serialVersionUID = 1l;
    private List<Predicate> ors;

    /**
     * Once matched, we should return true until reset
     */
    private boolean matched;
    private int visitedCount;

    @Deprecated
    public OrPredicate() {
    }

    public OrPredicate(List<Predicate> ors) {
        this.ors = ors;
    }

    @Override
    public boolean applies(int column) {
        for(Predicate predicate:ors){
            if(predicate.applies(column)) return true;
        }
        return false;
    }

    @Override
    public boolean match(int column,byte[] data, int offset, int length) {
        if(matched) return true;
        if(visitedCount>=ors.size()) return false; //we've visited all of our fields, and none matched

        for(Predicate predicate:ors){
            if(!predicate.applies(column))
                continue;

            if(predicate.match(column,data,offset,length)){
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
        for(Predicate predicate:ors){
            if(predicate.checkAfter()) return true;
        }
        return false;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(ors.size());
        for(Predicate predicate:ors){
            out.writeObject(predicate);
        }
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        int size = in.readInt();
        ors = Lists.newArrayListWithCapacity(size);
        for(int i=0;i<size;i++){
            ors.add((Predicate)in.readObject());
        }
    }

    @Override
    public void setCheckedColumns(BitSet checkedColumns) {
        for(Predicate or:ors){
            or.setCheckedColumns(checkedColumns);
        }
    }

    @Override
    public void reset() {
        matched=false;
        visitedCount=0;
        //reset children
        for(Predicate predicate:ors){
            predicate.reset();
        }
    }
}
