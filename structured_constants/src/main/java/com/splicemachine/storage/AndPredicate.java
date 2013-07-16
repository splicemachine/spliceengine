package com.splicemachine.storage;

import com.google.common.collect.Lists;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.BitSet;
import java.util.List;

/**
 * @author Scott Fines
 *         Created on: 7/9/13
 */
public class AndPredicate implements Predicate{
    private static final long serialVersionUID = 1l;
    private List<Predicate> ands;

    @Deprecated
    public AndPredicate() { }

    public AndPredicate(List<Predicate> ands) {
        this.ands = ands;
    }

    @Override
    public boolean match(int column, byte[] data, int offset, int length) {
        for(Predicate predicate:ands){
            if(!predicate.match(column,data,offset,length))
                return false;
        }
        return true;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(ands.size());
        for(Predicate predicate: ands){
            out.writeObject(predicate);
        }
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        int size = in.readInt();
        ands = Lists.newArrayListWithCapacity(size);
        for(int i=0;i<size;i++){
            ands.add((Predicate)in.readObject());
        }
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
}
