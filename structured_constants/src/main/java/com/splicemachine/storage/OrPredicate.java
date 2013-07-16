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

    @Deprecated
    public OrPredicate() {
    }

    public OrPredicate(List<Predicate> ors) {
        this.ors = ors;
    }

    @Override
    public boolean match(int column,byte[] data, int offset, int length) {
        for(Predicate predicate:ors){
            if(predicate.match(column,data,offset,length))
                return true;
        }
        return false; //nobody matches
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
}
