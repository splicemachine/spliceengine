package com.splicemachine.storage;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * @author Scott Fines
 * Created on: 7/8/13
 */
public class NullPredicate implements Predicate{
    private static final long serialVersionUID = 1l;
    private boolean equals; //when true, equivalent to equals null
    private int column;

    /**
     * Used for Serialization, DO NOT USE
     */
    @Deprecated
    public NullPredicate() { }

    public NullPredicate(boolean equals, int column) {
        this.equals = equals;
        this.column = column;
    }

    @Override
    public int getColumn() {
        return column;
    }

    @Override
    public boolean match(byte[] data, int offset, int length) {
        if(equals){
            //make sure data is null--either data itself is null, or length==0
            return data == null || length == 0;
        }else{
            //make sure data is NOT null---data cannot be null, and length >0
            return data!=null && length>0;
        }
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeBoolean(equals);
        out.writeInt(column);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        equals = in.readBoolean();
        column = in.readInt();
    }
}
