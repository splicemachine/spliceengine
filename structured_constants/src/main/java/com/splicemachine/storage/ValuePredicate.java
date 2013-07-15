package com.splicemachine.storage;

import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * @author Scott Fines
 * Created on: 7/8/13
 */
public class ValuePredicate implements Predicate {
    private static final long serialVersionUID=2l;
    private CompareFilter.CompareOp compareOp;
    private int column;
    private byte[] compareValue;

    private boolean removeNullEntries;

    /**
     * Used for serialization, DO NOT USE
     */
    @Deprecated
    public ValuePredicate() {}

    public ValuePredicate(CompareFilter.CompareOp compareOp, int column, byte[] compareValue,boolean removeNullEntries) {
        this.compareOp = compareOp;
        this.column = column;
        this.compareValue = compareValue;
        this.removeNullEntries = removeNullEntries;
    }

    @Override
    public boolean match(int column,byte[] data, int offset, int length){
        if(this.column!=column) return true; //no need to perform anything, because it isn't the correct column

        if(data==null||length==0){
            /*
             * The value passed in was null. Some comparisons can still be done, but numerical ones (for example)
             * cannot be--they are implicitly non-null comparisons. Thus, if a Predicate is a "nonNull predicate",
             * then removeNullEntries should be true, and this block with filter out those rows. Otherwise, it
             * will still attempt to perform those comparisons
             */
            if(removeNullEntries) return false;

            if(data==null){
                data = new byte[]{}; //for the purposes of comparisons, make sure data is not null
                length=0;
            }
        }
        int compare = Bytes.compareTo(compareValue,0,compareValue.length,data,offset,length);
        switch (compareOp) {
            case LESS:
                return compare>0;
            case LESS_OR_EQUAL:
                return compare >=0;
            case EQUAL:
                return compare==0;
            case NOT_EQUAL:
                return compare!=0;
            case GREATER_OR_EQUAL:
                return compare<=0;
            case GREATER:
                return compare<0;
            default:
                return true; //should never happen
        }
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(column);
        out.writeBoolean(removeNullEntries);
        out.writeInt(compareOp.ordinal());
        out.writeInt(compareValue.length);
        out.write(compareValue);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        column = in.readInt();
        removeNullEntries = in.readBoolean();
        int compareOrdinal = in.readInt();
        for(CompareFilter.CompareOp op: CompareFilter.CompareOp.values()){
            if(op.ordinal()==compareOrdinal){
                compareOp = op;
                break;
            }
        }
        compareValue = new byte[in.readInt()];
        in.readFully(compareValue);
    }
}
