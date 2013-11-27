package com.splicemachine.storage;

import com.splicemachine.constants.bytes.BytesUtil;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import com.carrotsearch.hppc.BitSet;


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

    public ValuePredicate(CompareFilter.CompareOp compareOp, int column, byte[] compareValue,boolean removeNullEntries) {
        this.compareOp = compareOp;
        this.column = column;
        this.compareValue = compareValue;
        this.removeNullEntries = removeNullEntries;
    }

    @Override
    public boolean applies(int column) {
        return this.column==column;
    }

    @Override
    public boolean match(int column,byte[] data, int offset, int length){
        if(this.column!=column) return true; //no need to perform anything, because it isn't the correct column

        if(data==null||length==0){
            if(compareValue==null||compareValue.length<=0)
                return true; //null matches null
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
    public boolean checkAfter() {
        /*
         * Remove null entries is an implicit not-null check--we thus need to make sure that
         * we are checking nulls AFTER the row is completed as well as before.
         */
        return removeNullEntries;
    }

    @Override
    public void setCheckedColumns(BitSet checkedColumns) {
        checkedColumns.set(column);
    }

    @Override
    public void reset() { } //no-op

    @Override
    public byte[] toBytes() {
        /*
         * Format is as follows:
         *
         * 1-byte type header (PredicateType.VALUE)
         * 4-bytes column
         * 1-byte removeNullEntries
         * 4-byte compareOrdinal
         * 4-byte compareValueLength
         * n-bytes the comparison value
         *
         * which results in an array of n+10 bytes
         */
        byte[] data = new byte[compareValue.length+14];
        data[0] = PredicateType.VALUE.byteValue();
        BytesUtil.intToBytes(column, data, 1);

        data[5] = removeNullEntries? (byte)0x01: 0x00;
        BytesUtil.intToBytes(compareOp.ordinal(), data, 6);
        BytesUtil.intToBytes(compareValue.length, data, 10);
        System.arraycopy(compareValue,0,data,14,compareValue.length);

        return data;
    }

    public static Pair<ValuePredicate,Integer> fromBytes(byte[] data, int offset){
        //first bytes are the Column
        int column = BytesUtil.bytesToInt(data, offset);
        boolean removeNullEntries = data[offset+4] ==0x01;
        CompareFilter.CompareOp compareOp = getCompareOp(BytesUtil.bytesToInt(data, offset + 5));

        int compareValueSize = BytesUtil.bytesToInt(data, offset + 9);
        byte[] compareValue = new byte[compareValueSize];
        System.arraycopy(data,offset+13,compareValue,0,compareValue.length);
        return Pair.newPair(new ValuePredicate(compareOp,column,compareValue,removeNullEntries),compareValue.length+14);
    }

    private static CompareFilter.CompareOp getCompareOp(int compareOrdinal) {
        for(CompareFilter.CompareOp op: CompareFilter.CompareOp.values()){
            if(op.ordinal()==compareOrdinal){
                return op;
            }
        }
        throw new IllegalArgumentException("Unable to find Compare op for ordinal "+ compareOrdinal);
    }

}
