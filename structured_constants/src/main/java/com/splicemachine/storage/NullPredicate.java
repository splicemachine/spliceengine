package com.splicemachine.storage;

import com.splicemachine.constants.bytes.BytesUtil;
import org.apache.hadoop.hbase.util.Pair;
import java.util.BitSet;

/**
 * @author Scott Fines
 * Created on: 7/8/13
 */
public class NullPredicate implements Predicate{
    private static final long serialVersionUID = 4l;
    private boolean filterIfMissing; //when true, equivalent to filterIfMissing null
    private boolean isNullNumericalComparision;
    private boolean isDoubleColumn;
    private boolean isFloatColumn;
    private int column;

    public NullPredicate(boolean filterIfMissing, boolean isNullNumericalComparison,
                         int column,boolean isDoubleColumn,boolean isFloatColumn) {
        this.filterIfMissing = filterIfMissing;
        this.isNullNumericalComparision = isNullNumericalComparison;
        this.column = column;
        this.isFloatColumn = isFloatColumn;
        this.isDoubleColumn = isDoubleColumn;
    }

    @Override
    public boolean applies(int column) {
        return this.column==column;
    }

    @Override
    public boolean match(int column,byte[] data, int offset, int length) {
        if(this.column!=column) return true; //not the right column, don't worry about it
        if(isNullNumericalComparision){
            return false; //a numerical comparison with null will never match any columns
        }
        if(filterIfMissing){
            if(isDoubleColumn){
                return data!=null && length==8;
            }else if(isFloatColumn)
                return data!=null && length==4;
            else
            //make sure data is NOT null---data cannot be null, and length >0
                return data!=null && length>0;
        }else{
            if(isDoubleColumn){
                return data==null || length!=8;
            }else if(isFloatColumn)
                return data==null || length!=4;
            else
                //make sure data is null--either data itself is null, or length==0
                return data==null|| length==0;
        }
    }

    @Override
    public boolean checkAfter() {
        /*
         * We have to also check null predicates AFTER the entire row is
         * dealt with. Otherwise, columns that are actually null may never be
         * checked (since they won't necessarily show up in any of the incremental
         * checks).
         */
        return true;
    }

    @Override
    public void setCheckedColumns(BitSet checkedColumns) {
        checkedColumns.set(column);
    }

    @Override
    public void reset() {
        //no-op
    }

    @Override
    public byte[] toBytes() {
        /*
         * Format is as follows:
         *
         * 1-byte filterIfMissing
         * 1-byte isNullNumericalComparison
         * 1-byte isDoubleColumn
         * 1-byte isFloatColumn
         * 4-byte column number
         */
        byte[] data = new byte[9];
        data[0] = PredicateType.NULL.byteValue();
        data[1] = filterIfMissing? (byte)0x01: 0x00;
        data[2] = isNullNumericalComparision? (byte)0x01: 0x00;
        data[3] = isDoubleColumn? (byte)0x01:0x00;
        data[4] = isFloatColumn? (byte)0x01:0x00;
        BytesUtil.intToBytes(column,data,5);
        return data;
    }

    public static Pair<NullPredicate,Integer> fromBytes(byte[] data, int offset){
        boolean filterIfMissing = data[offset]==0x01;
        boolean isNullNumericalComparison = data[offset+1]==0x01;
        boolean isDoubleColumn = data[offset+2]==0x01;
        boolean isFloatColumn = data[offset+3]==0x01;
        int column = BytesUtil.bytesToInt(data,offset+4);
        return Pair.newPair(new NullPredicate(filterIfMissing,isNullNumericalComparison,column,isDoubleColumn,isFloatColumn),9);
    }

}
