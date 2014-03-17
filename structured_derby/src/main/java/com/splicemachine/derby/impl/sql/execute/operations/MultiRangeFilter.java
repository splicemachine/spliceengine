package com.splicemachine.derby.impl.sql.execute.operations;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;
import java.util.List;

import com.google.common.collect.Lists;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.util.Bytes;

import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.utils.marshall.RowMarshaller;
import com.splicemachine.hbase.CellUtils;
import com.splicemachine.si.api.TransactionalFilter;

/**
 * Filter that filter's out data which doesn't fit within a group of
 * key ranges (start and stop keys). Uses SEEK_NEXT_USING_HINT to move
 * through the table efficiently.
 *
 * @author Scott Fines
 * Created on: 9/5/13
 */
public class MultiRangeFilter extends FilterBase implements TransactionalFilter {
    private static final long serialVersionUID = 1l;

    private KeyRange[] keyRanges;

    private boolean isDone;
    private int position = -1;
    private boolean filterRow;

    public MultiRangeFilter() { }

    public MultiRangeFilter(KeyRange[] keyRanges) {
        this.keyRanges = keyRanges;
    }

    @Override
    public void reset() {
        this.filterRow = true;
    }

    @Override
    public boolean filterRow() {
        return filterRow;
    }

    @Override
    public KeyValue getNextKeyHint(KeyValue currentKV) {
        if(position>=keyRanges.length){
            isDone=true;
            return null;
        }
        KeyRange nextRange = keyRanges[position];
        return KeyValue.createFirstOnRow(nextRange.startKey);
    }

    @Override
    public ReturnCode filterKeyValue(Cell currentKV) {
        if(!CellUtils.singleMatchingColumn(currentKV,SpliceConstants.DEFAULT_FAMILY_BYTES, RowMarshaller.PACKED_COLUMN_KEY))
            return ReturnCode.INCLUDE; //only consider data elements
        if(position>=keyRanges.length){
            isDone=true;
            return ReturnCode.NEXT_ROW;
        }

        byte[] data = CellUtils.getBuffer(currentKV);
        int rowKeyOffset = currentKV.getRowOffset();
        int rowKeyLength = currentKV.getRowLength();
        if(position<0){
            /*
             * We must initialize our state to the proper location. Because of region splits and so
             * forth, it is possible that that position is somewhere in the middle of our ranges. Thus,
             * we need to seek forward through our ranges until we find the first range that either
             *
             * A) contains this row
             * B) starts after this row
             *
             * This is succinctly contained by just checking the stop key for the KeyRange--if the end of the Range
             * is before the row, then the range can't satisfy A or B, but the first one that does will satisfy either
             * A or B, and so we can terminate
             */
            boolean shouldContinue;
            do{
                position++;
                KeyRange keyRange = keyRanges[position];
                shouldContinue = keyRange.before(data,rowKeyOffset,rowKeyLength);
            }while(position<keyRanges.length &&shouldContinue);
        }
        if(position>=keyRanges.length){
            /*
             * It's possible that there are no rows matching our Key Range. This means that we would have started, and eaten through
             * all the possible positions. Thus, we know that we are done and it's time to terminate the scan.
             */
            isDone=true;
            return ReturnCode.NEXT_ROW;
        }

        /*
         * There are three situations:
         *
         * A). The row is contained in the keyRange( keyRange.start <= row && keyRange.stop >row)
         * B). The row is before the KeyRange(keyRange.start >row)
         * C). The row is after the end of the range (keyRange.stop <=row)
         *
         * We want to be efficient, so we will check for C) first, then A if C) passes. If neither A nor C passes, it must
         * be B, so we know what to do there
         */
        KeyRange range = keyRanges[position];
        if(range.before(data,rowKeyOffset,rowKeyLength)){
            //passed the end of the range. Move to the next range and SEEK_NEXT
            position++;
            filterRow=true;
            return ReturnCode.SEEK_NEXT_USING_HINT;
        }
        if(range.after(data,rowKeyOffset,rowKeyLength)){
            //before the start of the next range. SEEK_NEXT to that range
            filterRow=true;
            return ReturnCode.SEEK_NEXT_USING_HINT;
        }

        //We know that rangeStart <= rowKey < rangeStop, so we can include this row
        filterRow=false;
        return ReturnCode.INCLUDE;
    }

    @Override
    public boolean filterAllRemaining() {
        return isDone;
    }

    // FIXME: old Writable interface - use protobuff
//    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(keyRanges.length);
        for(KeyRange range:keyRanges){
            range.writeTo(out);
        }
    }

    // FIXME: old Writable interface - use protobuff
//    @Override
    public void readFields(DataInput in) throws IOException {
        keyRanges = new KeyRange[in.readInt()];
        for(int i=0;i<keyRanges.length;i++){
            KeyRange range = new KeyRange();
            range.read(in);
            keyRanges[i] = range;
        }

        //ensure that KeyRanges is sorted
        Arrays.sort(keyRanges);
    }

    @Override
    public boolean isBeforeSI() {
        /*
         * We can safely execute this filter before an SI check, for the following reason.
         *
         * The SI check will consider a row, apply predicates to that row, and determine if the
         * transactional state allows the row to be seen.
         *
         * This filter, on the other hand, ONLY considers row keys. Because of that, it need
         * not concern itself about what kinds of data is returned, only the row keys that it sees. As
         * a result, it can eliminate rows that will NEVER be returned by the query (regardless of
         * that row's transactional state). Thus, we can execute optimistically outside of the SI
         * filter (which should have the side effect of reducing the number of rows which are passed
         * to SI, further improving performance).
         */
        return true;
    }

    public byte[] getMinimumStart() {
        if(keyRanges==null||keyRanges.length==0)
            return HConstants.EMPTY_START_ROW;
        return keyRanges[0].startKey;
    }

    public byte[] getMaximumStop(){
        return keyRanges[keyRanges.length-1].stopKey;
    }

    private static class KeyRange implements Externalizable,Comparable<KeyRange>{
        private static final long serialVersionUID = 1l;
        private byte[] startKey;
        private byte[] stopKey;

        private KeyRange() { }

        private KeyRange(byte[] startKey, byte[] stopKey) {
            this.startKey = startKey;
            this.stopKey = stopKey;
        }

        @Override
        public void writeExternal(ObjectOutput out) throws IOException {
            out.writeInt(startKey.length);
            out.write(startKey);
            out.writeInt(stopKey.length);
            out.write(stopKey);
        }

        @Override
        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            startKey = new byte[in.readInt()];
            in.readFully(startKey);
            stopKey = new byte[in.readInt()];
            in.readFully(stopKey);
        }

        public void writeTo(DataOutput out) throws IOException {
            out.writeInt(startKey.length);
            out.write(startKey);
            out.writeInt(stopKey.length);
            out.write(stopKey);
        }

        public void read(DataInput in) throws IOException{
            startKey = new byte[in.readInt()];
            in.readFully(startKey);
            stopKey = new byte[in.readInt()];
            in.readFully(stopKey);
        }

        public boolean intersects(KeyRange range){
            return !this.before(range.startKey, 0, startKey.length) && !this.after(range.stopKey, 0, stopKey.length);
        }

        public KeyRange union(KeyRange range){
            //find the smallest start key
            byte[] newStart;
            if(startKey ==null || startKey.length==0)
               newStart = startKey;
            else if(range.startKey==null||range.startKey.length==0)
                newStart = range.startKey;
            else{
                int startCompare = Bytes.compareTo(startKey,range.startKey);
                if(startCompare<=0){
                    newStart = startKey;
                }else
                    newStart = range.startKey;
            }

            //find the largest end key
            byte[] newStop;
            if(stopKey==null||stopKey.length==0)
               newStop = stopKey;
            else if(range.stopKey==null||range.stopKey.length==0)
                newStop = range.stopKey;
            else{
                int stopCompare = Bytes.compareTo(stopKey,range.stopKey);
                if(stopCompare<=0)
                    newStop = range.stopKey;
                else
                    newStop = stopKey;
            }

            return new KeyRange(newStart,newStop);
        }

        @Override
        public int compareTo(KeyRange o) {
            if(o==null)
                return 1;

            return Bytes.compareTo(startKey,o.startKey);
        }

        public boolean before(byte[] data, int rowKeyOffset, int rowKeyLength) {
            //stopKey = [] => this range goes to the end of the table, so it can't be before anything
            if(stopKey==null||stopKey.length==0) return false;

            return Bytes.compareTo(stopKey,0,stopKey.length,data,rowKeyOffset,rowKeyLength) <=0;
        }

        public boolean after(byte[] data, int rowKeyOffset, int rowKeyLength) {
            //startKey = [] => this range starts at the beginning of the table, so it can't be after anything
            if(startKey==null||startKey.length==0) return false;

            return Bytes.compareTo(startKey,0,startKey.length,data,rowKeyOffset,rowKeyLength)>0;
        }
    }

    public static class Builder{
        private List<KeyRange> keyRanges = Lists.newArrayList();

        public Builder addRange(byte[] start, byte[] stop){
            KeyRange range = new KeyRange(start,stop);
            //see if this should be merged with another range
            boolean merged=false;
            for(int i=0;i<keyRanges.size();i++){
                KeyRange existingRange = keyRanges.get(i);
                if(existingRange.intersects(range)){
                    keyRanges.set(i,existingRange.union(range));
                    merged=true;
                    break;
                }
            }
            if(!merged)
                keyRanges.add(range);

            return this;
        }

        public MultiRangeFilter build(){
            KeyRange[] ranges = new KeyRange[keyRanges.size()];
            ranges = keyRanges.toArray(ranges);

            Arrays.sort(ranges); //make sure we're in sorted order for our disjoint ranges
            return new MultiRangeFilter(ranges);
        }
    }
}
