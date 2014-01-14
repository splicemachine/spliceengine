package com.splicemachine.hbase;

import com.splicemachine.constants.SIConstants;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.utils.marshall.RowMarshaller;
import com.splicemachine.si.api.TransactionalFilter;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.MetricName;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Scan filter-based replacement for Multi-Get operations. Use a scan with this filter attached
 * as a replacement anywhere you would use a normal MultiGet.
 *
 * @author Scott Fines
 * Created on: 8/30/13
 */
public class SkipScanFilter extends FilterBase implements TransactionalFilter {
    private byte[][] rowsToReturn;
    private int position = -1;
    private boolean failOnMissingRow = true;
    private boolean isDone;

    private boolean filterRow;

    private Counter visitedCounter;

    public SkipScanFilter(){}

    public void setFailOnMissingRow(boolean failOnMissingRow){
        this.failOnMissingRow = failOnMissingRow;
    }

    @Override
    public boolean filterRowKey(byte[] buffer, int offset, int length) {
        return super.filterRowKey(buffer, offset, length);    //To change body of overridden methods use File | Settings | File Templates.
    }

    @Override
    public ReturnCode filterKeyValue(KeyValue ignored) {
        if(!ignored.matchingColumn(SpliceConstants.DEFAULT_FAMILY_BYTES,RowMarshaller.PACKED_COLUMN_KEY))
            return ReturnCode.INCLUDE;
        if(position>=rowsToReturn.length){
            isDone=true;
            return ReturnCode.NEXT_ROW;
        }
        if(visitedCounter==null)
            visitedCounter = SpliceDriver.driver().getRegistry().newCounter(new MetricName("com.splicemachine.operations", "indexOp", "scanRowsVisited"));

        visitedCounter.inc();
        /*
         * Imagine the situation where there is a region split, and the scan encompasses
         * two regions. In that situation, the first row we receive can be located anywhere
         * in between the rows that we want to return. We need to then find the initial
         * position
         */
        if(position<0){
            byte[] rowToCheck;
            boolean shouldContinue;
            do{
                position++;
                rowToCheck = rowsToReturn[position];
                shouldContinue = Bytes.compareTo(rowToCheck,0,rowToCheck.length,ignored.getBuffer(),ignored.getRowOffset(),ignored.getRowLength())<0;
            }while(position<rowsToReturn.length &&shouldContinue);
        }

        /*
         * Our position is viable. We now find ourselves in one of three situations:
         *
         * 1. rowsToReturn[position] < currentRow --the current row position is not present. If
         * failOnMissingRow = true, then explode. Otherwise, keep going.
         * 2. rowsToReturn[position] = currentRow --we've found a row. adjust position up by one and include
         * 3. rowsToReturn[position] > currentRow --we haven't yet found this row, skip forward until we reach it
         */
        byte[] rowToCheck = rowsToReturn[position];
        int compareState = Bytes.compareTo(rowToCheck,0,rowToCheck.length,ignored.getBuffer(),ignored.getRowOffset(),ignored.getRowLength());
        if(compareState<0){
            //this row wasn't found. Blow up if so instructed. Otherwise, skip past it
            if(failOnMissingRow)
                throw new MissingRowException(rowToCheck);
            position++;
            filterRow = true;
            return ReturnCode.SEEK_NEXT_USING_HINT;
        } else if(compareState>0){
            //we haven't yet reached the row. Skip forward to it
            filterRow=true;
            return ReturnCode.SEEK_NEXT_USING_HINT;
        }else{
            //we've found a row, whoo!
            position++;
            filterRow=false;
            return ReturnCode.INCLUDE;
        }
    }

    @Override
    public boolean filterRow() {
        return filterRow;
    }

    @Override
    public void reset() {
        this.filterRow=true;
    }

    @Override
    public KeyValue getNextKeyHint(KeyValue currentKV) {
        if(position>=rowsToReturn.length) {
            isDone=true;
            return null;
        }
        //TODO -sf- remove the extra KV here
        return KeyValue.createFirstOnRow(rowsToReturn[position]);
    }

    @Override
    public boolean filterAllRemaining() {
        return isDone;
    }

    public void setRows(byte[][] rowsToReturn){
        this.rowsToReturn = rowsToReturn;
        Arrays.sort(this.rowsToReturn,Bytes.BYTES_COMPARATOR);
    }

    public void setRows(List<byte[]> rowsToReturn){
        this.rowsToReturn = new byte[rowsToReturn.size()][];
        rowsToReturn.toArray(this.rowsToReturn);
        Arrays.sort(this.rowsToReturn,Bytes.BYTES_COMPARATOR);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(rowsToReturn.length);
        for(byte[] row:rowsToReturn){
            out.writeInt(row.length);
            out.write(row);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        int rowSize = in.readInt();
        rowsToReturn = new byte[rowSize][];

        if(rowSize==0)
            position=0; //terminate any scans that may occur

        //read the rows to return
        for(int i=0;i<rowSize;i++){
            byte[] next = new byte[in.readInt()];
            in.readFully(next);
            rowsToReturn[i] = next;
        }

        //make sure the rows are sorted
        Arrays.sort(rowsToReturn,Bytes.BYTES_COMPARATOR);
    }

    @Override
    public boolean isBeforeSI() {
        return true;
    }
}
