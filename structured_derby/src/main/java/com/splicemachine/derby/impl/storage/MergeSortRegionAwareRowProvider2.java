package com.splicemachine.derby.impl.storage;

import com.google.common.io.Closeables;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.impl.sql.execute.operations.JoinUtils;
import com.splicemachine.derby.impl.store.access.hbase.HBaseRowLocation;
import com.splicemachine.derby.utils.DerbyBytesUtil;
import com.splicemachine.derby.utils.JoinSideExecRow;
import com.splicemachine.derby.utils.marshall.RowDecoder;
import com.splicemachine.encoding.Encoding;
import com.splicemachine.encoding.MultiFieldDecoder;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.RowLocation;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.HRegion;

import java.util.NoSuchElementException;

/**
 * @author Scott Fines
 *         Created on: 6/17/13
 */
public class MergeSortRegionAwareRowProvider2 extends SingleScanRowProvider {
    protected final RegionAwareScanner scanner;
    private boolean populated=false;

    private ExecRow currentRow;
    private JoinSideExecRow joinSideRow;
    private RowLocation currentRowLocation;

    private RowDecoder leftDecoder;
    private RowDecoder rightDecoder;

    private JoinSideExecRow leftSideRow;
    private JoinSideExecRow rightSideRow;
    private MultiFieldDecoder rightKeyDecoder;
    private MultiFieldDecoder leftKeyDecoder;

    public MergeSortRegionAwareRowProvider2(String txnId,
                                            HRegion region,
                                            Scan scan,
                                            byte[] table,
                                            RowDecoder leftDecoder, RowDecoder rightDecoder) {
        this.scanner = RegionAwareScanner.create(txnId,region,scan,table,
                new MergeSortScanBoundary(SpliceConstants.DEFAULT_FAMILY_BYTES,leftDecoder,rightDecoder));
        this.leftDecoder = leftDecoder;
        this.rightDecoder = rightDecoder;
    }

    @Override
    public Scan toScan() {
        return scanner.toScan();
    }

    @Override
    public void open() throws StandardException {
        scanner.open();
    }

    @Override
    public void close() {
        if(rightKeyDecoder!=null)
            rightKeyDecoder.close();
        if(leftKeyDecoder!=null)
            leftKeyDecoder.close();
        super.close();
        Closeables.closeQuietly(scanner);
    }

    @Override
    public RowLocation getCurrentRowLocation() {
        return currentRowLocation;
    }

    @Override
    public byte[] getTableName() {
        return scanner.getTableName();
    }

    @Override
    public int getModifiedRowCount() {
        return 0;
    }

    public JoinSideExecRow nextJoinRow() throws StandardException{
        if(!hasNext()) throw new NoSuchElementException();
        populated=false;
        return joinSideRow;
    }

    @Override
    public boolean hasNext()throws StandardException {
        if(populated) return true;
        Result result = scanner.getNextResult();
        if(result!=null && !result.isEmpty()){
            /*
             * We need to get the ordinal from the Row Key.
             *
             * The format of the key is
             *
             * data + Ordinal + 8-byte task Id + 8 byte UUID
             *
             * The ordinal is either 0 or 1, both of which encode to a single byte. Thus,
             * the ordinal is the byte located at data.length-19 (don't forget the separators).
             */
            byte[] key = result.getRow();
            int ordinal = Encoding.decodeInt(key,key.length-19);
//            int ordinal = Encoding.decodeInt(result.getValue(SpliceConstants.DEFAULT_FAMILY_BYTES,JoinUtils.JOIN_SIDE_COLUMN));
            if(ordinal== JoinUtils.JoinSide.RIGHT.ordinal()){
                ExecRow rightRow = rightDecoder.decode(result.raw());
                currentRow = rightRow;
                if(rightSideRow==null){
                    rightKeyDecoder = MultiFieldDecoder.wrap(result.getRow(), SpliceDriver.getKryoPool());
                    rightKeyDecoder.seek(11);
                    byte[] data = DerbyBytesUtil.slice(rightKeyDecoder,rightDecoder.getKeyColumns(),rightRow.getRowArray());
                    rightSideRow = new JoinSideExecRow(rightRow,JoinUtils.JoinSide.RIGHT,data);
//                    rightSideRow = new JoinSideExecRow(rightRow, JoinUtils.JoinSide.RIGHT, rightKeyDecoder.slice(rightDecoder.getKeyColumns().length));
                }else{
                    rightKeyDecoder.set(result.getRow());
                    rightKeyDecoder.seek(11);
                    byte[] data = DerbyBytesUtil.slice(rightKeyDecoder,rightDecoder.getKeyColumns(),rightRow.getRowArray());
                    rightSideRow.setHash(data);
                }
                joinSideRow = rightSideRow;
            }else{
                ExecRow leftRow = leftDecoder.decode(result.raw());
                currentRow = leftRow;
                if(leftSideRow==null){
                    leftKeyDecoder = MultiFieldDecoder.wrap(result.getRow(),SpliceDriver.getKryoPool());
                    leftKeyDecoder.seek(11);
                    byte[] data = DerbyBytesUtil.slice(leftKeyDecoder,leftDecoder.getKeyColumns(),leftRow.getRowArray());
                    leftSideRow = new JoinSideExecRow(leftRow, JoinUtils.JoinSide.LEFT,data);
                }else{
                    leftKeyDecoder.set(result.getRow());
                    leftKeyDecoder.seek(11);
                    byte[] data = DerbyBytesUtil.slice(leftKeyDecoder,leftDecoder.getKeyColumns(),leftRow.getRowArray());
                    leftSideRow.setHash(data);
                }
                joinSideRow = leftSideRow;
            }
            if(currentRowLocation==null)
                currentRowLocation = new HBaseRowLocation(result.getRow());
            else
                currentRowLocation.setValue(result.getRow());
            populated=true;
            return true;
        }
        return false;
    }


    @Override
    public ExecRow next() throws StandardException {
        if(!hasNext()) throw new NoSuchElementException();
        populated=false;
        return currentRow;
    }

    private static class MergeSortScanBoundary extends BaseHashAwareScanBoundary{
        private final RowDecoder leftDecoder;
        private final RowDecoder rightDecoder;

        protected MergeSortScanBoundary(byte[] columnFamily,RowDecoder leftDecoder,RowDecoder rightDecoder) {
            super(columnFamily);
            this.leftDecoder = leftDecoder;
            this.rightDecoder = rightDecoder;
        }

        @Override
        public byte[] getStartKey(Result result) {
            byte[] data = result.getValue(SpliceConstants.DEFAULT_FAMILY_BYTES, JoinUtils.JOIN_SIDE_COLUMN);
            if(data==null) return null;
            int ordinal = Encoding.decodeInt(data);
            MultiFieldDecoder decoder = MultiFieldDecoder.wrap(result.getRow(),SpliceDriver.getKryoPool());
            decoder.seek(9); //skip the prefix value
            if(ordinal== JoinUtils.JoinSide.RIGHT.ordinal()){
                //copy out all the fields from the key until we reach the ordinal
                return decoder.slice(rightDecoder.getKeyColumns().length);
            }else{
                return decoder.slice(leftDecoder.getKeyColumns().length);
            }
        }

        @Override
        public byte[] getStopKey(Result result) {
            byte[] data = result.getValue(SpliceConstants.DEFAULT_FAMILY_BYTES, JoinUtils.JOIN_SIDE_COLUMN);
            if(data==null) return null;
            int ordinal = Encoding.decodeInt(data);
            MultiFieldDecoder decoder = MultiFieldDecoder.wrap(result.getRow(),SpliceDriver.getKryoPool());
            decoder.seek(9); //skip the prefix value
            if(ordinal== JoinUtils.JoinSide.RIGHT.ordinal()){
                //copy out all the fields from the key until we reach the ordinal
                return decoder.slice(rightDecoder.getKeyColumns().length);
            }else{
                return decoder.slice(leftDecoder.getKeyColumns().length);
            }
        }
    }

}
