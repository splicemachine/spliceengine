package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.impl.storage.ClientResultScanner;
import com.splicemachine.derby.impl.storage.KeyValueUtils;
import com.splicemachine.derby.impl.storage.RegionAwareScanner;
import com.splicemachine.derby.impl.storage.SpliceResultScanner;
import com.splicemachine.derby.utils.DerbyBytesUtil;
import com.splicemachine.derby.utils.JoinSideExecRow;
import com.splicemachine.derby.utils.StandardIterator;
import com.splicemachine.derby.utils.marshall.PairDecoder;
import com.splicemachine.derby.utils.marshall.RowDecoder;
import com.splicemachine.derby.utils.marshall.RowMarshaller;
import com.splicemachine.encoding.Encoding;
import com.splicemachine.encoding.MultiFieldDecoder;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.HRegion;

import java.io.IOException;

/**
 * @author Scott Fines
 * Created on: 10/29/13
 */
public class ResultMergeScanner implements StandardIterator<JoinSideExecRow> {
    private final SpliceResultScanner scanner;

    private final PairDecoder leftDecoder;
    private final PairDecoder rightDecoder;

    private JoinSideExecRow leftSideRow;
    private JoinSideExecRow rightSideRow;
    private MultiFieldDecoder rightKeyDecoder;
    private MultiFieldDecoder leftKeyDecoder;

    public ResultMergeScanner(SpliceResultScanner scanner,
															PairDecoder leftDecoder,
															PairDecoder rightDecoder) {
        this.scanner = scanner;
        this.leftDecoder = leftDecoder;
        this.rightDecoder = rightDecoder;
    }

    @Override
    public void open() throws StandardException, IOException {
        scanner.open();
    }

    @Override
    public void close() throws StandardException, IOException {
        scanner.close();
    }

    @Override
    public JoinSideExecRow next() throws StandardException, IOException {
        Result result = scanner.next();
        if(result==null) return null;
        byte[] rowKey = result.getRow();
        /*
         * We need to get the ordinal from the Row Key.
         *
         * The format of the key is
         *
         * data + Ordinal + 8-byte task Id + 8 byte UUID
         *
         * The ordinal is either 0 or 1, both of which encode to a single byte. Thus,
         * the ordinal is the byte located at data.length-17
         */
				//TODO -sf- make the magic number 17 dissappear
				int ordinalOffset = rowKey.length - 17;
				int ordinal = Encoding.decodeInt(rowKey, ordinalOffset);
        if(ordinal == JoinUtils.JoinSide.RIGHT.ordinal()){
            ExecRow rightRow = rightDecoder.decode(KeyValueUtils.matchDataColumn(result.raw()));
            if(rightSideRow==null){
                rightKeyDecoder = MultiFieldDecoder.wrap(rowKey, SpliceDriver.getKryoPool());
                rightSideRow = new JoinSideExecRow(rightRow, JoinUtils.JoinSide.RIGHT);
            }else{
                rightKeyDecoder.set(rowKey);
            }
						rightKeyDecoder.seek(rightDecoder.getKeyPrefixOffset());
						int length = ordinalOffset-rightKeyDecoder.offset()-1;
            byte[] data = rightKeyDecoder.slice(length);
            rightSideRow.setHash(data);
            rightSideRow.setRowKey(rowKey);
            return rightSideRow;
        }else{
            ExecRow leftRow = leftDecoder.decode(KeyValueUtils.matchDataColumn(result.raw()));
            if(leftSideRow==null){
                leftKeyDecoder = MultiFieldDecoder.wrap(rowKey, SpliceDriver.getKryoPool());
                leftSideRow = new JoinSideExecRow(leftRow, JoinUtils.JoinSide.LEFT);
            }else{
                leftKeyDecoder.set(rowKey);
            }
            leftKeyDecoder.seek(leftDecoder.getKeyPrefixOffset());
						int length = ordinalOffset-leftKeyDecoder.offset()-1;
            byte[] data = leftKeyDecoder.slice(length);
            leftSideRow.setHash(data);
            leftSideRow.setRowKey(rowKey);
            return leftSideRow;
        }
    }

    public static ResultMergeScanner regionAwareScanner(Scan scan,
                                                  String txnId,
                                                  PairDecoder leftDecoder,
                                                  PairDecoder rightDecoder,
                                                  HRegion region) {
        RegionAwareScanner ras = RegionAwareScanner.create(txnId,region,scan, SpliceConstants.TEMP_TABLE_BYTES,
                new MergeSortScanBoundary(SpliceConstants.DEFAULT_FAMILY_BYTES, rightDecoder.getKeyPrefixOffset()));
        return new ResultMergeScanner(ras,leftDecoder,rightDecoder);
    }

    public static ResultMergeScanner clientScanner(Scan reduceScan,
																									 PairDecoder leftDecoder,
																									 PairDecoder rightDecoder) {
        ClientResultScanner scanner = new ClientResultScanner(SpliceConstants.TEMP_TABLE_BYTES,reduceScan,true);
        return new ResultMergeScanner(scanner,leftDecoder,rightDecoder);
    }
}
