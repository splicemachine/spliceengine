package com.splicemachine.derby.impl.sql.execute.operations.scanner;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.ScanSetBuilder;
import com.splicemachine.mrio.api.SpliceTableMapReduceUtil;
import com.splicemachine.si.api.txn.Txn.IsolationLevel;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.si.impl.txn.ReadOnlyTxn;
import com.splicemachine.storage.HScan;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class TableScannerBuilderTest{
    protected static Scan scan=new Scan(Bytes.toBytes("1"));
    protected static int[] array1={1,2};
    protected static int[] array2={2,3};
    protected static int[] array3={3,4};
    protected static TxnView txn=ReadOnlyTxn.create(23,IsolationLevel.READ_UNCOMMITTED,null,null);


    @Test
    public void testBase64EncodingDecoding() throws IOException, StandardException{
        ScanSetBuilder scanSetBuilder=new TableScannerBuilder(){
            @Override
            public DataSet buildDataSet() throws StandardException{
                Assert.fail("build is not called!");
                return null;
            }
        }
                .keyColumnEncodingOrder(array1)
                .scan(new HScan(scan))
                .transaction(txn)
                .keyDecodingMap(array2)
                .keyColumnTypes(array3);
        String base64=((TableScannerBuilder)scanSetBuilder).getTableScannerBuilderBase64String();

        TableScannerBuilder builder=TableScannerBuilder.getTableScannerBuilderFromBase64String(base64);
        Assert.assertArrayEquals(array1,builder.keyColumnEncodingOrder);
        Assert.assertArrayEquals(array2,builder.keyDecodingMap);
        Assert.assertArrayEquals(array3,builder.keyColumnTypes);
        Assert.assertEquals(SpliceTableMapReduceUtil.convertScanToString(scan),SpliceTableMapReduceUtil.convertScanToString(builder.scan));
        Assert.assertEquals(txn,builder.txn);

    }


}
