/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.splicemachine.derby.impl.sql.execute.operations.scanner;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.ScanSetBuilder;
import com.splicemachine.mrio.api.SpliceTableMapReduceUtil;
import com.splicemachine.si.api.txn.Txn.IsolationLevel;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.si.impl.SimpleTxnOperationFactory;
import com.splicemachine.si.impl.txn.ReadOnlyTxn;
import com.splicemachine.storage.RecordScan;
import com.splicemachine.storage.HScan;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

public class TableScannerBuilderTest{
    protected static Scan scan=new Scan(Bytes.toBytes("1"));
    protected static int[] array1={1,2};
    protected static int[] array2={2,3};
    protected static int[] array3={3,4};
    protected static TxnView txn=ReadOnlyTxn.create(23,IsolationLevel.READ_UNCOMMITTED,null,null);


    @Test
    public void testBase64EncodingDecoding() throws IOException, StandardException{
        ScanSetBuilder scanSetBuilder=new TestBuilder()
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
        Assert.assertEquals(SpliceTableMapReduceUtil.convertScanToString(scan),SpliceTableMapReduceUtil.convertScanToString(((HScan)builder.scan).unwrapDelegate()));
        Assert.assertEquals(txn,builder.txn);

    }

    private static class TestBuilder extends TableScannerBuilder{
        public TestBuilder(){ }

        @Override
        public DataSet buildDataSet() throws StandardException{
            Assert.fail("build is not called!");
            return null;
        }

        @Override
        protected void writeScan(ObjectOutput out) throws IOException{
            Scan scan=((HScan)this.scan).unwrapDelegate();
            byte[] bytes =ProtobufUtil.toScan(scan).toByteArray();
            out.writeInt(bytes.length);
            out.write(bytes);
        }

        @Override
        protected RecordScan readScan(ObjectInput in) throws IOException{
            byte[] bytes = new byte[in.readInt()];
            in.readFully(bytes);
            ClientProtos.Scan scan=ClientProtos.Scan.parseFrom(bytes);
            return new HScan(ProtobufUtil.toScan(scan));
        }

        @Override
        protected void writeTxn(ObjectOutput out) throws IOException{
            new SimpleTxnOperationFactory(null,null).writeTxn(txn,out);
        }

        @Override
        protected TxnView readTxn(ObjectInput in) throws IOException{
            return new SimpleTxnOperationFactory(null,null).readTxn(in);
        }
    }

}
