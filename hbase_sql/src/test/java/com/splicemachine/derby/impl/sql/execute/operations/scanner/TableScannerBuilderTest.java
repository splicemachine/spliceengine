/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
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
import com.splicemachine.storage.DataScan;
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
        protected DataScan readScan(ObjectInput in) throws IOException{
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
