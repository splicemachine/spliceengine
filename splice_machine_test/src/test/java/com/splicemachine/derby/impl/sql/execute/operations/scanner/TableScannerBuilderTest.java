package com.splicemachine.derby.impl.sql.execute.operations.scanner;

import java.io.IOException;

import com.splicemachine.db.iapi.error.StandardException;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;

import com.splicemachine.mrio.api.SpliceTableMapReduceUtil;
import com.splicemachine.si.api.Txn.IsolationLevel;
import com.splicemachine.si.api.TxnView;
import com.splicemachine.si.impl.ReadOnlyTxn;

public class TableScannerBuilderTest {
	protected static Scan scan = new Scan(Bytes.toBytes("1"));
	protected static int[] array1 = {1,2};
	protected static int[] array2 = {2,3};
	protected static int[] array3 = {3,4};
	protected static TxnView txn = ReadOnlyTxn.create(23, IsolationLevel.READ_UNCOMMITTED, null);
		
	
	@Test 
	public void testBase64EncodingDecoding() throws IOException, StandardException {
		String base64 = new TableScannerBuilder().keyColumnEncodingOrder(array1)
				.scan(scan)
				.transaction(txn)
				.keyDecodingMap(array2)
				.keyColumnTypes(array3).getTableScannerBuilderBase64String();
		
		TableScannerBuilder builder = TableScannerBuilder.getTableScannerBuilderFromBase64String(base64);
		Assert.assertArrayEquals(array1, builder.keyColumnEncodingOrder);
		Assert.assertArrayEquals(array2, builder.keyDecodingMap);
		Assert.assertArrayEquals(array3, builder.keyColumnTypes);
		Assert.assertEquals(SpliceTableMapReduceUtil.convertScanToString(scan), SpliceTableMapReduceUtil.convertScanToString(builder.scan));
		Assert.assertEquals(txn, builder.txn);
		
	}
	
	
}
