package com.splicemachine.si.filters;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import org.junit.Assert;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.filter.Filter.ReturnCode;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.zookeeper.KeeperException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import com.splicemachine.iapi.txn.TransactionState;
import com.splicemachine.impl.si.txn.Transaction;
import com.splicemachine.impl.si.txn.TransactionManagerImpl;
import com.splicemachine.si.utils.SIConstants;
import com.splicemachine.si.utils.SIUtils;

public class SIFilterTest {
	protected static TransactionManagerImpl tm;
	protected static Transaction activeTransaction;
	protected static Transaction commitTransaction;
	protected static Transaction abortTransaction;
	protected static Transaction errorTransaction;
	protected static Transaction earlyActiveTransaction;
	protected static Transaction laterActiveTransaction;
	protected static Transaction latestActiveTransaction;
	@BeforeClass 
	public static void startup() throws IOException, KeeperException, InterruptedException, ExecutionException {
		tm = new TransactionManagerImpl();
		earlyActiveTransaction = tm.beginTransaction();
		activeTransaction = tm.beginTransaction();
		commitTransaction = tm.beginTransaction();
		tm.doCommit(commitTransaction);
		abortTransaction = tm.beginTransaction();
		tm.abort(abortTransaction);
		errorTransaction = tm.beginTransaction();
		errorTransaction.setTransactionState(TransactionState.ERROR);
		errorTransaction.write();
		laterActiveTransaction = tm.beginTransaction();
		latestActiveTransaction = tm.beginTransaction();
	}
	
	@AfterClass
	public static void tearDown() {

	}
		
	@Test
	public void isCommitTimestampTest() throws Exception {
		KeyValue commitKV = new KeyValue(new byte[0],SIConstants.SNAPSHOT_ISOLATION_FAMILY_BYTES,SIConstants.SNAPSHOT_ISOLATION_COMMIT_TIMESTAMP_COLUMN_BYTES,0,Bytes.toBytes(100l));
		Assert.assertTrue(SIFilter.isCommitTimestamp(commitKV));
		KeyValue nonCommitKV = new KeyValue(new byte[0],SIConstants.SNAPSHOT_ISOLATION_FAMILY_BYTES,SIConstants.SNAPSHOT_ISOLATION_TOMBSTONE_COLUMN_BYTES,0,Bytes.toBytes(100l));
		Assert.assertFalse(SIFilter.isCommitTimestamp(nonCommitKV));
	}

	@Test
	public void isTombstoneTest() throws Exception {
		KeyValue tombstoneKV = new KeyValue(new byte[0],SIConstants.SNAPSHOT_ISOLATION_FAMILY_BYTES,SIConstants.SNAPSHOT_ISOLATION_TOMBSTONE_COLUMN_BYTES,0,Bytes.toBytes(100l));
		Assert.assertTrue(SIFilter.isTombstone(tombstoneKV));
		KeyValue nonTombstoneKV = new KeyValue(new byte[0],SIConstants.SNAPSHOT_ISOLATION_FAMILY_BYTES,SIConstants.SNAPSHOT_ISOLATION_COMMIT_TIMESTAMP_COLUMN_BYTES,0,Bytes.toBytes(100l));
		Assert.assertFalse(SIFilter.isTombstone(nonTombstoneKV));
	}

	@Test
	public void testCommitMarker() {
		SIFilter filter = new SIFilter(100);
		Assert.assertEquals(ReturnCode.SKIP,filter.filterKeyValue(SIUtils.createActualCommitTimestamp(new byte[0], 1, 2)));
	}

	@Test
	public void testNoCommitMarker() {
		SIFilter filter = new SIFilter(100);
		Assert.assertEquals(ReturnCode.SKIP,filter.filterKeyValue(SIUtils.createTombstone(new byte[0], 1)));
		Assert.assertEquals(ReturnCode.SKIP,filter.filterKeyValue(new KeyValue(new byte[0],SIConstants.DEFAULT_FAMILY_BYTES,Bytes.toBytes(1),SIConstants.EMPTY_BYTE_ARRAY)));
	}
	
	@Test
	public void testSinglePriorInsertNoCommit() {
		SIFilter filter = new SIFilter(100);
		Assert.assertEquals(ReturnCode.SKIP,filter.filterKeyValue(SIUtils.createEmptyCommitTimestamp(new byte[0], activeTransaction.getStartTimestamp())));
		Assert.assertEquals(ReturnCode.SKIP,filter.filterKeyValue(new KeyValue(new byte[0],SIConstants.DEFAULT_FAMILY_BYTES,Bytes.toBytes(1),activeTransaction.getStartTimestamp(),SIConstants.EMPTY_BYTE_ARRAY)));
		Assert.assertEquals(ReturnCode.SKIP,filter.filterKeyValue(new KeyValue(new byte[0],SIConstants.DEFAULT_FAMILY_BYTES,Bytes.toBytes(1),activeTransaction.getStartTimestamp(),SIConstants.EMPTY_BYTE_ARRAY)));
	}

	@Test
	public void testSingleInsertWithCommitWithCommitTimestamp() {
		SIFilter filter = new SIFilter(latestActiveTransaction.getStartTimestamp());
		Assert.assertEquals(ReturnCode.SKIP,filter.filterKeyValue(SIUtils.createActualCommitTimestamp(new byte[0], commitTransaction.getStartTimestamp(),commitTransaction.getCommitTimestamp())));
		Assert.assertEquals(ReturnCode.INCLUDE,filter.filterKeyValue(new KeyValue(new byte[0],SIConstants.DEFAULT_FAMILY_BYTES,Bytes.toBytes(0),commitTransaction.getStartTimestamp(),Bytes.toBytes(1))));
		Assert.assertEquals(ReturnCode.INCLUDE,filter.filterKeyValue(new KeyValue(new byte[0],SIConstants.DEFAULT_FAMILY_BYTES,Bytes.toBytes(1),commitTransaction.getStartTimestamp(),Bytes.toBytes(1))));
	}

	@Test
	public void testSingleInsertWithCommitMissingCommitTimestamp() {
		SIFilter filter = new SIFilter(latestActiveTransaction.getStartTimestamp());
		Assert.assertEquals(ReturnCode.SKIP,filter.filterKeyValue(SIUtils.createEmptyCommitTimestamp(new byte[0], commitTransaction.getStartTimestamp())));
		Assert.assertEquals(ReturnCode.INCLUDE,filter.filterKeyValue(new KeyValue(new byte[0],SIConstants.DEFAULT_FAMILY_BYTES,Bytes.toBytes(0),commitTransaction.getStartTimestamp(),Bytes.toBytes(1))));
		Assert.assertEquals(ReturnCode.INCLUDE,filter.filterKeyValue(new KeyValue(new byte[0],SIConstants.DEFAULT_FAMILY_BYTES,Bytes.toBytes(1),commitTransaction.getStartTimestamp(),Bytes.toBytes(1))));
	}

	@Test
	public void testCommitWithActiveOnTopWithMissingCommitTimestamp() {
		SIFilter filter = new SIFilter(latestActiveTransaction.getStartTimestamp());
		Assert.assertEquals(ReturnCode.SKIP,filter.filterKeyValue(SIUtils.createEmptyCommitTimestamp(new byte[0], laterActiveTransaction.getStartTimestamp())));	
		Assert.assertEquals(ReturnCode.SKIP,filter.filterKeyValue(SIUtils.createEmptyCommitTimestamp(new byte[0], commitTransaction.getStartTimestamp())));
		Assert.assertEquals(ReturnCode.SKIP,filter.filterKeyValue(new KeyValue(new byte[0],SIConstants.DEFAULT_FAMILY_BYTES,Bytes.toBytes(0),laterActiveTransaction.getStartTimestamp(),Bytes.toBytes(1))));
		Assert.assertEquals(ReturnCode.INCLUDE,filter.filterKeyValue(new KeyValue(new byte[0],SIConstants.DEFAULT_FAMILY_BYTES,Bytes.toBytes(0),commitTransaction.getStartTimestamp(),Bytes.toBytes(1))));
		Assert.assertEquals(ReturnCode.SKIP,filter.filterKeyValue(new KeyValue(new byte[0],SIConstants.DEFAULT_FAMILY_BYTES,Bytes.toBytes(1),laterActiveTransaction.getStartTimestamp(),Bytes.toBytes(1))));
		Assert.assertEquals(ReturnCode.INCLUDE,filter.filterKeyValue(new KeyValue(new byte[0],SIConstants.DEFAULT_FAMILY_BYTES,Bytes.toBytes(1),commitTransaction.getStartTimestamp(),Bytes.toBytes(1))));
	}

	@Test
	public void testCommitWithAbortOnTopWithMissingCommitTimestamp() {
		SIFilter filter = new SIFilter(latestActiveTransaction.getStartTimestamp());
		Assert.assertEquals(ReturnCode.SKIP,filter.filterKeyValue(SIUtils.createEmptyCommitTimestamp(new byte[0], abortTransaction.getStartTimestamp())));	
		Assert.assertEquals(ReturnCode.SKIP,filter.filterKeyValue(SIUtils.createEmptyCommitTimestamp(new byte[0], commitTransaction.getStartTimestamp())));
		Assert.assertEquals(ReturnCode.SKIP,filter.filterKeyValue(new KeyValue(new byte[0],SIConstants.DEFAULT_FAMILY_BYTES,Bytes.toBytes(0),abortTransaction.getStartTimestamp(),Bytes.toBytes(1))));
		Assert.assertEquals(ReturnCode.INCLUDE,filter.filterKeyValue(new KeyValue(new byte[0],SIConstants.DEFAULT_FAMILY_BYTES,Bytes.toBytes(0),commitTransaction.getStartTimestamp(),Bytes.toBytes(1))));
		Assert.assertEquals(ReturnCode.SKIP,filter.filterKeyValue(new KeyValue(new byte[0],SIConstants.DEFAULT_FAMILY_BYTES,Bytes.toBytes(1),abortTransaction.getStartTimestamp(),Bytes.toBytes(1))));
		Assert.assertEquals(ReturnCode.INCLUDE,filter.filterKeyValue(new KeyValue(new byte[0],SIConstants.DEFAULT_FAMILY_BYTES,Bytes.toBytes(1),commitTransaction.getStartTimestamp(),Bytes.toBytes(1))));
	}
	
	@Test
	public void testCommitWithErrorOnTopWithMissingCommitTimestamp() {
		SIFilter filter = new SIFilter(latestActiveTransaction.getStartTimestamp());
		Assert.assertEquals(ReturnCode.SKIP,filter.filterKeyValue(SIUtils.createEmptyCommitTimestamp(new byte[0], errorTransaction.getStartTimestamp())));	
		Assert.assertEquals(ReturnCode.SKIP,filter.filterKeyValue(SIUtils.createEmptyCommitTimestamp(new byte[0], commitTransaction.getStartTimestamp())));
		Assert.assertEquals(ReturnCode.SKIP,filter.filterKeyValue(new KeyValue(new byte[0],SIConstants.DEFAULT_FAMILY_BYTES,Bytes.toBytes(0),errorTransaction.getStartTimestamp(),Bytes.toBytes(1))));
		Assert.assertEquals(ReturnCode.INCLUDE,filter.filterKeyValue(new KeyValue(new byte[0],SIConstants.DEFAULT_FAMILY_BYTES,Bytes.toBytes(0),commitTransaction.getStartTimestamp(),Bytes.toBytes(1))));
		Assert.assertEquals(ReturnCode.SKIP,filter.filterKeyValue(new KeyValue(new byte[0],SIConstants.DEFAULT_FAMILY_BYTES,Bytes.toBytes(1),errorTransaction.getStartTimestamp(),Bytes.toBytes(1))));
		Assert.assertEquals(ReturnCode.INCLUDE,filter.filterKeyValue(new KeyValue(new byte[0],SIConstants.DEFAULT_FAMILY_BYTES,Bytes.toBytes(1),commitTransaction.getStartTimestamp(),Bytes.toBytes(1))));
	}

	@Test
	public void testCommitWithLastestActiveOnTopWithMissingCommitTimestamp() {
		SIFilter filter = new SIFilter(latestActiveTransaction.getStartTimestamp());
		Assert.assertEquals(ReturnCode.SKIP,filter.filterKeyValue(SIUtils.createEmptyCommitTimestamp(new byte[0], latestActiveTransaction.getStartTimestamp())));	
		Assert.assertEquals(ReturnCode.SKIP,filter.filterKeyValue(SIUtils.createEmptyCommitTimestamp(new byte[0], commitTransaction.getStartTimestamp())));
		Assert.assertEquals(ReturnCode.INCLUDE,filter.filterKeyValue(new KeyValue(new byte[0],SIConstants.DEFAULT_FAMILY_BYTES,Bytes.toBytes(0),latestActiveTransaction.getStartTimestamp(),Bytes.toBytes(1))));
		Assert.assertEquals(ReturnCode.SKIP,filter.filterKeyValue(new KeyValue(new byte[0],SIConstants.DEFAULT_FAMILY_BYTES,Bytes.toBytes(0),commitTransaction.getStartTimestamp(),Bytes.toBytes(1))));
		Assert.assertEquals(ReturnCode.INCLUDE,filter.filterKeyValue(new KeyValue(new byte[0],SIConstants.DEFAULT_FAMILY_BYTES,Bytes.toBytes(1),latestActiveTransaction.getStartTimestamp(),Bytes.toBytes(1))));
		Assert.assertEquals(ReturnCode.SKIP,filter.filterKeyValue(new KeyValue(new byte[0],SIConstants.DEFAULT_FAMILY_BYTES,Bytes.toBytes(1),commitTransaction.getStartTimestamp(),Bytes.toBytes(1))));
	}

	@Test
	public void testTombstoneOnTopOfCommit() {
		SIFilter filter = new SIFilter(latestActiveTransaction.getStartTimestamp());
		Assert.assertEquals(ReturnCode.SKIP,filter.filterKeyValue(SIUtils.createEmptyCommitTimestamp(new byte[0], latestActiveTransaction.getStartTimestamp())));	
		Assert.assertEquals(ReturnCode.SKIP,filter.filterKeyValue(SIUtils.createEmptyCommitTimestamp(new byte[0], commitTransaction.getStartTimestamp())));
		Assert.assertEquals(ReturnCode.NEXT_COL,filter.filterKeyValue(SIUtils.createTombstone(new byte[0], latestActiveTransaction.getStartTimestamp())));
		Assert.assertEquals(ReturnCode.SKIP,filter.filterKeyValue(new KeyValue(new byte[0],SIConstants.DEFAULT_FAMILY_BYTES,Bytes.toBytes(0),commitTransaction.getStartTimestamp(),Bytes.toBytes(1))));
		Assert.assertEquals(ReturnCode.SKIP,filter.filterKeyValue(new KeyValue(new byte[0],SIConstants.DEFAULT_FAMILY_BYTES,Bytes.toBytes(1),commitTransaction.getStartTimestamp(),Bytes.toBytes(1))));
	}

	@Test
	public void testMixedColumnTombstone() {
		SIFilter filter = new SIFilter(latestActiveTransaction.getStartTimestamp());
		Assert.assertEquals(ReturnCode.SKIP,filter.filterKeyValue(SIUtils.createEmptyCommitTimestamp(new byte[0], latestActiveTransaction.getStartTimestamp())));	
		Assert.assertEquals(ReturnCode.SKIP,filter.filterKeyValue(SIUtils.createEmptyCommitTimestamp(new byte[0], commitTransaction.getStartTimestamp())));
		Assert.assertEquals(ReturnCode.NEXT_COL,filter.filterKeyValue(new KeyValue(new byte[0],SIConstants.DEFAULT_FAMILY_BYTES,Bytes.toBytes(0),latestActiveTransaction.getStartTimestamp(),SIConstants.EMPTY_BYTE_ARRAY)));
		Assert.assertEquals(ReturnCode.INCLUDE,filter.filterKeyValue(new KeyValue(new byte[0],SIConstants.DEFAULT_FAMILY_BYTES,Bytes.toBytes(1),commitTransaction.getStartTimestamp(),Bytes.toBytes(1))));
	}

	
}
