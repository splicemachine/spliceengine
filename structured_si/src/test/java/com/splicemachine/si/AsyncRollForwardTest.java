package com.splicemachine.si;

import com.google.common.base.Function;
import com.google.common.base.Suppliers;
import com.splicemachine.si.api.TransactionManager;
import com.splicemachine.si.api.Transactor;
import com.splicemachine.si.api.Txn;
import com.splicemachine.si.api.TxnLifecycleManager;
import com.splicemachine.si.coprocessors.RegionRollForwardAction;
import com.splicemachine.si.data.api.SDataLib;
import com.splicemachine.si.data.api.STableReader;
import com.splicemachine.si.impl.Tracer;
import com.splicemachine.si.impl.TransactionId;
import com.splicemachine.si.impl.WriteConflict;
import com.splicemachine.si.impl.rollforward.DelayedRollForwardAction;
import com.splicemachine.si.impl.rollforward.PushForwardAction;
import com.splicemachine.si.impl.rollforward.SIRollForwardQueue;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.*;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static com.splicemachine.constants.SIConstants.DEFAULT_FAMILY_BYTES;
import static com.splicemachine.constants.SIConstants.SNAPSHOT_ISOLATION_COMMIT_TIMESTAMP_COLUMN_STRING;
/**
 * Tests surrounding Asynchronous roll-foward queue testing
 * @author Scott Fines
 * Date: 2/17/14
 */
@Ignore("RollForwards don't work this way any longer")
public class AsyncRollForwardTest {
		boolean useSimple = true;
		StoreSetup storeSetup;
		TestTransactionSetup transactorSetup;
		Transactor transactor;
//		TransactionManager control;
		TxnLifecycleManager control;

		TransactorTestUtility testUtility;

		@SuppressWarnings("unchecked")
		void baseSetUp() throws IOException {
				transactor = transactorSetup.transactor;
				control = transactorSetup.txnLifecycleManager;
//				transactorSetup.rollForwardQueue = new SynchronousRollForwardQueue(
//								new RollForwardAction() {
//										@Override
//										public Boolean rollForward(long transactionId, List<byte[]> rowList) throws IOException {
//												final STableReader reader = storeSetup.getReader();
//												Object testSTable = reader.open(storeSetup.getPersonTableName());
//												return new RegionRollForwardAction(testSTable,
//																Providers.basicProvider(transactorSetup.transactionStore),
//																Providers.basicProvider(transactorSetup.dataStore)).rollForward(transactionId,rowList);
//										}
//								}, 1, 100, 1000, "test");
				testUtility = new TransactorTestUtility(useSimple,storeSetup,transactorSetup,transactor,control);
		}

		
		
		@Before
		public void setUp() throws Exception {
				storeSetup = new LStoreSetup();
				transactorSetup = new TestTransactionSetup(storeSetup, true);
				baseSetUp();
		}

		@After
		public void tearDown() throws Exception {
		}

		@Test(timeout = 11000)
		public void asynchRollForward() throws IOException, InterruptedException {
				checkAsynchRollForward(61,TransactionAction.COMMIT,false,null,false,new TransactionChecker() {
						@Override
						public void check(Txn tId, long timestamp) {
								Assert.assertEquals(tId.getTxnId() + 1, timestamp);
						}
				});
		}

		@Test(timeout = 11000)
		public void asynchRollForwardRolledBackTransaction() throws IOException, InterruptedException {
				checkAsynchRollForward(71, TransactionAction.ROLLBACK, false, null, false, new TransactionChecker() {
						@Override
						public void check(Txn tId, long timestamp) {
								Assert.assertEquals(-1, timestamp);

						}
				});

		}

		@Test(timeout = 11000)
		public void asynchRollForwardFailedTransaction() throws IOException, InterruptedException {

				checkAsynchRollForward(71, TransactionAction.FAIL, false, null, false, new TransactionChecker() {
						@Override
						public void check(Txn tId, long timestamp) {
								Assert.assertEquals(-1, timestamp);
						}
				});
		}

		@Test(timeout = 11000)
		public void asynchRollForwardNestedCommitFail() throws IOException, InterruptedException {

				checkAsynchRollForward(131, TransactionAction.COMMIT, true, TransactionAction.FAIL, false, new TransactionChecker() {
						@Override
						public void check(Txn tId, long timestamp) {
								Assert.assertEquals(-1, timestamp);
						}
				});
		}

		@Test(timeout = 11000)
		public void asynchRollForwardNestedFailCommitTransaction() throws IOException, InterruptedException {

				checkAsynchRollForward(132, TransactionAction.FAIL, true, TransactionAction.COMMIT, false, new TransactionChecker() {
						@Override
						public void check(Txn tId, long timestamp) {
								Assert.assertEquals(-1, timestamp);
						}
				});
		}

		@Test(timeout = 11000)
		public void asynchRollForwardNestedCommitCommit() throws IOException, InterruptedException {
				checkAsynchRollForward(133, TransactionAction.COMMIT, true, TransactionAction.COMMIT, false, new TransactionChecker() {
						@Override
						public void check(Txn t, long timestamp) {
								Assert.assertEquals(t.getTxnId() + 1, timestamp);
						}
				});
		}

		@Test(timeout = 11000)
		public void asynchRollForwardNestedFailFail() throws IOException, InterruptedException {

				checkAsynchRollForward(134, TransactionAction.FAIL, true, TransactionAction.FAIL, false, new TransactionChecker() {
						@Override
						public void check(Txn tId, long timestamp) {
								Assert.assertEquals(-1, timestamp);
						}
				});
		}

		@Test(timeout = 11000)
		public void asynchRollForwardFollowedByWriteConflict() throws IOException, InterruptedException {

				checkAsynchRollForward(83, TransactionAction.COMMIT, false, null, true, new TransactionChecker() {
						@Override
						public void check(Txn t, long timestamp) {
								Assert.assertEquals(t.getTxnId() + 2, timestamp);
						}
				});
		}

		@Test(timeout = 11000)
		public void writeDeleteScanWithIncludeSIColumnAfterRollForward() throws IOException, InterruptedException {
				try {
						Tracer.rollForwardDelayOverride = 200;
						final CountDownLatch latch = makeLatch("140moe");

						Txn t1 = control.beginTransaction();
						testUtility.insertAge(t1, "140moe", 50);
						testUtility.deleteRow(t1, "140moe");
						t1.commit();

						Txn t2 = control.beginTransaction();
						String expected = "";
						Assert.assertTrue(latch.await(11, TimeUnit.SECONDS));
						Assert.assertEquals(expected, testUtility.scanAll(t2, "140a", "140z", null));
				} finally {
						Tracer.rollForwardDelayOverride = null;
				}
		}

		protected long getTimestamp(Object[] input) {
				KeyValue cell = (KeyValue)input[1];
				final SDataLib dataLib = storeSetup.getDataLib();
				final byte[] keyValueValue = cell.getValue();
				if(keyValueValue.length==8)
						return (Long)dataLib.decode(keyValueValue,Long.class);
				else
						return (Integer) dataLib.decode(keyValueValue, Integer.class);
		}

		private static enum TransactionAction{
				COMMIT,
				ROLLBACK,
				FAIL
		}
		private static interface TransactionChecker{
				void check(Txn tId, long timestamp);
		}

		private void checkAsynchRollForward(int testIndex, TransactionAction action, boolean nested, TransactionAction parentAction,
																				boolean conflictingWrite, TransactionChecker checker)
						throws IOException, InterruptedException {
				try {
						Tracer.rollForwardDelayOverride = 100;
						Txn t0 = null;
						if (nested) {
								t0 = control.beginTransaction();
						}
						Txn t1;
						if (nested) {
								t1 = control.beginChildTransaction(t0, Bytes.toBytes("1184"));
						} else {
								t1 = control.beginTransaction();
						}
						Txn t1b = null;
						if (conflictingWrite) {
								t1b = control.beginTransaction();
						}
						final String testRow = "joe" + testIndex;
						testUtility.insertAge(t1, testRow, 20);
						final CountDownLatch latch = makeLatch(testRow);
												
						switch(action){
								case COMMIT:
										t1.commit();
										break;
								case ROLLBACK:
										t1.rollback();
										break;
								case FAIL:
										t1.rollback();
										break;
								default:
										Assert.fail("Unknown transaction action "+ action);
						}
						if (nested) {
								switch(parentAction){
										case COMMIT:
												t0.commit();
												break;
										case ROLLBACK:
												t0.rollback();
												break;
										case FAIL:
												t0.rollback();
												break;
										default:
												Assert.fail("Unknown transaction action "+ action);
								}
						}
						Result result = testUtility.readRaw(testRow,false);
						final SDataLib dataLib = storeSetup.getDataLib();
						latch.await();
						Result result2 = testUtility.readRaw(testRow,false);
						final List<KeyValue> commitTimestamps2 = result2.getColumn(dataLib.encode(DEFAULT_FAMILY_BYTES),
										dataLib.encode(SNAPSHOT_ISOLATION_COMMIT_TIMESTAMP_COLUMN_STRING));
						for (KeyValue c2 : commitTimestamps2) {
								long timestamp = getTimestamp(new Object[]{t1,c2});
								checker.check(t1,timestamp);
								Assert.assertEquals(t1.getTxnId(), c2.getTimestamp());
						}
						Txn t2 = control.beginTransaction();
						if(action==TransactionAction.COMMIT &&(!nested || parentAction==TransactionAction.COMMIT)){
								Assert.assertEquals(testRow + " age=20 job=null", testUtility.read(t2, testRow));
						} else {
								Assert.assertEquals(testRow + " absent", testUtility.read(t2, testRow));
						}
						if (conflictingWrite) {
								try {
										testUtility.insertAge(t1b, testRow, 21);
										Assert.fail();
								} catch (WriteConflict e) {
								} catch (RetriesExhaustedWithDetailsException e) {
										testUtility.assertWriteConflict(e);
								} finally {
										t1b.rollback();
								}
						}
				} finally {
						Tracer.rollForwardDelayOverride = null;
				}
		}



		private CountDownLatch makeLatch(final String targetKey) {
				final SDataLib dataLib = storeSetup.getDataLib();
				final CountDownLatch latch = new CountDownLatch(1);
				Tracer.registerRowRollForward(new Function<byte[],byte[]>() {
						@Override
						public byte[] apply(@Nullable byte[] input) {
								String key = (String) dataLib.decode(input, String.class);
								if (key.equals(targetKey)) {
										latch.countDown();
								}
								return null;
						}
				});
				return latch;
		}

		private CountDownLatch makeTransactionLatch(final Txn targetTransactionId) {
				final CountDownLatch latch = new CountDownLatch(1);
				Tracer.registerTransactionRollForward(new Function<Long, Object>() {
						@Override
						public Object apply(@Nullable Long input) {
								Assert.assertNotNull(input);
								if (input.equals(targetTransactionId.getTxnId())) {
										latch.countDown();
								}
								return null;
						}
				});
				return latch;
		}
}
