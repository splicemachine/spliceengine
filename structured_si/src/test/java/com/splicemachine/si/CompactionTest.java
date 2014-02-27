package com.splicemachine.si;

import com.google.common.base.Function;
import com.splicemachine.si.api.TransactionManager;
import com.splicemachine.si.api.Transactor;
import com.splicemachine.si.coprocessors.RegionRollForwardAction;
import com.splicemachine.si.data.api.SDataLib;
import com.splicemachine.si.data.api.STableReader;
import com.splicemachine.si.data.light.LStore;
import com.splicemachine.si.impl.RollForwardAction;
import com.splicemachine.si.impl.SynchronousRollForwardQueue;
import com.splicemachine.si.impl.Tracer;
import com.splicemachine.si.impl.TransactionId;
import com.splicemachine.utils.Providers;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Result;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.splicemachine.constants.SIConstants.*;

/**
 * @author Scott Fines
 *         Date: 2/18/14
 */
public class CompactionTest {
		boolean useSimple = true;
		StoreSetup storeSetup;
		TestTransactionSetup transactorSetup;
		Transactor transactor;
		TransactionManager control;
		TransactorTestUtility testUtility;
		@SuppressWarnings("unchecked")
		void baseSetUp() {
				transactor = transactorSetup.transactor;
				control = transactorSetup.control;
				transactorSetup.rollForwardQueue = new SynchronousRollForwardQueue(
								new RollForwardAction() {
										@Override
										public Boolean rollForward(long transactionId, List<byte[]> rowList) throws IOException {
												final STableReader reader = storeSetup.getReader();
												Object testSTable = reader.open(storeSetup.getPersonTableName());
												new RegionRollForwardAction(testSTable,
																Providers.basicProvider(transactorSetup.transactionStore),
																Providers.basicProvider(transactorSetup.dataStore)).rollForward(transactionId,rowList);
												return true;
										}
								}, 10, 100, 1000, "test");
				testUtility = new TransactorTestUtility(useSimple,storeSetup,transactorSetup,transactor,control);
		}

		@Before
		public void setUp() throws Exception {
				SynchronousRollForwardQueue.scheduler = Executors.newScheduledThreadPool(1);
				storeSetup = new LStoreSetup();
				transactorSetup = new TestTransactionSetup(storeSetup, true);
				baseSetUp();
		}

		@After public void tearDown() throws Exception { }

		@Test
		public void compaction() throws IOException, InterruptedException {
				checkCompaction(70, true, new Function<Object[], Object>() {
						@Override
						public Object apply(@Nullable Object[] input) {
								Assert.assertTrue(input!=null && input[0]!=null);
								TransactionId t = (TransactionId) input[0];
								KeyValue cell = (KeyValue)input[1];
								final SDataLib dataLib = storeSetup.getDataLib();
								final long timestamp = (Long) dataLib.decode(cell.getValue(), Long.class);
								Assert.assertEquals(t.getId() + 1, timestamp);
								return null;
						}
				});
		}

		@Test
		public void compactionRollback() throws IOException, InterruptedException {
				checkCompaction(80, false, new Function<Object[], Object>() {
						@Override
						public Object apply(@Nullable Object[] input) {
								Assert.assertTrue(input!=null && input[1]!=null);
								KeyValue cell = (KeyValue)input[1];
								final SDataLib dataLib = storeSetup.getDataLib();
								final int timestamp = (Integer) dataLib.decode(cell.getValue(), Integer.class);
								Assert.assertEquals(-1, timestamp);
								return null;
						}
				});
		}

		@Test
		public void noCompaction() throws IOException, InterruptedException {
				checkNoCompaction(69, true, new Function<Object[], Object>() {
						@Override
						public Object apply(@Nullable Object[] input) {
								Assert.assertTrue(input!=null && input[1]!=null);
								KeyValue cell = (KeyValue)input[1];
								final SDataLib dataLib = storeSetup.getDataLib();
								final int timestamp = (Integer) dataLib.decode(cell.getValue(), Integer.class);
								Assert.assertEquals(-1, timestamp);
								return null;
						}
				});
		}

		@Test
		public void noCompactionRollback() throws IOException, InterruptedException {
				checkNoCompaction(79, false, new Function<Object[], Object>() {
						@Override
						public Object apply(@Nullable Object[] input) {
								Assert.assertTrue(input!=null && input[1]!=null);
								KeyValue cell = (KeyValue)input[1];
								final SDataLib dataLib = storeSetup.getDataLib();
								final int timestamp = (Integer) dataLib.decode(cell.getValue(), Integer.class);
								Assert.assertEquals(-1, timestamp);
								return null;
						}
				});
		}

		private void checkCompaction(int testIndex, boolean commit, Function<Object[], Object> timestampProcessor) throws IOException, InterruptedException {
				final String testRow = "joe" + testIndex;
				System.out.println("testRow " + testRow);
				final CountDownLatch latch = new CountDownLatch(1);
				Tracer.registerCompact(new Runnable() {
						@Override
						public void run() {
								latch.countDown();
						}
				});

				final HBaseTestingUtility testCluster = storeSetup.getTestCluster();
				final HBaseAdmin admin = useSimple ? null : testCluster.getHBaseAdmin();
				TransactionId t0 = null;
				for (int i = 0; i < 10; i++) {
						TransactionId tx = control.beginTransaction();
						if (i == 0) {
								t0 = tx;
						}
						testUtility.insertAge(tx, testRow + "-" + i, i);
						if (!useSimple) {
								admin.flush(storeSetup.getPersonTableName());
						}
						if (commit) {
								control.commit(tx);
						} else {
								control.rollback(tx);
						}
				}
				Assert.assertNotNull(t0);
				if (useSimple) {
						final LStore store = (LStore) storeSetup.getStore();
						store.compact(transactor, storeSetup.getPersonTableName());
				} else {
						admin.majorCompact(storeSetup.getPersonTableName());
						Assert.assertTrue(latch.await(2, TimeUnit.SECONDS));
				}
				Result result = testUtility.readRaw(testRow + "-0");
				final SDataLib dataLib = storeSetup.getDataLib();
				if (!commit) {
					Assert.assertTrue("no raw results should return after compaction - it is gone",result == null || result.isEmpty());
				} else {
					final List<KeyValue> commitTimestamps = result.getColumn(dataLib.encode(DEFAULT_FAMILY_BYTES),
									dataLib.encode(SNAPSHOT_ISOLATION_COMMIT_TIMESTAMP_COLUMN_STRING));
					for (KeyValue c : commitTimestamps) {
							timestampProcessor.apply(new Object[]{t0, c});
							Assert.assertEquals(t0.getId(), c.getTimestamp());
					}
				}
		}

		private void checkNoCompaction(int testIndex, boolean commit, Function<Object[], Object> timestampProcessor) throws IOException {
				TransactionId t0 = null;
				String testKey = "joe" + testIndex;
				for (int i = 0; i < 10; i++) {
						TransactionId tx = control.beginTransaction();
						if (i == 0) {
								t0 = tx;
						}
						testUtility.insertAge(tx, testKey + "-" + i, i);
						if (commit) {
								control.commit(tx);
						} else {
								control.rollback(tx);
						}
				}
				Assert.assertNotNull(t0);
				Result result = testUtility.readRaw(testKey + "-0");
				final SDataLib dataLib = storeSetup.getDataLib();
				final List<KeyValue> commitTimestamps = result.getColumn(dataLib.encode(DEFAULT_FAMILY_BYTES),
								dataLib.encode(SNAPSHOT_ISOLATION_COMMIT_TIMESTAMP_COLUMN));
				for (KeyValue c : commitTimestamps) {
						timestampProcessor.apply(new Object[]{t0, c});
						Assert.assertEquals(t0.getId(), c.getTimestamp());
				}
		}
}
