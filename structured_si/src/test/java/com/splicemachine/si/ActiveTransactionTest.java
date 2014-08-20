package com.splicemachine.si;

import com.carrotsearch.hppc.LongArrayList;
import com.google.common.collect.Lists;
import com.splicemachine.si.api.*;
import com.splicemachine.si.impl.ForwardingLifecycleManager;
import com.splicemachine.si.impl.SITransactor;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * Tests around the logic for fetching active transactions.
 *
 * @author Scott Fines
 * Date: 8/21/14
 */
public class ActiveTransactionTest {
    public static final byte[] DESTINATION_TABLE = Bytes.toBytes("1184");
    boolean useSimple = true;
    protected static StoreSetup storeSetup;
    protected static TestTransactionSetup transactorSetup;
    Transactor transactor;
    TxnLifecycleManager control;
    //	TransactionManager control;
    TransactorTestUtility testUtility;
    final List<Txn> createdParentTxns = Lists.newArrayList();
    private TxnStore txnStore;

    @SuppressWarnings("unchecked")
    protected void baseSetUp() {
        transactor = transactorSetup.transactor;
        control = new ForwardingLifecycleManager(transactorSetup.txnLifecycleManager){
            @Override
            protected void afterStart(Txn txn) {
                createdParentTxns.add(txn);
            }
        };
        testUtility = new TransactorTestUtility(useSimple,storeSetup,transactorSetup,transactor,control);
        txnStore  = transactorSetup.txnStore;
    }

    @BeforeClass
    public static void setupClass(){
        storeSetup = new LStoreSetup();
        transactorSetup = new TestTransactionSetup(storeSetup, true);
    }
    @Before
    public void setUp() throws IOException {
//        SynchronousRollForwardQueue.scheduler = Executors.newScheduledThreadPool(1);
        baseSetUp();
    }

    @Test
    public void testGetActiveWriteTransactionsShowsWriteTransactions() throws Exception {
        Txn parent = control.beginTransaction(DESTINATION_TABLE);
        transactorSetup.timestampSource.rememberTimestamp(parent.getTxnId());
        long[] ids = txnStore.getActiveTransactionIds(parent, null);
        Assert.assertEquals("Incorrect size",1,ids.length);
        Assert.assertArrayEquals("Incorrect values", new long[]{parent.getTxnId()}, ids);

        TimestampSource timestampSource = transactorSetup.timestampSource;
        timestampSource.nextTimestamp();
        timestampSource.nextTimestamp();


        //now see if a read-only doesn't show
        Txn next = control.beginTransaction();
        ids = txnStore.getActiveTransactionIds(next, null);
        Assert.assertEquals("Incorrect size",1,ids.length);
        Assert.assertArrayEquals("Incorrect values", new long[]{parent.getTxnId()}, ids);
    }

    @Test
		public void testGetActiveTransactionsWorksWithGap() throws Exception {
				Txn parent = control.beginTransaction(DESTINATION_TABLE);

        transactorSetup.timestampSource.rememberTimestamp(parent.getTxnId());
        long[] ids = txnStore.getActiveTransactionIds(parent, null);
				Assert.assertEquals("Incorrect size",1,ids.length);
				Assert.assertArrayEquals("Incorrect values", new long[]{parent.getTxnId()}, ids);

				TimestampSource timestampSource = transactorSetup.timestampSource;
				timestampSource.nextTimestamp();
				timestampSource.nextTimestamp();


				Txn next = control.beginTransaction(DESTINATION_TABLE);
				ids = txnStore.getActiveTransactionIds(next, null);
				Assert.assertEquals("Incorrect size",2,ids.length);
				Arrays.sort(ids);
				Assert.assertArrayEquals("Incorrect values",new long[]{parent.getTxnId(),next.getTxnId()},ids);
		}

    @Test
		public void testGetActiveTransactionsFiltersOutIfParentRollsbackGrandChildCommits() throws Exception {
				Txn parent = control.beginTransaction(DESTINATION_TABLE);
        transactorSetup.timestampSource.rememberTimestamp(parent.getTxnId());
				Txn child = control.beginChildTransaction(parent,parent.getIsolationLevel(), DESTINATION_TABLE);
				Txn grandChild = control.beginChildTransaction(child,child.getIsolationLevel(), DESTINATION_TABLE);
				long[] ids = txnStore.getActiveTransactionIds(grandChild, null);
				Assert.assertEquals("Incorrect size",3,ids.length);
				Arrays.sort(ids);
				Assert.assertArrayEquals("Incorrect values", new long[]{parent.getTxnId(), child.getTxnId(), grandChild.getTxnId()}, ids);
				@SuppressWarnings("UnusedDeclaration") Txn grandGrandChild = control.beginChildTransaction(child,child.getIsolationLevel(), DESTINATION_TABLE);

				//commit grandchild, leave child alone, and rollback parent, then see who's active. should be noone
				grandChild.commit();
				parent.rollback();

				Txn next = control.beginTransaction(DESTINATION_TABLE);
				ids = txnStore.getActiveTransactionIds(next, null);
				Assert.assertEquals("Incorrect size",1,ids.length);
				Assert.assertArrayEquals("Incorrect values", new long[]{next.getTxnId()}, ids);
		}

    @Test
    public void oldestActiveTransactionsOne() throws IOException {
        final Txn t1 = control.beginTransaction(DESTINATION_TABLE);
        transactorSetup.timestampSource.rememberTimestamp(t1.getTxnId());
        long[] ids = txnStore.getActiveTransactionIds(t1, null);
        Assert.assertEquals(1, ids.length);
        Assert.assertEquals(t1.getTxnId(), ids[0]);
    }

    @Test
    public void oldestActiveTransactionsTwo() throws IOException {
        final Txn t0 = control.beginTransaction(DESTINATION_TABLE);
        transactorSetup.timestampSource.rememberTimestamp(t0.getTxnId());
        final Txn t1 = control.beginTransaction(DESTINATION_TABLE);
        long[] ids = txnStore.getActiveTransactionIds(t1, null);
				System.out.printf("%d,%d,%s%n",t0.getTxnId(),t1.getTxnId(),Arrays.toString(ids));
        Assert.assertEquals(2, ids.length);
				Arrays.sort(ids);
        Assert.assertEquals(t0.getTxnId(), ids[0]);
        Assert.assertEquals(t1.getTxnId(), ids[1]);
    }

    @Test
    public void oldestActiveTransactionsFuture() throws IOException {
        final Txn t0 = control.beginTransaction(DESTINATION_TABLE);
        transactorSetup.timestampSource.rememberTimestamp(t0.getTxnId());
        final Txn t1 = control.beginTransaction(DESTINATION_TABLE);
        control.beginTransaction();
        final long[] ids = txnStore.getActiveTransactionIds(t1, null);
        Assert.assertEquals(2, ids.length);
				Arrays.sort(ids);
        Assert.assertEquals(t0.getTxnId(), ids[0]);
        Assert.assertEquals(t1.getTxnId(), ids[1]);
    }

    @Test
    public void oldestActiveTransactionsSkipCommitted() throws IOException {
        final Txn t0 = control.beginTransaction(DESTINATION_TABLE);
        transactorSetup.timestampSource.rememberTimestamp(t0.getTxnId());
        final Txn t1 = control.beginTransaction(DESTINATION_TABLE);
        final Txn t2 = control.beginTransaction(DESTINATION_TABLE);
        t0.commit();
        final long[] ids = txnStore.getActiveTransactionIds(t2, null);
        Assert.assertEquals(2, ids.length);
				Arrays.sort(ids);
        Assert.assertEquals(t1.getTxnId(), ids[0]);
        Assert.assertEquals(t2.getTxnId(), ids[1]);
    }

    @Test
    public void oldestActiveTransactionsSkipCommittedGap() throws IOException {
        final Txn t0 = control.beginTransaction(DESTINATION_TABLE);
        transactorSetup.timestampSource.rememberTimestamp(t0.getTxnId());
        final Txn t1 = control.beginTransaction(DESTINATION_TABLE);
        final Txn t2 = control.beginTransaction(DESTINATION_TABLE);
        t1.commit();
        final long[] ids = txnStore.getActiveTransactionIds(t2, null);
        Assert.assertEquals(2, ids.length);
				Arrays.sort(ids);
        Assert.assertEquals(t0.getTxnId(), ids[0]);
        Assert.assertEquals(t2.getTxnId(), ids[1]);
    }
    @Test
    public void oldestActiveTransactionsSavedTimestampAdvances() throws IOException {
        final Txn t0 = control.beginTransaction(DESTINATION_TABLE);
        transactorSetup.timestampSource.rememberTimestamp(t0.getTxnId() - 1);
				Txn t2 = control.beginTransaction(DESTINATION_TABLE);
				final Txn t3 = control.beginTransaction(DESTINATION_TABLE);
        t0.commit();
        final long originalSavedTimestamp = transactorSetup.timestampSource.retrieveTimestamp();
				transactorSetup.timestampSource.rememberTimestamp(t3.getTxnId()-1);
        txnStore.getActiveTransactionIds(t3, null);
        final long newSavedTimestamp = transactorSetup.timestampSource.retrieveTimestamp();
        Assert.assertEquals(originalSavedTimestamp + 2, newSavedTimestamp);
    }

    @Test
    public void oldestActiveTransactionsDoesNotIgnoreEffectiveStatus() throws IOException {
        final Txn t0 = control.beginTransaction(DESTINATION_TABLE);
        transactorSetup.timestampSource.rememberTimestamp(t0.getTxnId());
        final Txn t1 = control.beginChildTransaction(t0,DESTINATION_TABLE);
        t1.commit();
        final Txn t2 = control.beginChildTransaction(t0,DESTINATION_TABLE);
        long[] active = txnStore.getActiveTransactionIds(t2, null);
        Assert.assertEquals(3, active.length);
				Arrays.sort(active);
				Assert.assertArrayEquals(new long[]{t0.getTxnId(),t1.getTxnId(), t2.getTxnId()}, active);
    }

    @Test
    public void oldestActiveTransactionIgnoresCommitTimestampIds() throws IOException {
        final Txn t0 = control.beginTransaction(DESTINATION_TABLE);
        transactorSetup.timestampSource.rememberTimestamp(t0.getTxnId());
        Txn committedTransactionID = null;
        for (int i = 0; i < SITransactor.MAX_ACTIVE_COUNT; i++) {
            final Txn transactionId = control.beginTransaction(DESTINATION_TABLE);
            if (committedTransactionID == null) {
                committedTransactionID = transactionId;
            }
            transactionId.commit();
        }
				Assert.assertNotNull(committedTransactionID);
				Long commitTimestamp = (committedTransactionID.getTxnId() + 1);
				TxnView voidedTransactionID = txnStore.getTransaction(commitTimestamp);
				Assert.assertNull("Transaction id mistakenly found!",voidedTransactionID);

        final Txn t1 = control.beginTransaction(DESTINATION_TABLE);
        final long[] ids = txnStore.getActiveTransactionIds(t1, null);
        Assert.assertEquals(2, ids.length);
				Arrays.sort(ids);
        Assert.assertEquals(t0.getTxnId(), ids[0]);
				Assert.assertEquals(t1.getTxnId(), ids[1]);
				//this transaction should still be missing
				voidedTransactionID = txnStore.getTransaction(commitTimestamp);
				Assert.assertNull("Transaction id mistakenly found!",voidedTransactionID);
		}

    @Test
    public void oldestActiveTransactionsManyActive() throws IOException {
				LongArrayList startedTxns = LongArrayList.newInstance();
        final Txn t0 = control.beginTransaction(DESTINATION_TABLE);
				startedTxns.add(t0.getTxnId());
        transactorSetup.timestampSource.rememberTimestamp(t0.getTxnId());
        for (int i = 0; i < SITransactor.MAX_ACTIVE_COUNT / 2; i++) {
						Txn transactionId = control.beginTransaction(DESTINATION_TABLE);
						startedTxns.add(transactionId.getTxnId());
				}
        final Txn t1 = control.beginTransaction(DESTINATION_TABLE);
				startedTxns.add(t1.getTxnId());
        final long[] ids = txnStore.getActiveTransactionIds(t1, null);

        Assert.assertEquals(startedTxns.size(), ids.length);
				Arrays.sort(ids);
				Assert.assertArrayEquals(startedTxns.toArray(),ids);
//        Assert.assertEquals(t0.getTxnId(), ids[0]);
//        Assert.assertEquals(t1.getTxnId(), result.get(ids.length - 1).getId());
    }

    /*
     * The intent of the below test is to ensure that no more than a certain
     * number of transactions come back when you are fetching the active list--
     * the functionality as implemented is awkward and difficult, since it basically
     * defines an arbitrary constant.
     *
     * Further, the actual functionality that Splice uses involves the ActiveTransactionReader
     * logic, which submits tasks and does its own form of limiting; the only reason we
     * expose the logic in the TxnStore as this class tests is to allow us visibility into
     * a testable structure, and thus confirm that its working as we expect that way (and, hypothetically,
     * someone might want to use the coprocessor call directly because they don't have access to the task framework.
     * I don't recommend that approach, but you never know).
     *
     * As a result of these two points, I opted to remove this test. I didn't want to delete
     * the test, as it confers some historical interest to git spelunkers, but it has no relevance
     * to our current system any longer
     */
//    @Test
//    public void oldestActiveTransactionsTooManyActive() throws IOException {
//        final Txn t0 = control.beginTransaction(DESTINATION_TABLE);
//        transactorSetup.timestampSource.rememberTimestamp(t0.getTxnId());
//        for (int i = 0; i < SITransactor.MAX_ACTIVE_COUNT; i++) {
//            control.beginTransaction();
//        }
//
//        final Txn t1 = control.beginTransaction(DESTINATION_TABLE);
//        try {
//            txnStore.getActiveTransactionIds(t1, null);
//            Assert.fail();
//        } catch (RuntimeException ex) {
//            Assert.assertTrue(ex.getMessage().startsWith("expected max id of"));
//        }
//    }
}
