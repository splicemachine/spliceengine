package com.ir.hbase.test.txn;

import java.util.ArrayList;
import java.util.List;
import junit.framework.Assert;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.RecoverableZooKeeper;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import com.ir.constants.TransactionStatus;
import com.ir.constants.TxnConstants;
import com.ir.hbase.txn.TransactionManager;
import com.ir.hbase.txn.TransactionState;

public class TransactionManagerTest extends BaseTxnTest{
    static final Log LOG = LogFactory.getLog(TransactionManagerTest.class);
    private static TransactionManager tm;
    private static HTable txnTable;
    private static HTable htable1;
    private static HTable htable2;
    private static ZooKeeperWatcher zkw;
    private static RecoverableZooKeeper rzk;
    @Test
	public void testBeginTransaction() throws Exception {
		TransactionState state1 = tm.beginTransaction();
		Assert.assertTrue(state1.getTransactionID().startsWith(DEFAULT_TRANSACTION_PATH));
		Assert.assertTrue(state1.getTransactionID().endsWith("0000000000"));
	}
    /**
     * put rows into table with given transaction id
     */
    public void putDataInTable(HTable table, String transactionID) throws Exception {
    	List<Put> puts = new ArrayList<Put>();
		for (int i=1; i<11; ++i) {
	    	Put put = new Put(("REGULAR_ROW"+i).getBytes());
	    	put.setAttribute(TxnConstants.TRANSACTION_ID, transactionID.getBytes());
	    	put.add(DEFAULT_FAMILY.getBytes(), "REGULAR_COLM1".getBytes(), ("VALUE"+i+"-"+1).getBytes());
	    	put.add(DEFAULT_FAMILY.getBytes(), "REGULAR_COLM2".getBytes(), ("VALUE"+i+"-"+2).getBytes());
	    	puts.add(put);
		}
		table.put(puts);
    }
    
    public void putDataInTable2(HTable table, String transactionID) throws Exception {
    	List<Put> puts = new ArrayList<Put>();
		for (int i=1; i<11; ++i) {
	    	Put put = new Put(("REGULAR_ROW"+i).getBytes());
	    	put.setAttribute(TxnConstants.TRANSACTION_ID, transactionID.getBytes());
	    	put.add("FAMILY1".getBytes(), "REGULAR_COLM1".getBytes(), ("VALUE"+i+"-"+1).getBytes());
	    	put.add("FAMILY1".getBytes(), "REGULAR_COLM2".getBytes(), ("VALUE"+i+"-"+2).getBytes());
	    	put.add("FAMILY2".getBytes(), "REGULAR_COLM3".getBytes(), ("VALUE"+i+"-"+1).getBytes());
	    	put.add("FAMILY2".getBytes(), "REGULAR_COLM4".getBytes(), ("VALUE"+i+"-"+2).getBytes());
	    	put.add("FAMILY3".getBytes(), "REGULAR_COLM5".getBytes(), ("VALUE"+i+"-"+1).getBytes());
	    	put.add("FAMILY3".getBytes(), "REGULAR_COLM6".getBytes(), ("VALUE"+i+"-"+2).getBytes());
	    	puts.add(put);
		}
		table.put(puts);
    }
    /**
     * READ_UNCOMMITED get rows, READ_COMMITED get null, since we didn't commit yet.
     */
    @Test
    public void testGetCommitAndUncommit() throws Exception {
		TransactionState state1 = tm.beginTransaction();
		String transactionID = state1.getTransactionID();
		putDataInTable(htable1, transactionID);

		for (int i=1; i<5; ++i) {
			Get get = new Get(("REGULAR_ROW"+i).getBytes());
	    	get.setAttribute(TxnConstants.TRANSACTION_ID, transactionID.getBytes());
	    	get.setAttribute(TxnConstants.TRANSACTION_ISOLATION_LEVEL, 
	    			Bytes.toBytes(TxnConstants.TransactionIsolationLevel.READ_UNCOMMITED.toString()));
			Assert.assertEquals("VALUE"+i+"-"+1, Bytes.toString(htable1.get(get).value()));
		}
		for (int i=5; i<11; ++i) {
	    	Get get = new Get(("REGULAR_ROW"+i).getBytes());
	    	//As for READ_COMMITTED, don't need to have transaction id
	    	get.setAttribute(TxnConstants.TRANSACTION_ISOLATION_LEVEL, 
	    			Bytes.toBytes(TxnConstants.TransactionIsolationLevel.READ_COMMITTED.toString()));
			Assert.assertEquals(null, htable1.get(get).value());
		}
    }
    /**
     * READ_UNCOMMITED scans rows, READ_COMMITED scans null, since we didn't commit yet.
     */
    @Test
    public void testScanCommitAndUncommit() throws Exception {
    	TransactionState state1 = tm.beginTransaction();
		String transactionID = state1.getTransactionID();
		putDataInTable(htable1, transactionID);
		
		Scan scan = new Scan(("REGULAR_ROW"+1).getBytes(), ("REGULAR_ROW"+10).getBytes());
		scan.setAttribute(TxnConstants.TRANSACTION_ID, transactionID.getBytes());
    	scan.setAttribute(TxnConstants.TRANSACTION_ISOLATION_LEVEL, 
    			Bytes.toBytes(TxnConstants.TransactionIsolationLevel.READ_UNCOMMITED.toString()));
		ResultScanner scanner = htable1.getScanner(scan);
		Result result = null;
		while ((result = scanner.next()) != null) {
			for (byte[] family : result.getMap().keySet())
				for (byte[] col : result.getFamilyMap(family).keySet())
					Assert.assertTrue(null, Bytes.toString(result.getFamilyMap(family).get(col)).startsWith("VALUE"));
		}
		
		Scan scan2 = new Scan(("REGULAR_ROW"+1).getBytes(), ("REGULAR_ROW"+10).getBytes());
    	//As for READ_COMMITTED, don't need to have transaction id
    	scan2.setAttribute(TxnConstants.TRANSACTION_ISOLATION_LEVEL, 
    			Bytes.toBytes(TxnConstants.TransactionIsolationLevel.READ_COMMITTED.toString()));
		ResultScanner scanner2 = htable1.getScanner(scan2);
		if (scanner2.next() != null) {
			assert false;
		}
    }
    
    @Test
    public void testDeleteAndGetCommitAndUncommit() throws Exception {
    	TransactionState state1 = tm.beginTransaction();
		String transactionID = state1.getTransactionID();
		putDataInTable(htable1, transactionID);
		//Delete column 1 of first 5 rows
		for (int i=1; i<5; ++i) {
			Delete delete = new Delete(("REGULAR_ROW"+i).getBytes());
			delete.deleteColumn(DEFAULT_FAMILY.getBytes(), "REGULAR_COLM1".getBytes());
	    	delete.setAttribute(TxnConstants.TRANSACTION_ID, transactionID.getBytes());
	    	htable1.delete(delete);
		}
		//READ_UNCOMMITED column 1 of first 5 rows, will get nothing.
		for (int i=1; i<5; ++i) {
			Get get = new Get(("REGULAR_ROW"+i).getBytes());
			get.addColumn(DEFAULT_FAMILY.getBytes(), "REGULAR_COLM1".getBytes());
	    	get.setAttribute(TxnConstants.TRANSACTION_ID, transactionID.getBytes());
			get.setAttribute(TxnConstants.TRANSACTION_ISOLATION_LEVEL, 
	    			Bytes.toBytes(TxnConstants.TransactionIsolationLevel.READ_UNCOMMITED.toString()));
			Assert.assertEquals(null, htable1.get(get).value());
		}
		//READ_UNCOMMITED column 2 of first 5 rows, will get values, didn't touch column 2. (A bug fixed here, it works per-column transaction now) 
		for (int i=1; i<5; ++i) {
			Get get = new Get(("REGULAR_ROW"+i).getBytes());
			get.addColumn(DEFAULT_FAMILY.getBytes(), "REGULAR_COLM2".getBytes());
	    	get.setAttribute(TxnConstants.TRANSACTION_ID, transactionID.getBytes());
			get.setAttribute(TxnConstants.TRANSACTION_ISOLATION_LEVEL, 
	    			Bytes.toBytes(TxnConstants.TransactionIsolationLevel.READ_UNCOMMITED.toString()));
	    	Assert.assertEquals("VALUE"+i+"-"+2, Bytes.toString(htable1.get(get).value()));
		}
    }
    //A more complicated delete case
    @Test
    public void testDeleteAndGetCommitAndUncommit2() throws Exception {
    	TransactionState state1 = tm.beginTransaction();
		String transactionID = state1.getTransactionID();
		putDataInTable2(htable1, transactionID);
		//Delete col3 col4 of first 5 rows
		for (int i=1; i<5; ++i) {
			Delete delete = new Delete(("REGULAR_ROW"+i).getBytes());
			delete.deleteColumn("FAMILY2".getBytes(), "REGULAR_COLM3".getBytes());
			delete.deleteColumn("FAMILY2".getBytes(), "REGULAR_COLM4".getBytes());
	    	delete.setAttribute(TxnConstants.TRANSACTION_ID, transactionID.getBytes());
	    	htable1.delete(delete);
		}
		//READ_UNCOMMITED col1 col2 col5 col6 of first 5 rows, will get nothing.
		for (int i=1; i<5; ++i) {
			Get get = new Get(("REGULAR_ROW"+i).getBytes());
			get.addColumn("FAMILY2".getBytes(), "REGULAR_COLM3".getBytes());
			get.addColumn("FAMILY2".getBytes(), "REGULAR_COLM4".getBytes());
	    	get.setAttribute(TxnConstants.TRANSACTION_ID, transactionID.getBytes());
			get.setAttribute(TxnConstants.TRANSACTION_ISOLATION_LEVEL, 
	    			Bytes.toBytes(TxnConstants.TransactionIsolationLevel.READ_UNCOMMITED.toString()));
			Assert.assertEquals(null, htable1.get(get).value());
		}
		//READ_UNCOMMITED col1 col2 of first 5 rows, will get values. (A bug fixed here, it works per-column transaction now) 
		for (int i=1; i<5; ++i) {
			Get get = new Get(("REGULAR_ROW"+i).getBytes());
			get.addColumn("FAMILY1".getBytes(), "REGULAR_COLM1".getBytes());
			get.addColumn("FAMILY1".getBytes(), "REGULAR_COLM2".getBytes());
			get.setAttribute(TxnConstants.TRANSACTION_ID, transactionID.getBytes());
			get.setAttribute(TxnConstants.TRANSACTION_ISOLATION_LEVEL, 
	    			Bytes.toBytes(TxnConstants.TransactionIsolationLevel.READ_UNCOMMITED.toString()));
	    	Assert.assertTrue(Bytes.toString(htable1.get(get).value()).startsWith("VALUE"));
		}
    }

	@Test
	public void testAbortTransaction() throws Exception {
		//need to setup abort conditions based on actual situation
	}
	/**
	 * Before fire prepare commit, transaction id node and all its children have PENDING;
	 * After fire prepare commit, they all have PREPARE_COMMIT.
	 * Their watching each other works.
	 * Two regions are visited in practice in this case.
	 */
	@Test
	public void testPrepareCommit() throws Exception {
		TransactionState state1 = tm.beginTransaction();
		String transactionID = state1.getTransactionID();
		putDataInTable(htable1, transactionID);
		putDataInTable(htable2, transactionID);
		Assert.assertEquals(TransactionStatus.PENDING, TransactionStatus.valueOf(Bytes.toString(rzk.getData(transactionID, false, null))));
		for(String node : rzk.getChildren(transactionID, false)) {
			Assert.assertEquals(TransactionStatus.PENDING, TransactionStatus.valueOf(Bytes.toString(rzk.getData(transactionID + "/" + node, false, null))));
		}
		tm.prepareCommit(state1);
		Assert.assertEquals(TransactionStatus.PREPARE_COMMIT, TransactionStatus.valueOf(Bytes.toString(rzk.getData(transactionID, false, null))));
		for(String node : rzk.getChildren(transactionID, false)) {
			Assert.assertEquals(TransactionStatus.PREPARE_COMMIT, TransactionStatus.valueOf(Bytes.toString(rzk.getData(transactionID + "/" + node, false, null))));
		}
	}

	@Test
	public void testDoCommit() throws Exception {
		//docommit is used in testTryCommit
	}
	/**
	 * Two regions are visited in practice in this case.
	 */
	@Test
	public void testTryCommit() throws Exception {
		TransactionState state1 = tm.beginTransaction();
		String transactionID = state1.getTransactionID();
		putDataInTable(htable1, transactionID);
		putDataInTable(htable2, transactionID);
		//Check status
		Assert.assertEquals(TransactionStatus.PENDING, TransactionStatus.valueOf(Bytes.toString(rzk.getData(transactionID, false, null))));
		for(String node : rzk.getChildren(transactionID, false)) {
			Assert.assertEquals(TransactionStatus.PENDING, TransactionStatus.valueOf(Bytes.toString(rzk.getData(transactionID + "/" + node, false, null))));
		}
		 //READ_COMMITTED from table1, get null
		for (int i=1; i<5; ++i) {
			Get get = new Get(("REGULAR_ROW"+i).getBytes());
	    	//As for READ_COMMITTED, don't need to have transaction id
	    	get.setAttribute(TxnConstants.TRANSACTION_ISOLATION_LEVEL, 
	    			Bytes.toBytes(TxnConstants.TransactionIsolationLevel.READ_COMMITTED.toString()));
			Assert.assertEquals(null, htable1.get(get).value());
		}
		 //READ_COMMITTED from table2, get null
		for (int i=1; i<5; ++i) {
			Get get = new Get(("REGULAR_ROW"+i).getBytes());
	    	//As for READ_COMMITTED, don't need to have transaction id
	    	get.setAttribute(TxnConstants.TRANSACTION_ISOLATION_LEVEL, 
	    			Bytes.toBytes(TxnConstants.TransactionIsolationLevel.READ_COMMITTED.toString()));
			Assert.assertEquals(null, htable2.get(get).value());
		}
		//Try Commit
		tm.tryCommit(state1);
		//Check status
		Assert.assertEquals(TransactionStatus.DO_COMMIT, TransactionStatus.valueOf(Bytes.toString(rzk.getData(transactionID, false, null))));
		for(String node : rzk.getChildren(transactionID, false)) {
			Assert.assertEquals(TransactionStatus.DO_COMMIT, TransactionStatus.valueOf(Bytes.toString(rzk.getData(transactionID + "/" + node, false, null))));
		}
		//READ_COMMITTED from table1, get value
		for (int i=1; i<5; ++i) {
			Get get = new Get(("REGULAR_ROW"+i).getBytes());
	    	//As for READ_COMMITTED, don't need to have transaction id
	    	get.setAttribute(TxnConstants.TRANSACTION_ISOLATION_LEVEL, 
	    			Bytes.toBytes(TxnConstants.TransactionIsolationLevel.READ_COMMITTED.toString()));
			Assert.assertEquals("VALUE"+i+"-"+1, Bytes.toString(htable1.get(get).value()));
		}
		 //READ_COMMITTED from table2, get value
		for (int i=1; i<5; ++i) {
			Get get = new Get(("REGULAR_ROW"+i).getBytes());
	    	//As for READ_COMMITTED, don't need to have transaction id
	    	get.setAttribute(TxnConstants.TRANSACTION_ISOLATION_LEVEL, 
	    			Bytes.toBytes(TxnConstants.TransactionIsolationLevel.READ_COMMITTED.toString()));
			Assert.assertEquals("VALUE"+i+"-"+1, Bytes.toString(htable2.get(get).value()));
		}
	}

	public static void setUp() throws Exception {
		System.out.println(hbaseTestingUtility.getClusterTestDir());
		hbaseTestingUtility.getConfiguration().set(TxnConstants.TRANSACTION_PATH_NAME, DEFAULT_TRANSACTION_PATH);
		hbaseTestingUtility.getConfiguration().set("hbase.zookeeper.property.clientPort", "21818");
		hbaseTestingUtility.getConfiguration().set("hbase.coprocessor.region.classes", "com.ir.hbase.txn.coprocessor.region.TransactionalManagerRegionObserver,com.ir.hbase.txn.coprocessor.region.TransactionalRegionObserver");
		hbaseTestingUtility.getConfiguration().reloadConfiguration();
		start();
		txnTable = hbaseTestingUtility.createTable(TxnConstants.TRANSACTION_TABLE_BYTES, TxnConstants.TRANSACTION_TABLE_BYTES);
		HTableDescriptor desc = new HTableDescriptor("REGULAR_TABLE1");
		desc.addFamily(new HColumnDescriptor(DEFAULT_FAMILY));
		desc.addFamily(new HColumnDescriptor("FAMILY1"));
		desc.addFamily(new HColumnDescriptor("FAMILY2"));
		desc.addFamily(new HColumnDescriptor("FAMILY3"));
		hbaseTestingUtility.getHBaseAdmin().createTable(desc);
		htable1 = new HTable(hbaseTestingUtility.getConfiguration(), "REGULAR_TABLE1".getBytes());
		htable2 = hbaseTestingUtility.createTable("REGULAR_TABLE2".getBytes(), DEFAULT_FAMILY.getBytes());
		tm = new TransactionManager(hbaseTestingUtility.getConfiguration(), txnTable);
		zkw = HBaseTestingUtility.getZooKeeperWatcher(hbaseTestingUtility);
		rzk = zkw.getRecoverableZooKeeper();
	}
	
	@BeforeClass
	public static void setTest() throws Exception {
		System.out.println(hbaseTestingUtility.getClusterTestDir());
		basicConf();
		loadTxnCoprocessor();
		start();
		txnTable = hbaseTestingUtility.createTable(TxnConstants.TRANSACTION_TABLE_BYTES, TxnConstants.TRANSACTION_TABLE_BYTES);
		HTableDescriptor desc = new HTableDescriptor("REGULAR_TABLE1");
		desc.addFamily(new HColumnDescriptor(DEFAULT_FAMILY));
		desc.addFamily(new HColumnDescriptor("FAMILY1"));
		desc.addFamily(new HColumnDescriptor("FAMILY2"));
		desc.addFamily(new HColumnDescriptor("FAMILY3"));
		hbaseTestingUtility.getHBaseAdmin().createTable(desc);
		htable1 = new HTable(hbaseTestingUtility.getConfiguration(), "REGULAR_TABLE1".getBytes());
		htable2 = hbaseTestingUtility.createTable("REGULAR_TABLE2".getBytes(), DEFAULT_FAMILY.getBytes());
		tm = new TransactionManager(hbaseTestingUtility.getConfiguration(), txnTable);
		zkw = HBaseTestingUtility.getZooKeeperWatcher(hbaseTestingUtility);
		rzk = zkw.getRecoverableZooKeeper();
	}
	@AfterClass
	public static void tearDown() throws Exception {
		end();
	}
}

