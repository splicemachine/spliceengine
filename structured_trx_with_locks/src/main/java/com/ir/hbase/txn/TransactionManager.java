package com.ir.hbase.txn;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.zookeeper.RecoverableZooKeeper;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;

import com.ir.constants.SpliceConstants;
import com.ir.constants.TxnConstants;


public abstract class TransactionManager extends TxnConstants {
	
	protected ZooKeeperWatcher zkw;
    protected RecoverableZooKeeper rzk;
	
    @SuppressWarnings(value = "deprecation")
	public TransactionManager(Configuration config) {
		try {
			HBaseAdmin admin = new HBaseAdmin(config);
			if (!admin.tableExists(TxnConstants.TRANSACTION_LOG_TABLE_BYTES)) {
				HTableDescriptor desc = new HTableDescriptor(TxnConstants.TRANSACTION_LOG_TABLE_BYTES);
				desc.addFamily(new HColumnDescriptor(SpliceConstants.DEFAULT_FAMILY.getBytes(),
						SpliceConstants.DEFAULT_VERSIONS,
						SpliceConstants.DEFAULT_COMPRESSION,
						SpliceConstants.DEFAULT_IN_MEMORY,
						SpliceConstants.DEFAULT_BLOCKCACHE,
						SpliceConstants.DEFAULT_TTL,
						SpliceConstants.DEFAULT_BLOOMFILTER));
				desc.addFamily(new HColumnDescriptor(TxnConstants.DEFAULT_FAMILY));
				admin.createTable(desc);
			}
			zkw = admin.getConnection().getZooKeeperWatcher();
			rzk = zkw.getRecoverableZooKeeper();
		} catch (MasterNotRunningException e) {
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public abstract TransactionState beginTransaction() throws KeeperException, InterruptedException, IOException, ExecutionException;

	public abstract int prepareCommit(final TransactionState transactionState) throws KeeperException, InterruptedException, IOException;

	public abstract void doCommit(final TransactionState transactionState) throws KeeperException, InterruptedException, IOException;

	public abstract void tryCommit(final TransactionState transactionState) throws IOException, KeeperException, InterruptedException;

	public abstract void abort(final TransactionState transactionState) throws IOException, KeeperException, InterruptedException;
	
	public abstract JtaXAResource getXAResource();

}
