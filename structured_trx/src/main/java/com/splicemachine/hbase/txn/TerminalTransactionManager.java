package com.splicemachine.hbase.txn;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.RecoverableZooKeeper;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import com.splicemachine.utils.SpliceLogUtils;

public class TerminalTransactionManager extends TransactionManager {
	private static final Logger LOG = Logger.getLogger(TerminalTransactionManager.class);
    protected JtaXAResource xAResource;
    protected String transactionPath;
    protected Configuration conf;
    protected HTableInterface transactionTable;
    
    public TerminalTransactionManager(Configuration conf) throws InterruptedException, IOException, ExecutionException, KeeperException {
    	super(conf);
    	this.conf = conf;
    	HBaseAdmin admin = new HBaseAdmin(conf);
    	if (!admin.tableExists(TRANSACTION_TABLE_BYTES)) {
    		HTableDescriptor desc = new HTableDescriptor(TRANSACTION_TABLE_BYTES);
    		desc.addFamily(new HColumnDescriptor(TRANSACTION_TABLE_ABORT_FAMILY_BYTES));
    		desc.addFamily(new HColumnDescriptor(TRANSACTION_TABLE_PREPARE_FAMILY_BYTES));
    		desc.addFamily(new HColumnDescriptor(TRANSACTION_TABLE_DO_FAMILY_BYTES));
    		admin.createTable(new HTableDescriptor(TRANSACTION_TABLE_BYTES));
    	}
    	this.transactionTable = new HTable(conf, TRANSACTION_TABLE_BYTES);
    }
    public TerminalTransactionManager(final Configuration conf, final HTable transactionTable) {
    	super(conf);
    	this.conf = conf;
	    this.transactionTable = transactionTable;
    }

    @Override
    public  TransactionState beginTransaction(boolean allowWrites, boolean nested, boolean dependent, String parentTransactionID)
            throws KeeperException, InterruptedException, IOException, ExecutionException {
    	SpliceLogUtils.debug(LOG,"Begin transaction.");
    	return new TransactionState(transactionTable.get(new Get(INITIALIZE_TRANSACTION_ID_BYTES)).getRow());
    }
   
    public int prepareCommit(final TransactionState transactionState) throws KeeperException, InterruptedException, IOException {
    	SpliceLogUtils.debug(LOG,"Do prepareCommit on " + transactionState.getTransactionID());
    	transactionTable.put(getPrepareCommit(transactionState));
    	return 0;
     }

    @Override
    public void prepareCommit2(Object bonus, TransactionState transactionState) throws KeeperException, InterruptedException, IOException {
        RecoverableZooKeeper recoverableZooKeeper = (RecoverableZooKeeper) bonus;
        recoverableZooKeeper.setData(transactionState.getTransactionID(), Bytes.toBytes(TransactionStatus.PREPARE_COMMIT.toString()), -1);
    }

    public void doCommit(final TransactionState transactionState) throws KeeperException, InterruptedException, IOException  {
    	if (LOG.isDebugEnabled())
    		LOG.debug("Do commit on " + transactionState.getTransactionID());
    	transactionTable.put(getDoCommit(transactionState));
    }

    public void tryCommit(final TransactionState transactionState) throws IOException, KeeperException, InterruptedException {
    	SpliceLogUtils.debug(LOG,"Try commit on " +transactionState.getTransactionID());
       	prepareCommit(transactionState);
       	doCommit(transactionState);
    }
    
    public void abort(final TransactionState transactionState) throws IOException, KeeperException, InterruptedException {
    	SpliceLogUtils.debug(LOG,"Abort on " +transactionState.getTransactionID());
    	transactionTable.put(getAbortCommit(transactionState));
     }

    public synchronized JtaXAResource getXAResource() {
        if (xAResource == null) {
            xAResource = new JtaXAResource(this);
        }
        return xAResource;
    }
    
    public static Put getPrepareCommit(TransactionState transactionState) {
    	SpliceLogUtils.debug(LOG,"Generate prepare commit put on " + transactionState.getTransactionID());
		Put put = new Put(Bytes.toBytes(transactionState.getTransactionID()));
		put.add(TRANSACTION_TABLE_PREPARE_FAMILY_BYTES, TRANSACTION_QUALIFIER, Bytes.toBytes(transactionState.getTransactionID()));
		return put;
	}
	
	public static Put getDoCommit(TransactionState transactionState) {
		SpliceLogUtils.debug(LOG,"Generate do commit on " + transactionState.getTransactionID());
		Put put = new Put(Bytes.toBytes(transactionState.getTransactionID()));
		put.add(TRANSACTION_TABLE_DO_FAMILY_BYTES, TRANSACTION_QUALIFIER, Bytes.toBytes(transactionState.getTransactionID()));
		return put;
	}
	
	public static Put getAbortCommit(TransactionState transactionState) {
		SpliceLogUtils.debug(LOG,"Generate abort commit put on " + transactionState.getTransactionID());
		Put put = new Put(Bytes.toBytes(transactionState.getTransactionID()));
		put.add(TRANSACTION_TABLE_ABORT_FAMILY_BYTES, TRANSACTION_QUALIFIER, Bytes.toBytes(transactionState.getTransactionID()));
		return put;
	}
	
}