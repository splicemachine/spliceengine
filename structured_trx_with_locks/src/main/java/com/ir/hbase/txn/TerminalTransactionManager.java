package com.ir.hbase.txn;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.zookeeper.KeeperException;
import com.ir.constants.TxnConstants;
import com.ir.hbase.txn.TransactionState;

public class TerminalTransactionManager extends TransactionManager {
    static final Log LOG = LogFactory.getLog(TerminalTransactionManager.class);
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
    public TransactionState beginTransaction() throws KeeperException, InterruptedException, IOException, ExecutionException {
    	if (LOG.isDebugEnabled()) 
    		LOG.debug("Begin transaction.");
    	return new TransactionState(transactionTable.get(new Get(INITIALIZE_TRANSACTION_ID_BYTES)).getRow());
    }
   
    public int prepareCommit(final TransactionState transactionState) throws KeeperException, InterruptedException, IOException {
    	if (LOG.isDebugEnabled()) 
    		LOG.debug("Do prepareCommit on " + transactionState.getTransactionID());
    	transactionTable.put(getPrepareCommit(transactionState));
    	return 0;
     }

    public void doCommit(final TransactionState transactionState) throws KeeperException, InterruptedException, IOException  {
    	if (LOG.isDebugEnabled()) 
    		LOG.debug("Do commit on " + transactionState.getTransactionID());
    	transactionTable.put(getDoCommit(transactionState));
    }

    public void tryCommit(final TransactionState transactionState) throws IOException, KeeperException, InterruptedException {
    	if (LOG.isDebugEnabled()) 
    		LOG.debug("Try commit on " +transactionState.getTransactionID());
       	prepareCommit(transactionState);
       	doCommit(transactionState);
    }
    
    public void abort(final TransactionState transactionState) throws IOException, KeeperException, InterruptedException {
    	if (LOG.isDebugEnabled()) 
    		LOG.debug("Abort on " +transactionState.getTransactionID());
    	transactionTable.put(getAbortCommit(transactionState));
     }

    public synchronized JtaXAResource getXAResource() {
        if (xAResource == null) {
            xAResource = new JtaXAResource(this);
        }
        return xAResource;
    }
    
    public static Put getPrepareCommit(TransactionState transactionState) {
    	if (LOG.isDebugEnabled()) 
    		LOG.debug("Generate prepare commit put on " + transactionState.getTransactionID());
		Put put = new Put(Bytes.toBytes(transactionState.getTransactionID()));
		put.add(TRANSACTION_TABLE_PREPARE_FAMILY_BYTES, TRANSACTION_QUALIFIER, Bytes.toBytes(transactionState.getTransactionID()));
		return put;
	}
	
	public static Put getDoCommit(TransactionState transactionState) {
    	if (LOG.isDebugEnabled()) 
    		LOG.debug("Generate do commit on " + transactionState.getTransactionID());
		Put put = new Put(Bytes.toBytes(transactionState.getTransactionID()));
		put.add(TRANSACTION_TABLE_DO_FAMILY_BYTES, TRANSACTION_QUALIFIER, Bytes.toBytes(transactionState.getTransactionID()));
		return put;
	}
	
	public static Put getAbortCommit(TransactionState transactionState) {
    	if (LOG.isDebugEnabled()) 
    		LOG.debug("Generate abort commit put on " + transactionState.getTransactionID());
		Put put = new Put(Bytes.toBytes(transactionState.getTransactionID()));
		put.add(TRANSACTION_TABLE_ABORT_FAMILY_BYTES, TRANSACTION_QUALIFIER, Bytes.toBytes(transactionState.getTransactionID()));
		return put;
	}
	
}