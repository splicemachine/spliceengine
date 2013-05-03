package com.ir.hbase.txn.coprocessor.region;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.KeyValue;

import com.ir.constants.TxnConstants;
import com.ir.constants.TxnConstants.TableEnv;
import com.ir.constants.TxnConstants.TransactionIsolationLevel;
import com.ir.constants.environment.EnvUtils;
import com.ir.hbase.txn.logger.LogConstants.LogRecordType;
import com.ir.hbase.txn.logger.TxnLogger;

import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Bytes;

public class TransactionalRegionObserver extends BaseTxnRegionObserver {
	private static Logger LOG = Logger.getLogger(TransactionalRegionObserver.class);

	private ConcurrentHashMap<String, TransactionState> transactionStateByID = new ConcurrentHashMap<String, TransactionState>();
//	private HTablePool tablePool;
	private boolean tableEnvMatch = false;

	@Override
	public void start(CoprocessorEnvironment e) throws IOException {
		if (LOG.isDebugEnabled())
			LOG.debug("Starting the CoProcessor " + TransactionalRegionObserver.class + " in table " + tableName);
		tableEnvMatch = TxnUtils.getTableEnv((RegionCoprocessorEnvironment) e).equals(TableEnv.OTHER_TABLES) || TxnUtils.getTableEnv((RegionCoprocessorEnvironment) e).equals(TableEnv.INDEX_TABLE);
		if(!tableEnvMatch) {
			super.start(e);
			return;
		}
		initZooKeeper(e);
		tablePool = new HTablePool(e.getConfiguration(), 1);
		HTableInterface txnLogTable = tablePool.getTable(TxnConstants.TRANSACTION_LOG_TABLE_BYTES);
		if (TxnLogger.txnLogNodeExist(txnLogPath, regionId, rzk)) {
			if (LOG.isDebugEnabled())
				LOG.debug("Reading txn Log for region " + regionId);
			TxnLogger.scheduleReadingTxnLog(txnLogTable, ((RegionCoprocessorEnvironment) e).getRegionServerServices(), regionLogPath, txnLogPath, tableName, regionId, region, rzk, lockManager, transactionStateByID, tablePool);
			txnLogTable.close();
		} else {
			//Read log for split, it has no effect if no log exist in log table
			TxnLogger.readTransactionStatesLog(txnLogTable, LogRecordType.SPLIT_LOG, tableName, null, region.getStartKey(), region.getEndKey(), region, rzk, lockManager, transactionStateByID, tablePool, txnLogPath, false);
		}
		txnLogTable.close();
	}

	@Override
	public void preSplit(ObserverContext<RegionCoprocessorEnvironment> e) {
		if (LOG.isTraceEnabled())
			LOG.trace("preSplit in CoProcessor " + TransactionalRegionObserver.class + " in table " + tableName);
		byte[] splitPoint = e.getEnvironment().getRegion().checkSplit();
		if (LOG.isDebugEnabled())
			LOG.debug("Split point " + Bytes.toString(splitPoint) + " for table " + region.getTableDesc().getNameAsString() + " key range: [" + Bytes.toString(region.getStartKey()) + ", " + Bytes.toString(region.getEndKey()) + "], region id " + regionId);
		if (LOG.isDebugEnabled())
			LOG.debug("Start split and log transaction state map for region " + regionId);
		try {
			TxnUtils.splitAndLogTransactionStateMap(region.getStartKey(), splitPoint, region.getEndKey(), transactionStateByID, tablePool);
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		super.preSplit(e);
	}

	@Override
	public void postSplit(ObserverContext<RegionCoprocessorEnvironment> e, HRegion l, HRegion r) { 
		//Note: the split generate two regions whose ranges are [, split) and [split, ], the split point would be located in the right daughter.
		if (LOG.isTraceEnabled())
			LOG.trace("postSplit in CoProcessor " + TransactionalRegionObserver.class);
		if (LOG.isDebugEnabled()) {
			LOG.debug("Left half region id " + EnvUtils.getRegionId(l) + " key range: [" + Bytes.toString(l.getStartKey()) + ", " + Bytes.toString(l.getEndKey()) + "]");
			LOG.debug("Right half region id " + EnvUtils.getRegionId(r) + " key range: [" + Bytes.toString(r.getStartKey()) + ", " + Bytes.toString(r.getEndKey()) + "]");
			LOG.debug("Old region id " + regionId);
		}
		//Do nothing, only for debug
		super.postSplit(e, l, r);
	}
	@Override
	public void preGet(ObserverContext<RegionCoprocessorEnvironment> e,Get get, List<KeyValue> results) throws IOException {
		if (LOG.isDebugEnabled())
			LOG.debug("preGet in CoProcessor " + TransactionalRegionObserver.class + " in table " + tableName);
		if(!tableEnvMatch) {
			return;
		}
		if (TxnUtils.isTransactional(get)) {
			TxnUtils.editGetResult(get, region, results, rzk, lockManager, transactionStateByID, tablePool, e, txnLogPath);
		}
	}
	
	@Override
	public void postGet(ObserverContext<RegionCoprocessorEnvironment> e, Get get, List<KeyValue> results) throws IOException {
		if (LOG.isDebugEnabled())
			LOG.debug("postGet in CoProcessor " + TransactionalRegionObserver.class + " in table " + tableName);
		if(!tableEnvMatch) {
			return;
		}
		String transactionID = TxnUtils.getTransactionID(get);
		if (transactionID != null && TxnUtils.getIsolationLevel(get).equals(TransactionIsolationLevel.READ_COMMITTED)) {
			lockManager.releaseSharedReadLock(get.getRow());
		}
		super.postGet(e, get, results);
	}

	@Override
	public RegionScanner preScannerOpen(ObserverContext<RegionCoprocessorEnvironment> e, Scan scan, RegionScanner s) throws IOException {
		if (LOG.isDebugEnabled())
			LOG.debug("preScannerOpen in CoProcessor " + TransactionalRegionObserver.class + " in table " + tableName);
		if(!tableEnvMatch) {
			return s;
		}
		if (TxnUtils.isTransactional(scan))
			return TxnUtils.generateScanner(scan, region, rzk, lockManager, transactionStateByID, tablePool, e, txnLogPath);
		return s;
	}

	@Override
	public void prePut(ObserverContext<RegionCoprocessorEnvironment> e,Put put, WALEdit edit, boolean writeToWAL) throws IOException {
		if (LOG.isTraceEnabled())
			LOG.trace("prePut in CoProcessor " + TransactionalRegionObserver.class + " in table " + tableName);
		if(!tableEnvMatch) {
			return;
		}
		if (TxnUtils.isTransactional(put)) { // Removed extra Zookeeper call		
			String transactionID = TxnUtils.getTransactionID(put);
			if (transactionID != null) {
				lockManager.acquireExclusiveWriteLock(put.getRow(), transactionID);
				if (transactionStateByID.containsKey(transactionID)) {
					if (LOG.isDebugEnabled())
						LOG.debug("Transaction state exists in region " + regionId);
					transactionStateByID.get(transactionID).addWrite(put);
				} else {
					if (LOG.isDebugEnabled())
						LOG.debug("Create new transaction state in region " + regionId);
					TransactionState state = new TransactionState(transactionID, region, rzk, lockManager, transactionStateByID, tablePool, txnLogPath, false);
					state.addWrite(put);
					transactionStateByID.put(transactionID, state);
				}
				e.bypass();
			}
		}
	}

	@Override
	public void preDelete(ObserverContext<RegionCoprocessorEnvironment> e,Delete delete, WALEdit edit, boolean writeToWAL) throws IOException {
		if (LOG.isDebugEnabled())
			LOG.debug("preDelete in CoProcessor " + TransactionalRegionObserver.class + " in table " + tableName);
		if(!tableEnvMatch) {
			return;
		}
		if (TxnUtils.isTransactional(delete)) {
			String transactionID = TxnUtils.getTransactionID(delete);
			if (transactionID != null) {
				lockManager.acquireExclusiveWriteLock(delete.getRow(), transactionID);
				if (transactionStateByID.containsKey(transactionID)) {
					transactionStateByID.get(transactionID).addDelete(delete);
				} else {
					TransactionState state = new TransactionState(transactionID, region, rzk, lockManager, transactionStateByID, tablePool, txnLogPath, false);
					state.addDelete(delete);
					transactionStateByID.put(transactionID, state);
				}
				e.bypass();
			}
		}
	}
}
