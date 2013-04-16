package com.splicemachine.hbase.txn.coprocessor.region;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Attributes;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.KeyValueScanner;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.RegionScannerUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.RecoverableZooKeeper;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZKUtil;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooDefs.Ids;

import com.splicemachine.constants.SchemaConstants;
import com.splicemachine.hbase.txn.TransactionStatus;
import com.splicemachine.hbase.txn.TxnConstants;
import com.splicemachine.constants.environment.EnvUtils;
import com.splicemachine.hbase.locks.TxnLockManager;
import com.splicemachine.hbase.txn.TxnCoordinator2;
import com.splicemachine.hbase.txn.logger.LogConstants.LogRecordType;
import com.splicemachine.hbase.txn.logger.TxnLogger;
import com.splicemachine.utils.SpliceLogUtils;

public class TxnUtils extends TxnConstants{
	
	private static Logger LOG = Logger.getLogger(TxnUtils.class);

	public static boolean isTransactional(Attributes attributableOperation) {
		String txnId = getTransactionID(attributableOperation);
		if (txnId != null)
			return true;
		else 
			return false;
	}

	public static boolean txnIdExist(String txnId, RecoverableZooKeeper rzk) {
		try {
			return rzk.exists(txnId, false) != null;
		} catch (KeeperException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return false;
	}

	public static String getTransactionalRegionPath(String txnId, String regionId) {
		return txnId + SchemaConstants.PATH_DELIMITER + regionId;
	}

	public static String getTransactionID(Attributes attributableOperation) {
		byte[] value = attributableOperation.getAttribute(TRANSACTION_ID);
		return (value == null) ? null : Bytes.toString(value);
	}

	public static TableEnv getTableEnv(RegionCoprocessorEnvironment e) {
		return EnvUtils.getTableEnv(e.getRegion().getTableDesc().getNameAsString());
	}

	public static TransactionIsolationLevel getIsolationLevel(Attributes attributableOperation) {
		byte[] value = attributableOperation.getAttribute(TRANSACTION_ISOLATION_LEVEL);
		return (value == null) ? null : TransactionIsolationLevel.valueOf(Bytes.toString(value));
	}

	public static String getLockType(Attributes attributableOperation) {
		byte[] value = attributableOperation.getAttribute(LOCK_TYPE);
		return (value == null) ? null : Bytes.toString(value);
	}

	public static void editGetResult(Get get, HRegion region, List<KeyValue> results, RecoverableZooKeeper rzk, TxnLockManager lockManager, 
			Map<String, TransactionState> transactionStateByID, HTablePool tablePool, ObserverContext<RegionCoprocessorEnvironment> e, 
			String txnLogPath) throws IOException {
		SpliceLogUtils.trace(LOG,"Edite Get resutlt based on Isolation Level");
		TransactionIsolationLevel isoLevel = TxnUtils.getIsolationLevel(get);
		if (isoLevel == null) isoLevel = TransactionIsolationLevel.READ_UNCOMMITED;
		String transactionID = TxnUtils.getTransactionID(get);
		switch (isoLevel) {
		case READ_UNCOMMITED:
			if (!transactionStateByID.containsKey(transactionID))
				transactionStateByID.put(transactionID, new TransactionState(transactionID, region, rzk, lockManager, transactionStateByID, tablePool, txnLogPath, false));
			Scan scan = wrapWithDeleteActionFilter(new Scan(get), transactionStateByID.get(transactionID));
			List<KeyValueScanner> addScanners = new ArrayList<KeyValueScanner>(1);
			addScanners.add(transactionStateByID.get(transactionID).getScanner(scan));
			RegionScannerUtil.getRegionScanner(region, scan, addScanners).next(results);
			e.bypass();
			break;
		case READ_COMMITTED:
			if (!lockManager.acquireSharedReadLock(get.getRow()))
				setAbort(transactionID, rzk);
			break;
		case REPEATABLE_READ:
			if (!lockManager.acquireExclusiveReadLock(get.getRow(), transactionID))
				setAbort(transactionID, rzk);
			break;
		case SERIALIZABLE:
			if (!lockManager.acquireExclusiveReadLock(get.getRow(), transactionID))
				setAbort(transactionID, rzk);
			break;
		default:
			throw new RuntimeException("Unknown transaction isolation level.");
		}
	}

	public static Scan wrapWithDeleteActionFilter(final Scan scan, final TransactionState state) {
		SpliceLogUtils.debug(LOG,"Wrap scan with delete action filter.");
		FilterBase deleteFilter = new FilterBase() {
			private boolean rowFiltered = false;
			@Override
			public void reset() {
				rowFiltered = false;
			}
			@Override
			public boolean hasFilterRow() {
				return true;
			}
			@Override
			public void filterRow(final List<KeyValue> kvs) {
				state.applyDeletes(kvs, scan.getTimeRange().getMin(), scan.getTimeRange().getMax());
				rowFiltered = kvs.isEmpty();
			}
			@Override
			public boolean filterRow() {
				return rowFiltered;
			}
			@Override
			public void write(final DataOutput out) throws IOException {
			}
			@Override
			public void readFields(final DataInput in) throws IOException {
			}
		};
		if (scan.getFilter() == null) {
			scan.setFilter(deleteFilter);
			return scan;
		}
		FilterList wrappedFilter = new FilterList(Arrays.asList(deleteFilter, scan.getFilter()));
		scan.setFilter(wrappedFilter);
		return scan;
	}

	public static RegionScanner generateScanner(Scan scan, HRegion region, RecoverableZooKeeper rzk, TxnLockManager lockManager, 
			Map<String, TransactionState> transactionStateByID, HTablePool tablePool, ObserverContext<RegionCoprocessorEnvironment> e, 
			String txnLogPath) throws IOException {
		SpliceLogUtils.debug(LOG,"Generate RegionScanner based on Isolation Level.");
		TransactionIsolationLevel isoLevel = TxnUtils.getIsolationLevel(scan);
		if (isoLevel == null) isoLevel = TransactionIsolationLevel.READ_UNCOMMITED;
		switch (isoLevel) {
		case READ_UNCOMMITED:
			String transactionID = TxnUtils.getTransactionID(scan);
			SpliceLogUtils.debug(LOG,"generateScanner read_uncommited, transactionID="+transactionID);
			boolean exists = true;
			if (!transactionStateByID.containsKey(transactionID)) {
				SpliceLogUtils.debug(LOG,"generateScanner, transactionID not in map");
				exists = false;
				transactionStateByID.put(transactionID, new TransactionState(transactionID, region, rzk, lockManager, 
						transactionStateByID, tablePool, txnLogPath, false));
			}
			scan = TxnUtils.wrapWithDeleteActionFilter(scan, transactionStateByID.get(transactionID));
			List<KeyValueScanner> addScanners = new ArrayList<KeyValueScanner>();
			/*if (!exists) {
				KeyValueScanner kvScanner = null;
				for (String tid : transactionStateByID.keySet()) {
					LOG.info("what's in transactionStateByID, id="+tid);
					kvScanner = transactionStateByID.get(tid).getScanner(scan);
					if (kvScanner != null) {
						LOG.info("add in scanners");
						addScanners.add(kvScanner);
					}
				}
			} else*/
				addScanners.add(transactionStateByID.get(transactionID).getScanner(scan));
			RegionScanner scanner = RegionScannerUtil.getRegionScanner(region, scan, addScanners);
			e.bypass();
			return scanner;
		case READ_COMMITTED:
			break;
		case REPEATABLE_READ:
			break;
		case SERIALIZABLE:
			break;
		default:
			throw new RuntimeException("Unknown transaction isolation level.");
		}
		return null;
	}
	/**
	 * Creates the specified node and all parent nodes required for it to exist.
	 *
	 * No watches are set and no errors are thrown if the node already exists.
	 *
	 * The nodes created are persistent and open access.
	 *
	 * @param zkw zk reference
	 * @param znode path of node
	 * @throws KeeperException if unexpected zookeeper exception
	 */
	public static void createWithParents(ZooKeeperWatcher zkw, String znode)
			throws KeeperException {
		try {
			if(znode == null) {
				return;
			}
			SpliceLogUtils.debug(LOG,"create znode for "+znode);
			zkw.getRecoverableZooKeeper().create(znode, new byte[0], Ids.OPEN_ACL_UNSAFE,
					CreateMode.PERSISTENT);
		} catch(KeeperException.NodeExistsException nee) {
			SpliceLogUtils.debug(LOG, "znode exists: %s",znode);
		} catch(KeeperException.NoNodeException nne) {
			createWithParents(zkw, getParent(znode));
			createWithParents(zkw, znode);
		} catch(InterruptedException ie) {
			zkw.interruptedException(ie);
		}
	}
	
	public static void createWithParents(RecoverableZooKeeper rzk, String znode)
			throws KeeperException {
		try {
			if(znode == null) {
				return;
			}
			rzk.create(znode, new byte[0], Ids.OPEN_ACL_UNSAFE,
					CreateMode.PERSISTENT);
		} catch(KeeperException.NodeExistsException nee) {
			SpliceLogUtils.debug(LOG,"znode exists during createWithParents: "+znode);
			return;
		} catch(KeeperException.NoNodeException nne) {
			createWithParents(rzk, getParent(znode));
			createWithParents(rzk, znode);
		} catch(InterruptedException ie) {
			SpliceLogUtils.error(LOG,ie);
		}
	}

	/**
	 * Returns the full path of the immediate parent of the specified node.
	 * @param node path to get parent of
	 * @return parent of path, null if passed the root node or an invalid node
	 */
	public static String getParent(String node) {
		int idx = node.lastIndexOf(SchemaConstants.PATH_DELIMITER);
		return idx <= 0 ? null : node.substring(0, idx);
	}

	public static String beginTransaction(String transactionPath, ZooKeeperWatcher zkw) {
		SpliceLogUtils.trace(LOG,"Begin transaction at server and create znode for %s",transactionPath);
		String id = null;
		try {
//			TxnUtils.createWithParents(zkw, transactionPath);
			id = zkw.getRecoverableZooKeeper().create(transactionPath + "/txn-", Bytes.toBytes(TransactionStatus.PENDING.toString()), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
			SpliceLogUtils.debug(LOG,"Begin transaction at server and create znode for transId="+id);
		} catch (KeeperException e) {
            if(e.code()== KeeperException.Code.NONODE){
                safeCreateWithParents(zkw, transactionPath);
            }
            throw new RuntimeException(e);
		} catch (InterruptedException e) {
            throw new RuntimeException(e);
		}
		return id;
	}

    private static void safeCreateWithParents(ZooKeeperWatcher zkw, String transactionPath) {
        try{
            createWithParents(zkw,transactionPath);
        } catch (KeeperException e) {
            //we can ignore NODEEXISTS, because someone else created what we did already
            if(e.code()!= KeeperException.Code.NODEEXISTS)
                throw new RuntimeException(e);
        }
    }

    public static void commit(String transactionID, RecoverableZooKeeper rzk) {
		SpliceLogUtils.debug(LOG,"Committing Transaction: " + transactionID);
		prepareCommit(transactionID, rzk);
		doCommit(transactionID, rzk);
	}

	public static void prepareCommit(String transactionID, RecoverableZooKeeper rzk) {
		SpliceLogUtils.debug(LOG,"Prepare commit transaction at server for transaction " + transactionID);
		try {
			rzk.setData(transactionID, Bytes.toBytes(TransactionStatus.PREPARE_COMMIT.toString()), -1);
			TxnCoordinator2 coordinator = new TxnCoordinator2(rzk);
			if (coordinator.watch(transactionID, TransactionStatus.PREPARE_COMMIT)) {
				SpliceLogUtils.info(LOG,"Fail to prepare commit " + transactionID + " set back to pending.");
				rzk.setData(transactionID, Bytes.toBytes(TransactionStatus.PENDING.toString()), -1);
			} else {
				SpliceLogUtils.debug(LOG,"Prepared commit " + transactionID);
			}
		} catch (KeeperException e) {
			SpliceLogUtils.info(LOG, "NoNode error occrres: "+e.getMessage());
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	public static void doCommit(String transactionID, RecoverableZooKeeper rzk) {
		SpliceLogUtils.debug(LOG,"Do commit transaction at server, transactionID="+transactionID);
		try {
			if (TransactionStatus.valueOf(Bytes.toString(rzk.getData(transactionID, false, null))).equals(TransactionStatus.PREPARE_COMMIT)) {
				rzk.setData(transactionID, Bytes.toBytes(TransactionStatus.DO_COMMIT.toString()), -1);				
				TxnCoordinator2 coordinator = new TxnCoordinator2(rzk);
				if (coordinator.watch(transactionID, TransactionStatus.DO_COMMIT)) {
					SpliceLogUtils.info(LOG,"Fail to commit " + transactionID + " set back to pending.");
					rzk.setData(transactionID, Bytes.toBytes(TransactionStatus.PENDING.toString()), -1);
				} else {
					SpliceLogUtils.debug(LOG,"Committed " + transactionID+" and its children. delete the nodes as well");
					/*List<String> tree = ZKUtil.listSubTreeBFS(rzk.getZooKeeper(), transactionID);
					LOG.info("Committed " + transactionID+",firstnode="+tree.get(0)+",size of nodes"+tree.size());
					if (tree.size() <= 1)
						return;
					for (int i = tree.size() - 2; i >= 0 ; --i) {
						LOG.info("committing & deleting the node - "+tree.get(i));
						//Delete the leaves nodes
						rzk.delete(tree.get(i), -1); 
					}*/
					ZKUtil.deleteRecursive(rzk.getZooKeeper(), transactionID);
				}
			} else {
				throw new RuntimeException("Transaction id " + transactionID + " has not been prepared commit.");
			}
		} catch (KeeperException e) {
			SpliceLogUtils.error(LOG,e);
			//e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	public static void abort(String transactionID, RecoverableZooKeeper rzk) {
		SpliceLogUtils.debug(LOG,"Abort transaction at " + transactionID);
		try {
			rzk.setData(transactionID, Bytes.toBytes(TransactionStatus.ABORT.toString()), -1);
			/*List<String> tree = ZKUtil.listSubTreeBFS(rzk.getZooKeeper(), transactionID);
			LOG.debug("aborting " + transactionID+",firstnode="+tree.get(0)+",size of nodes"+tree.size());
			if (tree == null || tree.size() <= 1)
				return;
			for (int i = tree.size() - 1; i >= 0 ; --i) {
				LOG.info("aborting the node - "+tree.get(i));
				//Delete the leaves nodes
				//rzk.delete(tree.get(i), -1); 
			}*/
			
			ZKUtil.deleteRecursive(rzk.getZooKeeper(), transactionID);
		} catch (KeeperException e) {
			SpliceLogUtils.debug(LOG,"in abort, node has already been deleted so no need to delete it again-"+transactionID);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Remove states that would be irrelevant to old region after splitting from transactionStateByID. Generate a new map for the new region.
	 * @param transactionStateByID
	 * @return mapFroNewRegion
	 * @throws IOException 
	 */
	public static void splitAndLogTransactionStateMap(byte[] startKey, byte[] splitPoint, byte[] endKey, Map<String, TransactionState> transactionStateByID, HTablePool tablePool) throws IOException {
		if (transactionStateByID != null) {
			HTableInterface txnLogTable = tablePool.getTable(TxnConstants.TRANSACTION_LOG_TABLE_BYTES);
			try {
				for (Map.Entry<String, TransactionState> entry : transactionStateByID.entrySet()) {
					String id = entry.getKey();
					TransactionState state = entry.getValue();
					switch (state.getSplitPointPosition(splitPoint)) {
					case IN_STATE_RANGE: 
						SpliceLogUtils.info(LOG,"Split point in the range of transaction state " + id + " in region " + EnvUtils.getRegionId(state.getRegion()));
						List<WriteAction> left = new LinkedList<WriteAction>(state.getWriteOrdering());
						List<WriteAction> right = new LinkedList<WriteAction>();
						splitWriteActionList(splitPoint, left, right);
						try {
							TxnLogger.logWriteActions(txnLogTable, LogRecordType.SPLIT_LOG, startKey, splitPoint, left);
							TxnLogger.logWriteActions(txnLogTable, LogRecordType.SPLIT_LOG, splitPoint, endKey, right);
						} catch (IOException e) {
							SpliceLogUtils.error(LOG,"Fail to log write action for Split.",e);
						}
						break;
					case BEFORE_STATE_RANGE:
						SpliceLogUtils.info(LOG,"Split point before the range of transaction state " + id + " in region " + EnvUtils.getRegionId(state.getRegion()));
						try {
							TxnLogger.logWriteActions(txnLogTable, LogRecordType.SPLIT_LOG, splitPoint, endKey, state.getWriteOrdering());
						} catch (IOException e) {
							SpliceLogUtils.error(LOG,"Fail to log write action for Split.",e);
						}
						break;
					case AFTER_STATE_RANGE:
						SpliceLogUtils.debug(LOG,"Split point after the range of transaction state " + id + " in region " + EnvUtils.getRegionId(state.getRegion()));
						try {
							TxnLogger.logWriteActions(txnLogTable, LogRecordType.SPLIT_LOG, startKey, splitPoint, state.getWriteOrdering());
						} catch (IOException e) {
							SpliceLogUtils.debug(LOG,"Fail to log write action for Split.",e);
						}
						break;
					default:
						throw new RuntimeException("Not supported split point position.");
					}
					state.retire();
				}
			} finally {
				txnLogTable.close();
			}
		}
	}

	/**
	 * split writeAction list. move part of write actions from left to right.
	 */
	public static void splitWriteActionList(byte[] splitPoint, List<WriteAction> left, List<WriteAction> right) {
		Iterator<WriteAction> iter = left.iterator();
		while (iter.hasNext()) {
			WriteAction action = iter.next();
			if (isAfterSplitPoint(splitPoint, action)) {
				iter.remove();
				right.add(action);
			}
		}
	}

	public static boolean isAfterSplitPoint(byte[] splitPoint, WriteAction action) {
		return Bytes.compareTo(splitPoint, action.getRow()) <= 0;
	}	
	
	public static void setAbort(String transactionID, RecoverableZooKeeper rzk) {
		try {
			rzk.setData(transactionID, TransactionStatus.ABORT.toString().getBytes(), -1);
		} catch (KeeperException e1) {
			e1.printStackTrace();
		} catch (InterruptedException e1) {
			e1.printStackTrace();
		}
	}
	
	public static boolean isAbort(String transactionID, RecoverableZooKeeper rzk) {
		try {
			return TransactionStatus.valueOf(Bytes.toString(rzk.getData(transactionID, false, null))).equals(TransactionStatus.ABORT);
		} catch (KeeperException.NoNodeException nne) {
			
		} catch (KeeperException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return true;
	}
}