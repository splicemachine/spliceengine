package com.splicemachine.hbase.txn.coprocessor.region;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.KeyValueScanner;
import org.apache.hadoop.hbase.regionserver.ScanQueryMatcher;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.zookeeper.RecoverableZooKeeper;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooDefs;

import com.splicemachine.constants.TransactionStatus;
import com.splicemachine.constants.TxnConstants;
import com.splicemachine.constants.environment.EnvUtils;
import com.splicemachine.hbase.locks.TxnLockManager;
import com.splicemachine.hbase.txn.logger.LogConstants.LogRecordType;
import com.splicemachine.hbase.txn.logger.TxnLogger;
import com.splicemachine.utils.SpliceLogUtils;

public class TransactionState {
	private static Logger LOG = Logger.getLogger(TransactionState.class);
	private String transactionID;
	private String path; //path down to region id 
	private List<Delete> deletes = new LinkedList<Delete>();
	private RecoverableZooKeeper rzk;
	private Map<String, TransactionState> transactionStateByID;
	private List<WriteAction> writeOrdering = new LinkedList<WriteAction>();
	private HRegion region;
	private String regionId;
	private String txnLogNodePath;
	private HTablePool tablePool;
	private TxnLockManager lockManager;

	public TransactionState(final String transactionID, HRegion region, RecoverableZooKeeper rzk, TxnLockManager lockManager, Map<String, TransactionState> transactionStateByID, HTablePool tablePool, String txnLogPath, boolean isRecovered) {
		SpliceLogUtils.debug(LOG,"New regional transaction state with ID " + transactionID);			
		
		this.transactionStateByID = transactionStateByID;
		this.transactionID = transactionID;
		this.region = region;
		this.txnLogNodePath = TxnLogger.getTxnLogNodePath(txnLogPath, transactionID);
		this.regionId = EnvUtils.getRegionId(region);
		this.tablePool = tablePool;
		this.rzk = rzk;
		this.lockManager = lockManager;
		
		try {
			path = TxnUtils.getTransactionalRegionPath(transactionID, regionId);
			if (!isRecovered) { 
				SpliceLogUtils.debug(LOG,"Creating node " + path);
				path = rzk.create(path, Bytes.toBytes(TransactionStatus.PENDING.toString()), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
				TransactionStatus idNodeStatus = TransactionStatus.valueOf(Bytes.toString(rzk.getData(transactionID, new TxnIdWatcher(), null)));
				if(idNodeStatus.equals(TransactionStatus.PREPARE_COMMIT))
					prepareCommit();
			} else {
				SpliceLogUtils.debug(LOG,"recovering node " + path);
				if (getStatus().equals(TransactionStatus.PREPARE_COMMIT) || getStatus().equals(TransactionStatus.ABORT)) {
					rzk.getData(transactionID, new TxnIdWatcher(), null);
				} else {
					throw new RuntimeException("Imcomplete txn log read from Log Talbe for transaction state " + transactionID + " in region " + regionId);
				}
			}
		} catch (KeeperException.NoNodeException e) {
			LOG.info("Given transaction ID was not found.");
		} catch (KeeperException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	private class TxnIdWatcher implements Watcher {
		@Override
		public void process(WatchedEvent event) {
			SpliceLogUtils.debug(LOG,"Transaction state node watcher is triggered, transaction ID " + transactionID 
						+ " in table " + region.getTableDesc().getNameAsString());
			if (event.getType().equals(EventType.NodeDataChanged)) {
				if (nodeExist()) {
					TransactionStatus status = getStatus(); //regional transaction id node status
					SpliceLogUtils.debug(LOG,"Transaction state node watcher is triggered, transaction ID " + transactionID+",status="+status);
					TransactionStatus txnStatus = null;
					try {
						txnStatus = TransactionStatus.valueOf(Bytes.toString(rzk.getData(transactionID, this, null)));
					} catch (KeeperException e) {
						e.printStackTrace();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					SpliceLogUtils.debug(LOG,"Transaction state node watcher is triggered, transaction ID " + transactionID+",txnStatus="+txnStatus
							+",status="+status);
					switch(txnStatus) {
					case PREPARE_COMMIT:
						if(status.equals(TransactionStatus.PENDING))
							prepareCommit();
						else
							throw new RuntimeException("Status transaction not supported, " + " from " + status.toString() + " to PREPARE_COMMIT");
						break;
					case DO_COMMIT:
						if(status.equals(TransactionStatus.PREPARE_COMMIT))
							doCommit();
						else
							throw new RuntimeException("Status transaction not supported, " + " from " + status.toString() + " to DO_COMMIT");
						break;
					case ABORT:
						switch(status) {
						case PENDING:
						case PREPARE_COMMIT:
						case DO_COMMIT:
							abort();
							break;
						case ABORT:
							break;
						default:
							throw new RuntimeException("Status transaction not supported, " + " from " + status.toString() + " to ABORT");
						}
						break;
					case PENDING:
						setStatus(TransactionStatus.PENDING);
						break;
					default:
						throw new RuntimeException("Status transaction not supported, " + txnStatus);
					}
				} else {
					try {//retire watcher
						SpliceLogUtils.debug(LOG,"retire watcher transaction ID " + transactionID);
						rzk.getData(transactionID, false, null);
					} catch (KeeperException e) {
						e.printStackTrace();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			}
		}
	}

	protected void prepareCommit() {
		SpliceLogUtils.debug(LOG,"Prepare commit, transaction ID " + transactionID);
		try {
			HTableInterface txnLogTable = tablePool.getTable(TxnConstants.TRANSACTION_LOG_TABLE_BYTES);
			TxnLogger.logWriteActions(txnLogTable, LogRecordType.TXN_LOG, null, null, writeOrdering);
			tablePool.close();
		} catch (IOException e) {
			SpliceLogUtils.error(LOG,"Failed to log write action to prepare commit.", e);
			return;
		}
		setStatus(TransactionStatus.PREPARE_COMMIT);
		TxnLogger.createLogNode(txnLogNodePath, rzk, new byte[0], CreateMode.PERSISTENT);
	}

	protected void doCommit() {
		SpliceLogUtils.debug(LOG, "Do commit, transaction ID " + transactionID+",hasWrite="+hasWrite());
		if (hasWrite()) {
			try {
				for (WriteAction action : writeOrdering) {
					executeAction(action, region);
				}
			} catch (IOException e){
				setStatus(TransactionStatus.ABORT);
				e.printStackTrace();
				LOG.info("Failed to perform write action to do commit.");
				return;
			}
		}
		setStatus(TransactionStatus.DO_COMMIT);
		retire();
		TxnLogger.removeLogNode(txnLogNodePath, rzk);
	}

	protected void executeAction(WriteAction action, HRegion region) throws IOException {
		Put put = action.getPut();
		if (null != put) {
			put.setAttribute(TxnConstants.TRANSACTION_ID, null); //prevent trigger observer once more
			SpliceLogUtils.debug(LOG,"Execute put action with row key " + Bytes.toString(put.getRow()) + " on region " + regionId);
			region.put(put, true);
		}

		Delete delete = action.getDelete();
		if (null != delete) {
			delete.setAttribute(TxnConstants.TRANSACTION_ID, null); //prevent trigger observer once more
			SpliceLogUtils.debug(LOG,"Execute delete action with row key " + Bytes.toString(delete.getRow()) + " on region " + regionId);
			region.delete(delete, null, true);
		}
	}

	protected boolean hasWrite() {
		return writeOrdering.size() > 0;
	}

	protected void abort() {
		SpliceLogUtils.debug(LOG,"Abort, transaction ID " + transactionID);
		retire();
	}

	protected void retire() {
		SpliceLogUtils.debug(LOG,"Retire, transaction "+transactionID+" in region " + regionId + " " + path);
		
		lockManager.releaseExclusiveLocks(transactionID);
		transactionStateByID.remove(transactionID);
	}

	public void addWrite(final Put write) {
		SpliceLogUtils.debug(LOG,"Add write to transaction state with transaction ID " + transactionID);
		updateLatestTimestamp(write.getFamilyMap().values(), EnvironmentEdgeManager.currentTimeMillis());
		writeOrdering.add(new WriteAction(write, region, transactionID));
	}

	static void updateLatestTimestamp(final Collection<List<KeyValue>> kvsCollection, final long time) {
		byte[] timeBytes = Bytes.toBytes(time);
		for (List<KeyValue> kvs : kvsCollection) {
			for (KeyValue kv : kvs) {
				if (kv.isLatestTimestamp()) {
					kv.updateLatestStamp(timeBytes);
				}
			}
		}
	}

	public void addDelete(final Delete delete) {
		SpliceLogUtils.debug(LOG,"Add delete to transaction state with transaction ID " + transactionID);
		long now = EnvironmentEdgeManager.currentTimeMillis();
		updateLatestTimestamp(delete.getFamilyMap().values(), now);
		if (delete.getTimeStamp() == HConstants.LATEST_TIMESTAMP) {
			delete.setTimestamp(now);
		}
		deletes.add(delete);
		writeOrdering.add(new WriteAction(delete, region, transactionID));
	}

	public void applyDeletes(final List<KeyValue> input, final long minTime, final long maxTime) {
		if (deletes.isEmpty()) {
			return;
		}
		for (Iterator<KeyValue> itr = input.iterator(); itr.hasNext();) {
			KeyValue included = applyDeletes(itr.next(), minTime, maxTime);
			if (null == included) {
				itr.remove();
			}
		}
	}

	KeyValue applyDeletes(final KeyValue kv, final long minTime, final long maxTime) {
		if (deletes.isEmpty()) {
			return kv;
		}
		for (Delete delete : deletes) {
			// Skip if delete should not apply
			if (!Bytes.equals(kv.getRow(), delete.getRow()) || kv.getTimestamp() > delete.getTimeStamp()
					|| delete.getTimeStamp() > maxTime || delete.getTimeStamp() < minTime) {
				continue;
			}
			// Whole-row delete
			if (delete.isEmpty()) {
				return null;
			}

			for (Entry<byte[], List<KeyValue>> deleteEntry : delete.getFamilyMap().entrySet()) {
				byte[] family = deleteEntry.getKey();
				if (!Bytes.equals(kv.getFamily(), family)) {
					continue;
				}
				List<KeyValue> familyDeletes = deleteEntry.getValue();

				for (KeyValue keyDeletes : familyDeletes) {
					byte[] deleteQualifier = keyDeletes.getQualifier();
					byte[] kvQualifier = kv.getQualifier();
					if (keyDeletes.getTimestamp() > kv.getTimestamp() && (keyDeletes.isDeleteFamily() || Bytes.equals(deleteQualifier, kvQualifier))) {
						return null;
					}
				}
			}
		}

		return kv;
	}

	@Override
	public String toString() {
		StringBuilder result = new StringBuilder();
		result.append("[transactionId: ");
		result.append(transactionID);
		result.append(" status: ");
		try {
			result.append(Bytes.toString(rzk.getData(transactionID, false, null)));
		} catch (KeeperException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		result.append(" region: ");
		result.append(region);
		result.append(" status: ");
		try {
			result.append(Bytes.toString(rzk.getData(path, false, null)));
		} catch (KeeperException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}        result.append(" write Size: ");
		result.append(writeOrdering.size());
		return result.toString();
	}

	KeyValueScanner getScanner(final Scan scan) {
		return new TransactionScanner(scan);
	}

	private KeyValue[] getAllKVs(final Scan scan) {
		SpliceLogUtils.debug(LOG,"Get all KVs from write actions, transaction ID " + transactionID);

		List<KeyValue> kvList = new ArrayList<KeyValue>();

		for (WriteAction action : writeOrdering) {
			byte[] row = action.getRow();
			if (scan.getStartRow() != null && !Bytes.equals(scan.getStartRow(), HConstants.EMPTY_START_ROW)
					&& Bytes.compareTo(row, scan.getStartRow()) < 0) {
				continue;
			}
			if (scan.getStopRow() != null && !Bytes.equals(scan.getStopRow(), HConstants.EMPTY_END_ROW)
					&& Bytes.compareTo(row, scan.getStopRow()) > 0) {
				continue;
			}
			List<KeyValue> kvs = action.getKeyValues();
			if (kvs.isEmpty())
				continue;
			Map<byte [], NavigableSet<byte []>> familyMap = scan.getFamilyMap();
			if (familyMap.isEmpty()) {
				kvList.addAll(kvs);
			} else {
				for (KeyValue kv : kvs) {
					for (Entry<byte[], NavigableSet<byte[]>> familyEntry : familyMap.entrySet()) {
						byte[] family = familyEntry.getKey();
						if (!Bytes.equals(kv.getFamily(), family))
							continue;
						NavigableSet<byte[]> familySet = familyEntry.getValue();
						if (familySet == null || familySet.isEmpty()) {
							kvList.add(kv);
							break;
						}
						for (byte[] qualifier : familySet) {
							if (!Bytes.equals(kv.getQualifier(), qualifier))
								continue;
							kvList.add(kv);
						}
						break;
					}
				}
			}
		}
		return kvList.toArray(new KeyValue[kvList.size()]);
	}

	private int getTransactionSequenceIndex(final KeyValue kv) {
		for (int i = 0; i < writeOrdering.size(); i++) {
			WriteAction action = writeOrdering.get(i);
			if (isKvInPut(kv, action.getPut())) {
				return i;
			}
			if (isKvInDelete(kv, action.getDelete())) {
				return i;
			}
		}
		throw new IllegalStateException("Can not find kv in transaction writes");
	}

	private boolean isKvInPut(final KeyValue kv, final Put put) {
		if (null != put) {
			for (List<KeyValue> putKVs : put.getFamilyMap().values()) {
				for (KeyValue putKV : putKVs) {
					if (putKV == kv) {
						return true;
					}
				}
			}
		}
		return false;
	}

	private boolean isKvInDelete(final KeyValue kv, final Delete delete) {
		if (null != delete) {
			for (List<KeyValue> putKVs : delete.getFamilyMap().values()) {
				for (KeyValue deleteKv : putKVs) {
					if (deleteKv == kv) {
						return true;
					}
				}
			}
		}
		return false;
	}

	/**
	 * Scanner of the puts and deletes that occur during this transaction.
	 * 
	 * @author clint.morgan
	 */
	private class TransactionScanner extends KeyValueListScanner implements InternalScanner {

		private ScanQueryMatcher matcher;

		TransactionScanner(final Scan scan) {
			super(new KeyValue.KVComparator() {

				@Override
				public int compare(final KeyValue left, final KeyValue right) {
					int result = super.compare(left, right);
					if (result != 0) {
						return result;
					}
					if (left == right) {
						return 0;
					}
					int put1Number = getTransactionSequenceIndex(left);
					int put2Number = getTransactionSequenceIndex(right);
					return put2Number - put1Number;
				}
			}, getAllKVs(scan));

			// We want transaction scanner to always take priority over store
			// scanners.
			setSequenceID(Long.MAX_VALUE);

			matcher = new ScanQueryMatcher(scan, null, null, HConstants.FOREVER, KeyValue.KEY_COMPARATOR,
					scan.getMaxVersions());
		}

		/**
		 * Get the next row of values from this transaction.
		 * 
		 * @param outResult
		 * @param limit
		 * @return true if there are more rows, false if scanner is done
		 */
		@Override
		public synchronized boolean next(final List<KeyValue> outResult, final int limit) throws IOException {
			KeyValue peeked = this.peek();
			if (peeked == null) {
				close();
				return false;
			}
			matcher.setRow(peeked.getRow());
			KeyValue kv;
			List<KeyValue> results = new ArrayList<KeyValue>();
			LOOP: while ((kv = this.peek()) != null) {
				ScanQueryMatcher.MatchCode qcode = matcher.match(kv);
				switch (qcode) {
				case INCLUDE:
					KeyValue next = this.next();
					results.add(next);
					if (limit > 0 && results.size() == limit) {
						break LOOP;
					}
					continue;

				case DONE:
					// copy jazz
					outResult.addAll(results);
					return true;

				case DONE_SCAN:
					close();

					// copy jazz
					outResult.addAll(results);

					return false;

				case SEEK_NEXT_ROW:
					this.next();
					break;

				case SEEK_NEXT_COL:
					this.next();
					break;

				case SKIP:
					this.next();
					break;

				default:
					throw new RuntimeException("UNEXPECTED");
				}
			}

			if (!results.isEmpty()) {
				// copy jazz
				outResult.addAll(results);
				return true;
			}

			// No more keys
			close();
			return false;
		}

		@Override
		public boolean next(final List<KeyValue> results) throws IOException {
			return next(results, -1);
		}

	}

	public String getTransactionID() {
		return transactionID;
	}

	public void setTransactionID(String transactionID) {
		this.transactionID = transactionID;
	}

	public boolean nodeExist() {
		if (path != null) {
			try {
				return rzk.exists(path, false) != null;
			} catch (KeeperException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		return false;
	}

	//this regional transaction state status
	public TransactionStatus getStatus() {
		TransactionStatus status = null;
		if (nodeExist()) {
			try {
				status = TransactionStatus.valueOf(Bytes.toString(rzk.getData(path, false, null)));
			} catch (KeeperException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		return status;
	}

	protected void setStatus(TransactionStatus status) {
		SpliceLogUtils.debug(LOG,"Set status to " + status.toString() + ", transaction ID " + transactionID + " in table " + region.getTableDesc().getNameAsString());
		if (nodeExist()) {
			try {
				rzk.setData(path, Bytes.toBytes(status.toString()), -1);
			} catch (KeeperException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	public List<Delete> getDeletes() {
		return deletes;
	}

	public void setDeletes(List<Delete> deletes) {
		this.deletes = deletes;
	}

	public String getPath() {
		return transactionID;
	}

	public HRegion getRegion() {
		return region;
	}

	public RecoverableZooKeeper getRecoverableZooKeeper() {
		return rzk;
	}

	public void setTransactionStateByID(Map<String, TransactionState> transactionStateByID) {
		this.transactionStateByID = transactionStateByID;
	}

	public enum SplitPointPosition {
		IN_STATE_RANGE,
		BEFORE_STATE_RANGE,
		AFTER_STATE_RANGE
	}

	public SplitPointPosition getSplitPointPosition(byte[] splitPoint) {
		byte[][] range = getWriteActionRange();
		if (Bytes.compareTo(range[0], splitPoint) <= 0) {
			if (Bytes.compareTo(splitPoint, range[1]) <= 0) {
				return SplitPointPosition.IN_STATE_RANGE;
			} else {
				return SplitPointPosition.AFTER_STATE_RANGE;
			}
		} else {
			return SplitPointPosition.BEFORE_STATE_RANGE;
		}
	}

	public byte[][] getWriteActionRange() {
		Iterator<WriteAction> iter = writeOrdering.iterator();
		byte[][] range = new byte[2][];
		if (iter.hasNext()) {
			range[0] = range[1] = iter.next().getRow();
			while (iter.hasNext()) {
				byte[] key = iter.next().getRow();
				if (Bytes.compareTo(key,range[0]) < 0) {
					range[0] = key;
				} else if (Bytes.compareTo(range[1], key) < 0) {
					range[1] = key;
				}
			}
		} else {
			throw new RuntimeException("Ordered write map is empty.");
		}
		return range;
	}

	public List<WriteAction> getWriteOrdering() {
		return writeOrdering;
	}

	public void recreateDeleteList() {
		if (!deletes.isEmpty())
			deletes.clear();
		if (hasWrite()) {
			for (WriteAction action : writeOrdering) {
				Delete delete = action.getDelete();
				if (null != delete) {
					deletes.add(delete);
				}
			}
		}
	}
}
