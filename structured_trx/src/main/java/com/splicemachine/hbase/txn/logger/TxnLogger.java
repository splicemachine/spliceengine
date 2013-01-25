package com.splicemachine.hbase.txn.logger;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.catalog.CatalogTracker;
import org.apache.hadoop.hbase.catalog.MetaReader;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryPrefixComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.RecoverableZooKeeper;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;

import com.splicemachine.constants.HBaseConstants;
import com.splicemachine.constants.SchemaConstants;
import com.splicemachine.constants.TxnConstants;
import com.splicemachine.constants.bytes.BytesUtil;
import com.splicemachine.constants.environment.EnvUtils;
import com.splicemachine.hbase.locks.TxnLockManager;
import com.splicemachine.hbase.txn.coprocessor.region.TransactionState;
import com.splicemachine.hbase.txn.coprocessor.region.WriteAction;
import com.splicemachine.utils.SpliceLogUtils;


public class TxnLogger extends LogConstants {
	
	private static Logger LOG = Logger.getLogger(TxnLogger.class);
	
	/**
	 * split key could be null for txn log.
	 */
	public static void logWriteActions(HTableInterface txnTable, LogRecordType logType, byte[] beginKey, byte[] endKey, List<WriteAction> writeActions) throws IOException {
		SpliceLogUtils.debug(LOG,"Logging a new write action list from transaction state");
		List<Put> logPuts = new ArrayList<Put>();
		for (int sequenceNum = 0; sequenceNum < writeActions.size(); ++sequenceNum) {
			Put put = writeActions.get(sequenceNum).generateLogPut(logType, beginKey, endKey, sequenceNum);
			logPuts.add(put);
		}
		SpliceLogUtils.info(LOG,"Generated " + logPuts.size() + " puts to be operated on table " + txnTable.getTableDescriptor().getNameAsString());
		txnTable.put(logPuts);
	}

	/**
	 * Read transaction states of one region from log. For split log use, logTyep indicates left or right split record as for split log. 
	 * Result is stored in the Map parameter.
	 */
	public static void readTransactionStatesLog(HTableInterface txnTable, LogRecordType logType, String tableName, String regionID, 
			byte[] beginKey, byte[] endKey, HRegion holder, RecoverableZooKeeper rzk, TxnLockManager lockManager, 
			Map<String, TransactionState> transactionStateByID, HTablePool tablePool, String txnLogPath, boolean isRecovered) 
					throws IOException {
			
		SpliceLogUtils.debug(LOG,"readTransactionStatesLog");
		try {
			Scan scan = new Scan();
			if (logType.equals(LogRecordType.SPLIT_LOG))
				scan.setFilter(new RowFilter(CompareFilter.CompareOp.EQUAL, new BinaryPrefixComparator(TxnLogger.getSplitLogScanKey(logType, tableName, beginKey, endKey, null))));
			else if (logType.equals(LogRecordType.TXN_LOG))
				scan.setFilter(new RowFilter(CompareFilter.CompareOp.EQUAL, new BinaryPrefixComparator(TxnLogger.getTxnLogScanKey(tableName, regionID, null))));
			else
				throw new RuntimeException("Unsupported log type " + logType + " when trying to get scan key");
			SpliceLogUtils.debug(LOG,"Read txn states for region " +  EnvUtils.getRegionId(holder) + ", using prefix filter");
			ResultScanner scanner = txnTable.getScanner(scan);
			Result result = null;
			String priorID = null;
			String currentID = null;
			TransactionState transactionState = null;
			while ((result = scanner.next()) != null) {
				currentID = Bytes.toString(result.getColumnLatest(HBaseConstants.DEFAULT_FAMILY_BYTES, TXN_ID_COLUMN_BYTES).getValue());
				if (currentID != null && !currentID.equals(priorID)) {
					SpliceLogUtils.debug(LOG,"Read transaction state " + currentID + " from logger and hold it in new region " + EnvUtils.getRegionId(holder));
					priorID = currentID;
					transactionState = new TransactionState(currentID, holder, rzk, lockManager, transactionStateByID, tablePool, txnLogPath, isRecovered);
					transactionStateByID.put(currentID, transactionState);
				}
				getWriteActionFromResult(result, currentID, holder, transactionState.getWriteOrdering());
			}
			for (TransactionState ts : transactionStateByID.values()) {
				ts.recreateDeleteList();
			}
		} finally {
			txnTable.close();
		}
	}

	/**
	 * Actions would be kept in the List parameter. 
	 */
	public static void getWriteActionFromResult(Result result, String transactionID, HRegion actionHolder, List<WriteAction> writeOrdering) throws IOException {
		SpliceLogUtils.debug(LOG,"getWriteActionFromResult, result= " +  result);
		byte[] row = result.getColumnLatest(HBaseConstants.DEFAULT_FAMILY_BYTES, ROW_KEY_BYTES).getValue();
		WriteActionType actionType = WriteActionType.valueOf(Bytes.toString(result.getColumnLatest(HBaseConstants.DEFAULT_FAMILY_BYTES, ACTION_TYPE_BYTES).getValue()));
		byte[] action = result.getColumnLatest(HBaseConstants.DEFAULT_FAMILY_BYTES, ACTION_WRITABLE_BYTE).getValue();
		ByteArrayInputStream istream = new ByteArrayInputStream(action);
		DataInput in = new DataInputStream(istream);
		switch (actionType) {
		case PUT:
			Put put = new Put(row);
			put.readFields(in);
			writeOrdering.add(new WriteAction(put, actionHolder, transactionID));
			break;
		case DELETE:
			Delete delete = new Delete(row);
			delete.readFields(in);
			writeOrdering.add(new WriteAction(delete, actionHolder, transactionID));
			break;
		default:
			throw new RuntimeException("Unsupported write action from transaction logger.");
		}
	}

	public static List<HRegionInfo> readTxnTableRegionsFromZk(String regionLogPath, RecoverableZooKeeper rzk) {
		SpliceLogUtils.debug(LOG,"Read txn table region info from zookeeper");
		List<HRegionInfo> infos = new ArrayList<HRegionInfo>();
		try {
			if (rzk.exists(regionLogPath, false) != null && !rzk.getChildren(regionLogPath, false).isEmpty()) {
				for (String regionId : rzk.getChildren(regionLogPath, false)) {
					byte[] infoBytes = rzk.getData(getRegionLogPath(regionLogPath, regionId), false, null);
					ByteArrayInputStream istream = new ByteArrayInputStream(infoBytes);
					DataInput in = new DataInputStream(istream);
					HRegionInfo info = new HRegionInfo();
					info.readFields(in);
					infos.add(info);
				}
			}
		} catch (KeeperException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return infos;
	}

	public static String getRegionLogPath(String regionLogPath, String regionId) {
		return regionLogPath + SchemaConstants.PATH_DELIMITER + regionId;
	}

	public static void logTxnTableRegionsToZk(String regionLogPath, ZooKeeperWatcher zkw, Configuration conf) {
		SpliceLogUtils.debug(LOG,"logTxnTableRegionsToZk,regionLogPath="+regionLogPath);
		try {
			HBaseAdmin admin = new HBaseAdmin(conf);
			RecoverableZooKeeper rzk = zkw.getRecoverableZooKeeper();
			if (admin.tableExists(TxnConstants.TRANSACTION_LOG_TABLE_BYTES)) {
				List<HRegionInfo> infos = admin.getTableRegions(TxnConstants.TRANSACTION_LOG_TABLE_BYTES);
				if (infos != null) {
					if (rzk.exists(regionLogPath, false) != null)
						ZKUtil.deleteNodeRecursively(zkw, regionLogPath);
					ZKUtil.createWithParents(zkw, regionLogPath);
					for (HRegionInfo info : infos) {
						ByteArrayOutputStream ostream = new ByteArrayOutputStream();
						DataOutput out = new DataOutputStream(ostream);
						info.write(out);
						rzk.create(getRegionLogPath(regionLogPath, EnvUtils.getRegionId(info.getRegionNameAsString())), ostream.toByteArray(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
					}
				}
			} else {
				SpliceLogUtils.info(LOG,"in TxnLogger's logTxnTableRegionsToZk,transaction table does not exist");
			}
		} catch (KeeperException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (MasterNotRunningException e) {
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static byte[] getTxnLogScanKey(String tableName, String regionID, String transactionID) {
		SpliceLogUtils.debug(LOG,"getTxnLogScanKey, tableName="+tableName+",regionID="+regionID+",transactionID="+transactionID);
		List<byte[]> components = new ArrayList<byte[]>();
		components.add(Bytes.toBytes(LogRecordType.TXN_LOG.toString()));
		components.add(LogConstants.LOG_DELIMITER_BYTES);
		components.add(tableName.getBytes());
		components.add(LogConstants.LOG_DELIMITER_BYTES);
		components.add(regionID.getBytes());
		if (transactionID != null) {
			components.add(LogConstants.LOG_DELIMITER_BYTES);
			components.add(transactionID.getBytes());
		}
		return BytesUtil.concat(components);
	}

	public static byte[] getSplitLogScanKey(LogRecordType logType, String tableName, byte[] beginKey, byte[] endKey, String transactionID) {
		SpliceLogUtils.debug(LOG,"getSplitLogScanKey, tableName="+tableName+",logType="+logType+",transactionID="+transactionID);
		List<byte[]> components = new ArrayList<byte[]>();
		components.add(Bytes.toBytes(logType.toString()));
		components.add(LogConstants.LOG_DELIMITER_BYTES);
		components.add(tableName.getBytes());
		components.add(LogConstants.LOG_DELIMITER_BYTES);
		components.add(beginKey);
		components.add(LogConstants.LOG_DELIMITER_BYTES);
		components.add(endKey);
		if (transactionID != null) {
			components.add(LogConstants.LOG_DELIMITER_BYTES);
			components.add(transactionID.getBytes());
		}
		return BytesUtil.concat(components);
	}
	/**
	 * Only Used for recovering transaction state when region server crash down and prepare commit has been triggered before that.
	 */
	public static boolean isTxnTableRegionsOnline(List<HRegionInfo> infos, RegionServerServices rss) {
		SpliceLogUtils.debug(LOG,"isTxnTableRegionsOnline");
		for (HRegionInfo info : infos) {
			SpliceLogUtils.debug(LOG,"Checking online status of txn table region " + info.getRegionNameAsString());
			if (rss.getFromOnlineRegions(info.getEncodedName()) == null) {
				SpliceLogUtils.info(LOG,"Txn table region " + info.getRegionNameAsString() + " is not online.");
				return false;
			}
		}
		return true;
	}
	
	public static void scheduleReadingTxnLog(final HTableInterface txnLogTable, final RegionServerServices rss, String regionLogPath, final String txnLogPath, final String tableName, final String regionId, final HRegion region, final RecoverableZooKeeper rzk, final TxnLockManager lockManager, final Map<String, TransactionState> transactionStateByID, final HTablePool tablePool) {
		SpliceLogUtils.debug(LOG,"scheduleReadingTxnLog, tableName="+tableName);
		final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
		final List<HRegionInfo> infos = readTxnTableRegionsFromZk(regionLogPath, rzk);
		final ScheduledFuture<?> handler = scheduler.scheduleWithFixedDelay(new Runnable() {
			@Override
			public void run() {
				SpliceLogUtils.debug(LOG,"scheduleReadingTxnLog ticking...");
				if (isTxnTableRegionsOnline(infos, rss)) {
					SpliceLogUtils.debug(LOG,"Txn table regions are online, start reading log.");
					try {
						TxnLogger.readTransactionStatesLog(txnLogTable, LogRecordType.TXN_LOG, tableName, regionId, null, null, region, rzk, lockManager, transactionStateByID, tablePool, txnLogPath, true);
					} catch (IOException e) {
						SpliceLogUtils.error(LOG,"Fail to read txn log for table " + tableName,e);
					} finally {
						scheduler.shutdownNow();
					}
				}
			}
		}, 1000, 1000, TimeUnit.MILLISECONDS);
		scheduler.schedule(new Runnable() {
			@Override
			public void run() {
				handler.cancel(true);
			}
		}, 30, TimeUnit.SECONDS);
	}

	public static String getTxnLogPath(String logPath, String regionId) {
		return logPath + LogConstants.TXN_LOG_SUBPATH + SchemaConstants.PATH_DELIMITER + regionId;
	}
	
	public static String getTxnLogNodePath(String txnLogPath, String txnId) {
		return txnLogPath + SchemaConstants.PATH_DELIMITER + txnId.replace("/", "-");
	}

	public static String getSplitLogPath(String logPath, String tableName) {
		return logPath + LogConstants.SPLIT_LOG_SUBPATH + SchemaConstants.PATH_DELIMITER + tableName;
	}

	public static boolean logNodeExist(String path, RecoverableZooKeeper rzk) {
		try {
			return rzk.exists(path, null) != null;
		} catch (KeeperException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return false;
	}

	public static void removeLogNode(String path, RecoverableZooKeeper rzk) {
		SpliceLogUtils.debug(LOG,"TxnLogger, removeLogNode for "+path);
		try {
			rzk.delete(path, -1);
		} catch (KeeperException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	public static byte[] composeSplitLogNodeData(byte[] startKey, byte[] endKey) {
		SpliceLogUtils.debug(LOG,"composeSplitLogNodeData");
		byte[] bytes = new byte[startKey.length + LOG_NODE_DELIMITER.length + endKey.length];
		System.arraycopy(startKey, 0, bytes, 0, startKey.length);
		System.arraycopy(LOG_NODE_DELIMITER, 0, bytes, startKey.length, LOG_NODE_DELIMITER.length);
		System.arraycopy(endKey, 0, bytes, startKey.length+LOG_NODE_DELIMITER.length, endKey.length);
		return bytes;
	}

	public static String isSplitGenerated(String splitLogNodePath, byte[] startKey, byte[] endKey, RecoverableZooKeeper rzk) {
		SpliceLogUtils.debug(LOG,"isSplitGenerated");
		String splitRegionPath = null;
		byte[] bytes = composeSplitLogNodeData(startKey, endKey);
		try {
			SpliceLogUtils.debug(LOG,"Get children from split node path: " + splitLogNodePath);
			for (String child : rzk.getChildren(splitLogNodePath, false)) {
				String path = splitLogNodePath + SchemaConstants.PATH_DELIMITER + child;
				byte[] data = rzk.getData(path, false, null);
				SpliceLogUtils.debug(LOG,"Read data from split log node: " + path +  "; data: " + Bytes.toString(data));
				if (Bytes.compareTo(data, bytes) == 0) {
					SpliceLogUtils.debug(LOG,"split log node data compare equal.");
					splitRegionPath = path;
					break;
				}
			}
		} catch (KeeperException.NoNodeException nne) {
			LOG.info("Split node doesn't exist; path: " + splitLogNodePath);
		} catch (KeeperException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return splitRegionPath;
	}

	public static String createLogNode(String path, RecoverableZooKeeper rzk, byte[] status, CreateMode createMode) {
		SpliceLogUtils.debug(LOG,"createLogNode path="+path+",createMode="+createMode);
		try {
			return rzk.create(path, status, Ids.OPEN_ACL_UNSAFE,
					createMode);
		} catch (KeeperException.NodeExistsException ne) {
			SpliceLogUtils.debug(LOG,"Log Node " + path + " already exists.");
		} catch (KeeperException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return null;
	}

	public static int getTxnLogChildNum(String TxnLogPath, RecoverableZooKeeper rzk) {
		SpliceLogUtils.debug(LOG,"getTxnLogChildNum TxnLogPath="+TxnLogPath);
		try {
			return rzk.getChildren(TxnLogPath, false).size();
		} catch (KeeperException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return -1;
	}

	public static List<HRegionInfo> getTxnTableOfflineRegions(Configuration conf) throws IOException {
		SpliceLogUtils.debug(LOG,"getTxnTableOfflineRegions ");
		final List<HRegionInfo> regions = new ArrayList<HRegionInfo>();
		MetaReader.Visitor visitor = new MetaReader.Visitor() {
			@Override
			public boolean visit(Result r) throws IOException {
				if (r == null || r.isEmpty()) return true;
				HRegionInfo info = MetaReader.parseHRegionInfoFromCatalogResult(
						r, HConstants.REGIONINFO_QUALIFIER);
				SpliceLogUtils.debug(LOG,"Get region info from .META. " + info.getRegionNameAsString());
				if (info == null) return true; // Keep scanning
				if (info.getTableNameAsString().equals(TxnConstants.TRANSACTION_LOG_TABLE) && info.isOffline()) {
					SpliceLogUtils.debug(LOG,"Get txn table offline region " + EnvUtils.getRegionId(info.getRegionNameAsString()));
					regions.add(info);
				}
				// Returning true means "keep scanning"
				return true;
			}
		};
		// Run full scan of .META. catalog table passing in our custom visitor
		MetaReader.fullScan(new CatalogTracker(conf), visitor);
		// Now work on our list of found parents. See if any we can clean up.
		return regions;
	}

}
