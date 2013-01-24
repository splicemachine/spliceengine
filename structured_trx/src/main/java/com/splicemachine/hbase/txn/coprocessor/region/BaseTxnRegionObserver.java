package com.splicemachine.hbase.txn.coprocessor.region;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.zookeeper.RecoverableZooKeeper;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;

import com.splicemachine.constants.TxnConstants;
import com.splicemachine.constants.environment.EnvUtils;
import com.splicemachine.hbase.locks.TxnLockManager;
import com.splicemachine.hbase.txn.logger.LogConstants;
import com.splicemachine.hbase.txn.logger.TxnLogger;

public class BaseTxnRegionObserver extends BaseRegionObserver {
	private static final Log LOG = LogFactory.getLog(BaseTxnRegionObserver.class);
	protected RecoverableZooKeeper rzk;
	protected ZooKeeperWatcher zkw;
	protected TxnLockManager lockManager;
	protected String transactionPath;
	protected String tableName;
	protected HRegion region;
	protected String regionId;
	protected String regionLogPath;
	protected String splitLogPath;
	protected String txnLogPath;
	
	protected void initZooKeeper(CoprocessorEnvironment e) throws IOException {
		if (LOG.isDebugEnabled()) 
			LOG.debug("Initialize ZooKeep in " + BaseTxnRegionObserver.class);
		rzk = ((RegionCoprocessorEnvironment) e).getRegionServerServices().getZooKeeper().getRecoverableZooKeeper();
		zkw = ((RegionCoprocessorEnvironment) e).getRegionServerServices().getZooKeeper();
		lockManager = new TxnLockManager(e.getConfiguration().getLong(TxnConstants.TRANSACTION_LOCK_TIMEOUT_ATTRIBUTE, TxnConstants.TRANSACTION_LOCK_TIMEOUT));
		region = ((RegionCoprocessorEnvironment) e).getRegion();
		regionId = EnvUtils.getRegionId(region);
		tableName = region.getTableDesc().getNameAsString();
		transactionPath = e.getConfiguration().get(TxnConstants.TRANSACTION_PATH_NAME,TxnConstants.DEFAULT_TRANSACTION_PATH);
		String logPath =  e.getConfiguration().get(LogConstants.LOG_PATH_NAME,LogConstants.DEFAULT_LOG_PATH);
		if (logPath == null)
			throw new IOException("Log Path Not Set in Configuration for " + LogConstants.LOG_PATH_NAME);
		regionLogPath = logPath + LogConstants.REGION_LOG_SUBPATH;
		splitLogPath = TxnLogger.getSplitLogPath(logPath, tableName);
		txnLogPath = TxnLogger.getTxnLogPath(logPath, regionId);
		if (transactionPath == null) {
			LOG.error("Transaction Path Not Set in Configuration for " + TxnConstants.TRANSACTION_PATH_NAME);
			throw new IOException("Transaction Path Not Set in Configuration for " + TxnConstants.TRANSACTION_PATH_NAME);
		}
		try {
			ZKUtil.createWithParents(zkw, regionLogPath);
			ZKUtil.createWithParents(zkw, splitLogPath);
		} catch (KeeperException e1) {
			e1.printStackTrace();
		}
	}
	
	public HRegion getRegion() {
		return region;
	}
	
	public String getTransactionPath() {
		return transactionPath;
	}
	
	public ZooKeeperWatcher getZooKeeperWatcher() {
		return zkw;
	}

}
