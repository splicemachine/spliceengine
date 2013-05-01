package com.ir.hbase.coprocessor;

import java.io.IOException;

import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.BaseMasterObserver;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.zookeeper.RecoverableZooKeeper;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;

import com.ir.constants.SpliceConstants;

public class BaseZkMasterObserver extends BaseMasterObserver {
	private static Logger LOG = Logger.getLogger(BaseZkMasterObserver.class);
	protected RecoverableZooKeeper rzk;
	protected ZooKeeperWatcher zkw;
	protected String schemaPath;
	
	protected void initZooKeeper(CoprocessorEnvironment ctx) throws IOException {
		if (LOG.isDebugEnabled())
			LOG.debug("Initialize ZooKeeper in " + BaseZkMasterObserver.class);
		zkw = ((MasterCoprocessorEnvironment) ctx).getMasterServices().getZooKeeper();
		rzk = zkw.getRecoverableZooKeeper();
		schemaPath = ctx.getConfiguration().get(SpliceConstants.SCHEMA_PATH_NAME,SpliceConstants.DEFAULT_SCHEMA_PATH);
		if (schemaPath == null)
			throw new IOException("Schema Path Not Set in Configuration for " + SpliceConstants.SCHEMA_PATH_NAME);
		try { //create schema root path if not exist
			if (rzk.exists(schemaPath, false) == null)
				SchemaUtils.createWithParents(rzk, schemaPath);
		} catch (KeeperException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	public ZooKeeperWatcher getZooKeeperWatcher() {
		return zkw;
	}
	public RecoverableZooKeeper getRecoverableZooKeeper() {
		return rzk;
	}
}
