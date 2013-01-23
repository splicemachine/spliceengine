package com.ir.hbase.coprocessor;

import java.io.IOException;

import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.zookeeper.RecoverableZooKeeper;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;

import com.ir.constants.SchemaConstants;

public class BaseZkRegionObserver extends BaseRegionObserver {
	private static Logger LOG = Logger.getLogger(BaseZkRegionObserver.class);
	protected HRegion region;
	protected RecoverableZooKeeper rzk;
	protected ZooKeeperWatcher zkw;
	protected String tableName;
	protected String schemaPath;
	
	protected void initZooKeeper(CoprocessorEnvironment e) throws IOException {
		if (LOG.isDebugEnabled())
			LOG.debug("Intialize ZooKeeper in " + BaseZkRegionObserver.class);
		rzk = ((RegionCoprocessorEnvironment) e).getRegionServerServices().getZooKeeper().getRecoverableZooKeeper();
		zkw = ((RegionCoprocessorEnvironment) e).getRegionServerServices().getZooKeeper();
		tableName = ((RegionCoprocessorEnvironment) e).getRegion().getTableDesc().getNameAsString();
		schemaPath = e.getConfiguration().get(SchemaConstants.SCHEMA_PATH_NAME,SchemaConstants.DEFAULT_SCHEMA_PATH);
		if (schemaPath == null)
			throw new IOException("Schema Path Not Set in Configuration for " + SchemaConstants.SCHEMA_PATH_NAME);
		try {
			if (rzk.exists(schemaPath, false) == null)
				throw new IOException("Schema Not Not Created in ZooKeeper for " + SchemaConstants.SCHEMA_PATH_NAME + ". Check if Index Master Observer is running.");
		} catch (KeeperException e1) {
			e1.printStackTrace();
		} catch (InterruptedException e1) {
			e1.printStackTrace();
		}
		region = ((RegionCoprocessorEnvironment) e).getRegion();
	}
	
	public HRegion getRegion() {
		return region;
	}
	public ZooKeeperWatcher getZooKeeperWatcher() {
		return zkw;
	}
	public RecoverableZooKeeper getRecoverableZooKeeper() {
		return rzk;
	}
	public String getSchemaPath() {
		return schemaPath;
	}
	public String getTableName() {
		return tableName;
	}
	
}
