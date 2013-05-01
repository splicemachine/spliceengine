package com.splicemachine.derby.utils;

import org.apache.hadoop.hbase.zookeeper.RecoverableZooKeeper;
import org.junit.Test;
import org.junit.Assert;
import com.splicemachine.constants.SpliceConstants;

public class ZKUtilsTest {

	@Test
	public void cleanZookeeperTest() throws Exception {
		ZkUtils.cleanZookeeper();
		RecoverableZooKeeper rzk = ZkUtils.getRecoverableZooKeeper();
		for (String path: SpliceConstants.zookeeperPaths) {
			Assert.assertNull(rzk.exists(path, false));
		}
	}
	
	@Test
	public void initialZookeeperTest() throws Exception {
		ZkUtils.initializeZookeeper();		
		RecoverableZooKeeper rzk = ZkUtils.getRecoverableZooKeeper();
		for (String path: SpliceConstants.zookeeperPaths) {
			Assert.assertNotNull(rzk.exists(path, false));
		}
	}
	
	@Test
	public void refreshZookeeperTest() throws Exception {
		ZkUtils.refreshZookeeper();		
		RecoverableZooKeeper rzk = ZkUtils.getRecoverableZooKeeper();
		for (String path: SpliceConstants.zookeeperPaths) {
			Assert.assertNotNull(rzk.exists(path, false));
		}
	}
	
}
