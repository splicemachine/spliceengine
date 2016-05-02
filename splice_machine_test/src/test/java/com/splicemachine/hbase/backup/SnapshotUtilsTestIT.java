
package com.splicemachine.hbase.backup;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.splicemachine.test.SlowTest;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.HFileLink;
import org.apache.hadoop.hbase.master.snapshot.SnapshotManager;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.snapshot.SnapshotTestingUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.hbase.HBaseSupport;
import com.splicemachine.hbase.HBaseSupportFactory;
import org.junit.experimental.categories.Category;

/**
 * Test clone snapshots from the client
 */
@Category(SlowTest.class)
@Ignore("DB-5047: attempts to start mini hbase cluster during setup, which fails test in cdh5.4.10.")
public class SnapshotUtilsTestIT {
  final Log LOG = LogFactory.getLog(getClass());

  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  private final byte[] FAMILY = Bytes.toBytes("cf");

  private byte[] snapshotName;
  
  private byte[] snapshotNameAfterSplit;
  
  private byte[] tableName;
  private HBaseAdmin admin;
  private HBaseSupport support;
  
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.getConfiguration().setBoolean(SnapshotManager.HBASE_SNAPSHOT_ENABLED, true);
    TEST_UTIL.getConfiguration().setBoolean("hbase.online.schema.update.enable", true);
    TEST_UTIL.getConfiguration().set(HConstants.HBASE_REGION_SPLIT_POLICY_KEY,
    		"org.apache.hadoop.hbase.regionserver.ConstantSizeRegionSplitPolicy");
    // Create multiple regions
    TEST_UTIL.getConfiguration().setLong("hbase.hregion.max.filesize", 100000000); // 100M
    // -> MapR work-around
    TEST_UTIL.getConfiguration().set(FileSystem.FS_DEFAULT_NAME_KEY,"file:///");
    TEST_UTIL.getConfiguration().set("fs.default.name", "file:///");
    TEST_UTIL.getConfiguration().set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
    System.setProperty("zookeeper.sasl.client", "false");   
    System.setProperty("zookeeper.sasl.serverconfig", "fake");
    // <- MapR work-around
    TEST_UTIL.startMiniCluster(1);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

 
  @Before
  public void setup() throws Exception {
    this.admin = TEST_UTIL.getHBaseAdmin();

    long tid = System.currentTimeMillis();
    tableName = Bytes.toBytes("testtb-" + tid);
    snapshotName = Bytes.toBytes("snaptb0-" + tid);
    snapshotNameAfterSplit = Bytes.toBytes("snaptb-split-"+ tid);
    support = HBaseSupportFactory.support;
    // create Table
    support.createTable(TEST_UTIL, tableName, FAMILY);

    try(HTable table = support.newTable(TEST_UTIL.getConfiguration(),tableName)) {
		SnapshotTestingUtils.loadData(TEST_UTIL,table.getName(),500000,FAMILY);
//      SnapshotTestingUtils.loadData(TEST_UTIL, table, 500000, FAMILY);
      // take a snapshot
      support.snapshot(admin, snapshotName, tableName);
      Path rootDir = FSUtils.getRootDir(TEST_UTIL.getConfiguration());      
      LOG.info("Root: "+ rootDir);
      SpliceConstants.config = TEST_UTIL.getConfiguration();
    }
  }

  private String getAsString(byte[] arr)
  {
	  return new String(arr);
  }
  
  @Test 
  public void getFilesForFullBackupTest() throws IOException
  {
	  LOG.info("Test get files for full backup ");

	  SnapshotUtils utils = SnapshotUtilsFactory.snapshotUtils;
	  // Get list of all regions
	  List<HRegion> regionList = support.getRegions(TEST_UTIL.getHBaseCluster(), tableName);
	  FileSystem fs = TEST_UTIL.getTestFileSystem();
	  LOG.info("Total regions: "+ regionList.size());
	  LOG.info("Root Dir: "+ TEST_UTIL.getDefaultRootDirPath());
	  for(HRegion region: regionList ){
		  LOG.info("Region: "+region);
		  List<Object> files = utils.getFilesForFullBackup(getAsString(snapshotName), region);
		  for(Object p: files){
			  Path pa = null;
			  if( p instanceof Path){
				  pa = (Path) p;
			  } else{
				  pa = ((HFileLink) p).getAvailablePath(fs);
			  }
			  LOG.info("-- "+pa);
		  }
		  // Get list of store files
		  List<Path> storeFileList = getStoreFileList(region);
		  assertTrue(compare(fs, files, storeFileList));
	  }
	  
	  LOG.info("Test get files for full backup finished ");
  
  }
  
  
  //@Test 
  public void getFilesForFullBackupAfterSplitTest() throws IOException, InterruptedException
  {
	  LOG.info("Test get files for full backup after split ");
      
	  splitTableAndSnapshot();
      FileSystem fs = TEST_UTIL.getTestFileSystem();
	  
	  SnapshotUtils utils = SnapshotUtilsFactory.snapshotUtils;
	  // Get list of all regions
	  List<HRegion> regionList = support.getRegions(TEST_UTIL.getHBaseCluster(),tableName);
	  LOG.info("Total regions: "+ regionList.size());
	  for(HRegion region: regionList ){
		  LOG.info("Region offline ["+region.getRegionInfo().isOffline()+"] : "+region);
		  List<Object> files = utils.getFilesForFullBackup(getAsString(snapshotNameAfterSplit), region);
		  for(Object p: files){
			  Path pa = null;
			  if( p instanceof Path){
				  pa = (Path) p;
			  } else{
				  pa = ((HFileLink) p).getAvailablePath(fs);
			  }
			  LOG.info("-- "+pa);
		  }
		  // Get list of store files
		  //List<Path> storeFileList = getStoreFileList(region);
		  //assertTrue(compare(files, storeFileList));
	  }
	  
	  LOG.info("Test get files for full backup after split finished ");
  
  }
  
  private void splitTableAndSnapshot() throws IOException, InterruptedException {
	HBaseAdmin admin = TEST_UTIL.getHBaseAdmin();
	List<HRegion> regionList = support.getRegions(TEST_UTIL.getHBaseCluster(),tableName);
	admin.split(regionList.get(0).getRegionName());
	
	Thread.sleep(100);
	support.snapshot(admin, snapshotNameAfterSplit, tableName);
	
  }

private List<Path> getStoreFileList(HRegion region) {
	Map<byte[], Store> storeMap = region.getStores();
	List<Path> files = new ArrayList<Path>();
	for(Store store: storeMap.values()){
		for(StoreFile sf: store.getStorefiles()){
			files.add(sf.getPath());
		}
	}
	return files;
}

@SuppressWarnings("unchecked")
private boolean compare(FileSystem fs, List<Object> files, List<Path> storeFileList) throws IOException {
	if(files.size() != storeFileList.size()) return false;
	List<Path> pfiles = new ArrayList<Path>();
	for(Object link: files)
	{
		Path p = null;
		if( link instanceof Path){
			p = (Path) link;
		} else{
			p = ((HFileLink) link).getAvailablePath(fs);
		}
		pfiles.add(p);
	}
	
	Collections.sort(pfiles);
	Collections.sort(storeFileList);
	for(int i=0; i < pfiles.size(); i++){
		Path one = pfiles.get(i);
		Path two = storeFileList.get(i);
		if(one.compareTo(two) != 0){
			return false;
		}
	}
	
	return true;
}

@After
  public void tearDown() throws Exception {
    if (support.tableExists(admin, tableName)) {
      support.deleteTable(TEST_UTIL, tableName);
    }
    SnapshotTestingUtils.deleteAllSnapshots(admin);
    SnapshotTestingUtils.deleteArchiveDirectory(TEST_UTIL);
  }


}
