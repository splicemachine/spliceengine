package com.splicemachine.mrio;


import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSTableDescriptors;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

import com.splicemachine.derby.hbase.SpliceIndexObserver;
import com.splicemachine.hbase.BaseTest;
import com.splicemachine.mrio.api.SpliceClientSideRegionScanner;



// TODO: Auto-generated Javadoc
/**
 * The Class CoprocessorLoadTest.
 */
public class SpliceClientSideRegionScannerTestIT extends BaseTest{

  /** The Constant LOG. */
  static final Log LOG = LogFactory.getLog(SpliceClientSideRegionScannerTestIT.class);
    
  /** The util. */
  private static HBaseTestingUtility UTIL = new HBaseTestingUtility();  
    
  /** The n. */
  static int N = 300000;
      
  /** The cluster. */
  static MiniHBaseCluster cluster;
    
  /** The _table c. */
  static HTable _tableA ;
  
  private static boolean initDone = true;
  
  
  /* (non-Javadoc)
   * @see junit.framework.TestCase#setUp()
   */
  @Override
  public void setUp() throws Exception {
    
    if(initDone) return;
    
    ConsoleAppender console = new ConsoleAppender(); // create appender
    // configure the appender
    String PATTERN = "%d [%p|%c|%C{1}] %m%n";
    console.setLayout(new PatternLayout(PATTERN));
    console.setThreshold(Level.INFO);

    console.activateOptions();
    // add appender to any Logger (here is root)
    Logger.getRootLogger().removeAllAppenders();
    Logger.getRootLogger().addAppender(console);
    Configuration conf = UTIL.getConfiguration();
    conf.set("hbase.zookeeper.useMulti", "false");

    // set coprocessor
    conf.set(CoprocessorHost.USER_REGION_COPROCESSOR_CONF_KEY,SpliceIndexObserver.class.getName());
    
	// Mapr4.0 specific fix
 	// See DB-2859        
	System.setProperty("zookeeper.sasl.client", "false");   
	System.setProperty("zookeeper.sasl.serverconfig", "fake");
	conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem"); 		
	conf.set(FileSystem.FS_DEFAULT_NAME_KEY,"file:///");
    
    UTIL.startMiniCluster(1);
    if( initDone) return;   
    initDone = true;

    cluster = UTIL.getMiniHBaseCluster();
    createTables(VERSIONS);
    createHBaseTables();
        
      
  }
  
  
  /* (non-Javadoc)
   * @see com.inclouds.hbase.test.BaseTest#createTables()
   */

  /**
   * Creates the hbase tables.
   *
   * @throws IOException Signals that an I/O exception has occurred.
   */
  protected void createHBaseTables() throws IOException {   
    Configuration cfg = cluster.getConf();
    HBaseAdmin admin = new HBaseAdmin(cfg);
    if( admin.tableExists(tableA.getName()) == true){
      LOG.info("Deleting table "+tableA);
      admin.disableTable(tableA.getName());
      admin.deleteTable(tableA.getName());
      LOG.info("Deleted table "+tableA);
    }
    admin.createTable(tableA);
    admin.close();
    LOG.info("Created table "+tableA);
    _tableA = new HTable(cfg, TABLE_A);   
    
  }



  /* (non-Javadoc)
   * @see junit.framework.TestCase#tearDown()
   */
  @Override 
  public void tearDown() throws Exception {
      LOG.error("\n Tear Down the cluster and test \n");
    //Thread.sleep(2000);
    //UTIL.shutdownMiniCluster();
  }
    


  /**
   * Put all data.
   *
   * @param table the table
   * @param n the n
   * @throws IOException Signals that an I/O exception has occurred.
   */
  protected void putAllData(HTable table, int n) throws IOException
  {
    LOG.error ("Put all " + n +" rows  starts.");
    table.setAutoFlush(false);
    long start = System.currentTimeMillis();
    for(int i=0; i < n; i++){
      Put put = createPut(generateRowData(i));

      if( i % 10000 == 0){
        System.out.println("put: "+i);
      }
      table.put(put);
    }
    table.flushCommits();
    LOG.error ("Put all " +n +" rows  finished in "+(System.currentTimeMillis() - start)+"ms");
  }
  
  /**
   * Delete all data.
   *
   * @param table the table
   * @param n the n
   * @throws IOException Signals that an I/O exception has occurred.
   */
  protected void deleteAllData(HTable table, int n) throws IOException
  {
    LOG.error ("Delete all " + n +" rows  starts.");
    long start = System.currentTimeMillis();
    for(int i=0; i < n; i++){
      Delete delete = createDelete(getRow(i));
      table.delete(delete);
    }
    table.flushCommits();
    LOG.error ("Delete all " +n +" rows  finished in "+(System.currentTimeMillis() - start)+"ms");
  }

  /**
   * Filter.
   *
   * @param list the list
   * @param fam the fam
   * @param col the col
   * @return the list
   */
  protected List<KeyValue> filter (List<KeyValue> list, byte[] fam, byte[] col)
  {
    List<KeyValue> newList = new ArrayList<KeyValue>();
    for(KeyValue kv: list){
      if(doFilter(kv, fam, col)){
        continue;
      }
      newList.add(kv);
    }
    return newList;
  }
  
  /**
   * Do filter.
   *
   * @param kv the kv
   * @param fam the fam
   * @param col the col
   * @return true, if successful
   */
  private final boolean doFilter(KeyValue kv, byte[] fam, byte[] col){
    if (fam == null) return false;
    byte[] f = kv.getFamily();
    if(Bytes.equals(f, fam) == false) return true;
    if( col == null) return false;
    byte[] c = kv.getQualifier();
    if(Bytes.equals(c, col) == false) return true;
    return false;
  }
  
  /**
   * Dump put.
   *
   * @param put the put
   */
  protected void dumpPut(Put put) {
    Map<byte[], List<Cell>> map = put.getFamilyCellMap();
    for(byte[] row: map.keySet()){
      List<Cell> list = map.get(row);
      for(Cell kv : list){
        LOG.error(kv);
      }
    }
    
  }
    
  public void _testLoadData() throws IOException
  {
    LOG.error("Loading data started ...");
    long startTime = System.currentTimeMillis();
    putAllData(_tableA, N);  
    LOG.error("Loading data finished in: "+(System.currentTimeMillis() - startTime));
    
    // Check MemStore size
    HBaseAdmin admin = UTIL.getHBaseAdmin();  
    

  }
  
  public void _testScan() throws IOException
  {
    LOG.info("Test run sequential scanner ");
    Configuration conf = UTIL.getConfiguration();
    Path rootDir = FSUtils.getRootDir(conf);
    FileSystem fs = rootDir.getFileSystem(conf);
    String tableName = _tableA.getName().getNameAsString();
    // load table descriptor
    HTableDescriptor htd = 
        FSTableDescriptors.getTableDescriptorFromFs(fs, rootDir, 
          TableName.valueOf(tableName.getBytes()));
    
    int count = 0;
    for(HRegionInfo hri: getActiveRegions()){
      LOG.info("Running scanner for "+ hri);	
      Scan scan = new Scan();
      scan.addFamily(FAMILIES[0]);
      scan.setMaxVersions();
      scan.setCaching(1000);
      LOG.error( new String(hri.getStartKey()) + " : "+ new String(hri.getEndKey()));
      SpliceClientSideRegionScanner scanner = 
    		  new SpliceClientSideRegionScanner(conf, fs, rootDir, htd, hri, scan, null, null);
      
      List<Cell> result = new ArrayList<Cell>();
      while(scanner.next(result) == true){
        count += result.size();
        result.clear();
      }
      count += result.size();
      scanner.close();
    }
    LOG.info("Found "+count+" kvs");
    
  }
  
  public void _testActiveRegions() throws IOException
  {
	    LOG.info("Test active regions ");
	    Configuration conf = new Configuration(UTIL.getConfiguration());
		HConnection connection = HConnectionManager.createConnection(conf);
		HTable table = (HTable) connection.getTable(TABLE_A);
		NavigableMap<HRegionInfo, ServerName> map = table.getRegionLocations();
		for(HRegionInfo hri: map.keySet()){
			LOG.info("Found: "+ hri);
		}
	    LOG.info("Test active regions finished ");
	    table.close();connection.close();

		
  }
  
  private Set<HRegionInfo> getActiveRegions() throws IOException
  {
	    Configuration conf = new Configuration(UTIL.getConfiguration());
		HConnection connection = HConnectionManager.createConnection(conf);
		HTable table = (HTable) connection.getTable(TABLE_A);
		NavigableMap<HRegionInfo, ServerName> map = table.getRegionLocations();		
	    table.close();connection.close();
	    return map.keySet();
  }
  
  public void _testMemStoreScan() throws IOException
  {
    LOG.info("Test run sequential scanner (memory)");
    List<HRegion> regions = cluster.findRegionsForTable(_tableA.getName());
    
    //HRegion region = regions.get(0);
    int count = 0;

    for(HRegion region: regions){
      Scan scan = new Scan();
      scan.addFamily(FAMILIES[0]);
      scan.setMaxVersions();
      scan.setCaching(1000);
    
      //TODO
      //scan.setAttribute(RegionScanner.CLIENT_SCANNER_MEMSTORE_ONLY, "true".getBytes());
    
      RegionScanner scanner = region.getScanner(scan);
      List<Cell> result = new ArrayList<Cell>();
      while(scanner.next(result) == true){
        count += result.size();
        result.clear();
      }
    }
    LOG.info("Found "+count+" kvs in "+ regions.size()+" regions");
    
  }
  
  HashMap<String, Integer> countMap = new HashMap<String, Integer>();
  
  
  public void _testRegionsFamilies() throws IOException
  {
    LOG.info("Test run sequential scanner (memory) - Regions - Families");
    List<HRegion> regions = cluster.findRegionsForTable(_tableA.getName());
    
    for(HRegion region: regions){
      LOG.info(region);
      for(int i=0; i < FAMILIES.length; i++){
        Scan scan = new Scan();
        scan.addFamily(FAMILIES[i]);
        scan.setMaxVersions();
        scan.setCaching(1000);
        //TODO
        //scan.setAttribute(RegionScanner.CLIENT_SCANNER_MEMSTORE_ONLY, "true".getBytes());
        RegionScanner scanner = region.getScanner(scan);
        List<Cell> result = new ArrayList<Cell>();
        int count = 0;
        while(scanner.next(result) == true){
          count += result.size();
          result.clear();
        }
        count+= result.size();
        LOG.info(new String(FAMILIES[i])+": found "+count+" kvs");
        countMap.put(getKey( region, FAMILIES[i]), count);
      }
    }
    
    for( String key: countMap.keySet()){
      LOG.info(key+"="+ countMap.get(key));
    }
  }
  
  private String getKey (HRegion region, byte[] fam)
  {
    return region.toString()+":"+ new String(fam);
  }
  
  public void _testRegionsFamiliesShutdown() throws IOException
  {
    LOG.info("Test run sequential scanner (memory) - Regions - Families shutdown");
    List<HRegion> regions = cluster.findRegionsForTable(_tableA.getName());
    
    for(HRegion region: regions){
      LOG.info(region);
      for(int i=0; i < FAMILIES.length; i++){
        String key = getKey(region, FAMILIES[i]);
        Integer cnt = countMap.get(key);
        if (cnt == 0) {
          LOG.info("Skipping "+ region);
          break;
        }
        

        Scan scan = new Scan();
        for(int k=0; k < FAMILIES.length; k++){
          scan.addFamily(FAMILIES[k]);
        }
        scan.setMaxVersions();
        scan.setCaching(1000);      
        //TODO
        //scan.setAttribute(RegionScanner.CLIENT_SCANNER_MEMSTORE_ONLY, "true".getBytes());
        RegionScanner scanner = region.getScanner(scan);
        
        flush(region, region.getStore(FAMILIES[i]));
        
        List<Cell> result = new ArrayList<Cell>();
        int count = 0;
        while(scanner.next(result) == true){
          count += result.size();
          result.clear();
        }
        count+= result.size();
        int expectedCount = getExpectedCount (region, i);
        LOG.info(" found "+count+" kvs. Expected: "+expectedCount);
        countMap.put(region.toString()+":"+ new String(FAMILIES[i]), count);
      }
    }
    
  }
  
  
  
  private void flush(HRegion region, Store store) throws IOException {
    region.flushcache();
  }


  private int getExpectedCount(HRegion region, int n) {
    int count = 0;
    for(int i = n+1; i < FAMILIES.length; i++){
      String key = getKey(region, FAMILIES[i]);
      int cnt = countMap.get(key);
      count += cnt;
    }
    return count;
  }


  public void _testMemStoreScanFlashScan() throws IOException
  {
    LOG.info("Test run sequential scanner (memory) + flush");
    List<HRegion> regions = cluster.findRegionsForTable(_tableA.getName());
    
    int count = 0;

    for(HRegion region: regions){
      Scan scan = new Scan();
      scan.addFamily(FAMILIES[0]);
      scan.setMaxVersions();
      scan.setCaching(1000);
    
      //TODO
      //scan.setAttribute(RegionScanner.CLIENT_SCANNER_MEMSTORE_ONLY, "true".getBytes());
    
      RegionScanner scanner = region.getScanner(scan);
      List<Cell> result = new ArrayList<Cell>();
      
      scanner.next(result);
     
      if(result.size() > 0){
        LOG.info("Memstore FLUSH for "+region);

        region.flushcache();
      
        try{
          scanner.next(result);
          assertTrue(false);
        } catch(Exception e){
          assertTrue(true);
          LOG.info("Expected "+ e);
        }
        result.clear();
        try{
          boolean hasNext = scanner.next(result);
          assertFalse(hasNext);
          assertTrue(result.size() == 0);
          //assertTrue(false);
        } catch(Exception e){
          assertTrue(true);
          LOG.info("Expected "+ e);
        }
        
      } else{
        LOG.info("Memstore is empty for "+region);
      }
    }
    LOG.info("Found "+count+" kvs in "+ regions.size()+" regions");
    
  }
  
  public void _testMemStoreScanAfterFlush() throws IOException
  {
    LOG.info("Test run sequential scanner (memory) after flush");
    List<HRegion> regions = cluster.findRegionsForTable(_tableA.getName());
    
    HRegion region = regions.get(0);
    
    region.flushcache();
    
    Scan scan = new Scan();
    scan.addFamily(FAMILIES[0]);
    scan.setMaxVersions();
    scan.setCaching(1000);
    
    //TODO
    //scan.setAttribute(RegionScanner.CLIENT_SCANNER_MEMSTORE_ONLY, "true".getBytes());
    
    RegionScanner scanner = region.getScanner(scan);
    List<Cell> result = new ArrayList<Cell>();
    int count = 0;
    while(scanner.next(result) == true){
      count += result.size();
      result.clear();
    }
    
    LOG.info("Found "+count+" kvs");
    
  }
  
  
  public void testAll() throws IOException
  {
    //_testLoadData();
    //_testActiveRegions();
    //_testScan();
    
    //_testRegionsFamilies();
    //_testRegionsFamiliesShutdown();
    //_testMemStoreScan();
    //_testMemStoreScanFlashScan();
    //_testMemStoreScanAfterFlush();
    
  }
}