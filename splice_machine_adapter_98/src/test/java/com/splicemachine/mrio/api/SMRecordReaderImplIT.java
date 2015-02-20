package com.splicemachine.mrio.api;

import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.List;

import org.junit.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableSplit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;

import com.splicemachine.derby.test.framework.SpliceDataWatcher;
import com.splicemachine.derby.test.framework.SpliceNetConnection;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceTableWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;

public class SMRecordReaderImplIT extends BaseMRIOTest {
    private static final Logger LOG = Logger.getLogger(SMRecordReaderImplIT.class);
	protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher();
	protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(SMRecordReaderImplIT.class.getSimpleName());	
	protected static SpliceTableWatcher spliceTableWatcherA = new SpliceTableWatcher("A",SMRecordReaderImplIT.class.getSimpleName(),"(col1 int, col2 varchar(56), primary key (col1))");

	
	@ClassRule 
	public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
		.around(spliceSchemaWatcher)
		.around(spliceTableWatcherA)
		.around(new SpliceDataWatcher(){
			@Override
			protected void starting(Description description) {
				try {
					for (int i = 0; i< 1000; i++) {
						PreparedStatement psA = spliceClassWatcher.prepareStatement("insert into "+ SMRecordReaderImplIT.class.getSimpleName() + ".A (col1,col2) values (?,?)");
						psA.setInt(1,i);
						psA.setString(2, "dataset"+i);
						psA.executeUpdate();
					}
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
				finally {
					spliceClassWatcher.closeAll();
				}
			}
			
		});
	
	@Rule public SpliceWatcher methodWatcher = new SpliceWatcher();


	
    @Test
    public void canYouSplitDuringAScan() throws Exception{
    	List<String> names = new ArrayList<String>();
    	names.add("COL1");
    	names.add("COL2");    	    	
    	config.set(SMMRConstants.SPLICE_SCAN_INFO, sqlUtil.getTableScannerBuilder(SMRecordReaderImplIT.class.getSimpleName()+".A", names).getTableScannerBuilderBase64String());    	
    	SMRecordReaderImpl recordReader = new SMRecordReaderImpl(config);
    	String tableName = sqlUtil.getConglomID(SMRecordReaderImplIT.class.getSimpleName()+".A");
    	HTable htable = new HTable(config,tableName);    	
    	ResultScanner rs = htable.getScanner(new Scan());
    	HBaseAdmin admin = new HBaseAdmin(config);
    	Result result;
    	while ( (result = rs.next()) != null) {
    		System.out.println("result -> " + result);
    	}
    }

	
    @Test
    public void testRecordReaderNoSplits() throws Exception{
    	List<String> names = new ArrayList<String>();
    	names.add("COL1");
    	names.add("COL2");    	    	
    	config.set(SMMRConstants.SPLICE_SCAN_INFO, sqlUtil.getTableScannerBuilder(SMRecordReaderImplIT.class.getSimpleName()+".A", names).getTableScannerBuilderBase64String());    	
    	SMRecordReaderImpl recordReader = new SMRecordReaderImpl(config);
    	String tableName = sqlUtil.getConglomID(SMRecordReaderImplIT.class.getSimpleName()+".A");
    	HTable htable = new HTable(config,tableName);    	
    	recordReader.setHTable(htable);
    	recordReader.setScan(new Scan());
    	TableSplit split = new TableSplit(Bytes.toBytes(tableName),null,null,null);
    	recordReader.init(config, split);
    	int i = 0;
    	while (recordReader.nextKeyValue()) {
    		i++;
    		Assert.assertNotNull("Column 1 is null", recordReader.getCurrentValue().getColumn(1));
    		Assert.assertNotNull("Column 2 is null", recordReader.getCurrentValue().getColumn(2));
    	}
    	Assert.assertEquals("Inaccurate number of rows returned",1000,i);
    }

    @Test
    public void testRecordReaderSingleColumnNoPrimaryKey() throws Exception{
    	List<String> names = new ArrayList<String>();
    	names.add("COL2");    	    	
    	config.set(SMMRConstants.SPLICE_SCAN_INFO, sqlUtil.getTableScannerBuilder(SMRecordReaderImplIT.class.getSimpleName()+".A", names).getTableScannerBuilderBase64String());    	
    	SMRecordReaderImpl recordReader = new SMRecordReaderImpl(config);
    	String tableName = sqlUtil.getConglomID(SMRecordReaderImplIT.class.getSimpleName()+".A");
    	HTable htable = new HTable(config,tableName);    	
    	recordReader.setHTable(htable);
    	recordReader.setScan(new Scan());
    	TableSplit split = new TableSplit(Bytes.toBytes(tableName),null,null,null);
    	recordReader.init(config, split);
    	int i = 0;
    	while (recordReader.nextKeyValue()) {
    		i++;
    		Assert.assertNotNull("Column 1 is null", recordReader.getCurrentValue().getColumn(1));
    	}
    	Assert.assertEquals("Inaccurate number of rows returned",1000,i);
    }

    @Test
    public void testRecordReaderSingleColumnPrimaryKey() throws Exception{
    	List<String> names = new ArrayList<String>();
    	names.add("COL1");    	    	
    	config.set(SMMRConstants.SPLICE_SCAN_INFO, sqlUtil.getTableScannerBuilder(SMRecordReaderImplIT.class.getSimpleName()+".A", names).getTableScannerBuilderBase64String());    	
    	SMRecordReaderImpl recordReader = new SMRecordReaderImpl(config);
    	String tableName = sqlUtil.getConglomID(SMRecordReaderImplIT.class.getSimpleName()+".A");
    	HTable htable = new HTable(config,tableName);    	
    	recordReader.setHTable(htable);
    	recordReader.setScan(new Scan());
    	TableSplit split = new TableSplit(Bytes.toBytes(tableName),null,null,null);
    	recordReader.init(config, split);
    	int i = 0;
    	while (recordReader.nextKeyValue()) {
    		i++;
    		Assert.assertNotNull("Column 1 is null", recordReader.getCurrentValue().getColumn(1));
    	}
    	Assert.assertEquals("Inaccurate number of rows returned",1000,i);
    }

    @Test
    public void testRecordReaderAfterFlush() throws Exception{
    	String tableName = sqlUtil.getConglomID(SMRecordReaderImplIT.class.getSimpleName()+".A");
    	HBaseAdmin admin = new HBaseAdmin(config);
    	admin.flush(tableName); // Synchronous
    	Thread.sleep(2000);
    	List<String> names = new ArrayList<String>();
    	names.add("COL1");
    	names.add("COL2");    	    	
    	config.set(SMMRConstants.SPLICE_SCAN_INFO, sqlUtil.getTableScannerBuilder(SMRecordReaderImplIT.class.getSimpleName()+".A", names).getTableScannerBuilderBase64String());    	
    	SMRecordReaderImpl recordReader = new SMRecordReaderImpl(config);
    	HTable htable = new HTable(config,tableName);    	
    	recordReader.setHTable(htable);
    	recordReader.setScan(new Scan());
    	TableSplit split = new TableSplit(Bytes.toBytes(tableName),null,null,null);
    	recordReader.init(config, split);
    	int i = 0;
    	while (recordReader.nextKeyValue()) {
    		i++;
    		Assert.assertNotNull("Column 1 is null", recordReader.getCurrentValue().getColumn(1));
    		Assert.assertNotNull("Column 2 is null", recordReader.getCurrentValue().getColumn(2));
    	}
    	Assert.assertEquals("Inaccurate number of rows returned",1000,i);
    	
    }

    
    @Test
    public void testRecordReaderMultipleRegions() throws Exception{
    	
    }

    @Test
    public void testRecordReaderWithSplits() throws Exception{
    
    }

    	    
}

