/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.mrio.api.core;

import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.List;
import org.junit.*;
import org.apache.hadoop.hbase.client.HTable;
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
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceTableWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.mrio.MRConstants;
import com.splicemachine.mrio.api.core.SMRecordReaderImpl;

@Ignore
public class SMRecordReaderImplIT extends BaseMRIOTest {
    private static final Logger LOG = Logger.getLogger(SMRecordReaderImplIT.class);
	protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher();
	protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(SMRecordReaderImplIT.class.getSimpleName());	
	protected static SpliceTableWatcher spliceTableWatcherA = new SpliceTableWatcher("A",SMRecordReaderImplIT.class.getSimpleName(),"(col1 int, col2 varchar(56), primary key (col1))");
	protected static SpliceTableWatcher spliceTableWatcherB = new SpliceTableWatcher("B",SMRecordReaderImplIT.class.getSimpleName(),"(col1 int, col2 varchar(56), primary key (col1))");
	protected static SpliceTableWatcher spliceTableWatcherC = new SpliceTableWatcher("C",SMRecordReaderImplIT.class.getSimpleName(),"(col1 int, col2 varchar(56), primary key (col1))");
	protected static SpliceTableWatcher spliceTableWatcherD = new SpliceTableWatcher("D",SMRecordReaderImplIT.class.getSimpleName(),"(col1 int, col2 varchar(56), primary key (col1))");
	protected static SpliceTableWatcher spliceTableWatcherE = new SpliceTableWatcher("E",SMRecordReaderImplIT.class.getSimpleName(),"(col1 int, col2 varchar(56), primary key (col1))");

	@ClassRule 
	public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
		.around(spliceSchemaWatcher)
		.around(spliceTableWatcherA)
		.around(spliceTableWatcherB)
		.around(spliceTableWatcherC)
		.around(spliceTableWatcherD)
		.around(spliceTableWatcherE)	
		.around(new SpliceDataWatcher(){
			@Override
			protected void starting(Description description) {
				try {
					PreparedStatement psA = spliceClassWatcher.prepareStatement("insert into "+ SMRecordReaderImplIT.class.getSimpleName() + ".A (col1,col2) values (?,?)");
					PreparedStatement psB = spliceClassWatcher.prepareStatement("insert into "+ SMRecordReaderImplIT.class.getSimpleName() + ".B (col1,col2) values (?,?)");
					PreparedStatement psC = spliceClassWatcher.prepareStatement("insert into "+ SMRecordReaderImplIT.class.getSimpleName() + ".C (col1,col2) values (?,?)");
					PreparedStatement psD = spliceClassWatcher.prepareStatement("insert into "+ SMRecordReaderImplIT.class.getSimpleName() + ".D (col1,col2) values (?,?)");
					PreparedStatement psE = spliceClassWatcher.prepareStatement("insert into "+ SMRecordReaderImplIT.class.getSimpleName() + ".E (col1,col2) values (?,?)");

					for (int i = 0; i< 1000; i++) {
						psA.setInt(1,i);
						psA.setString(2, "dataset"+i);
						psA.executeUpdate();
					}

					for (int i = 0; i< 1000; i++) {
						psB.setInt(1,i);
						psB.setString(2, "dataset"+i);
						psB.executeUpdate();
						if (i==500)
							flushTable(spliceTableWatcherB.toString());
					}

					for (int i = 0; i< 5000; i++) {
						psE.setInt(1,i);
						psE.setString(2, "dataset"+i);
						psE.executeUpdate();
					}
					flushTable(spliceTableWatcherE.toString());
					compactTable(spliceTableWatcherE.toString());
					
					for (int i = 0; i< 10000; i++) {
						psC.setInt(1,i);
						psC.setString(2, "dataset"+i);
						psC.executeUpdate();
						if (i==5000) {
							flushTable(spliceTableWatcherC.toString());
							splitTable(spliceTableWatcherC.toString());
						}
					}

					for (int i = 0; i< 1000; i++) {
						psD.setInt(1,i);
						psD.setString(2, "dataset"+i);
						psD.executeUpdate();
					}
					flushTable(spliceTableWatcherD.toString());
					
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
    public void completeMemstoreScan() throws Exception{
    	List<String> names = new ArrayList<String>();
    	names.add("COL1");
    	names.add("COL2");    	    	
    	config.set(MRConstants.SPLICE_SCAN_INFO, sqlUtil.getTableScannerBuilder(SMRecordReaderImplIT.class.getSimpleName()+".A", names).base64Encode());
    	SMRecordReaderImpl rr = new SMRecordReaderImpl(config);
    	String tableName = sqlUtil.getConglomID(SMRecordReaderImplIT.class.getSimpleName()+".A");
    	HTable htable = new HTable(config,tableName);    	
    	Scan scan = new Scan();
    	rr.setHTable(htable);
    	rr.setScan(scan);
    	SMSplit tableSplit = new SMSplit(new TableSplit(Bytes.toBytes(tableName), scan.getStartRow(),scan.getStopRow(),"sdfsdf"));
    	rr.initialize(tableSplit, null);
       	int i = 0;
    	while (rr.nextKeyValue()) {
    		i++;
    		Assert.assertNotNull("Column 1 is null", rr.getCurrentValue().getColumn(1));
    		Assert.assertNotNull("Column 2 is null", rr.getCurrentValue().getColumn(2));
    		Assert.assertNotNull("Current Key is null", rr.getCurrentKey());    		
    	}
    	Assert.assertEquals("incorrect results returned",1000,i);
    }
    
    @Test
    public void emptyMemstoreScan() throws Exception{
    	List<String> names = new ArrayList<String>();
    	names.add("COL1");
    	names.add("COL2");    	    	
    	config.set(MRConstants.SPLICE_SCAN_INFO, sqlUtil.getTableScannerBuilder(SMRecordReaderImplIT.class.getSimpleName()+".D", names).base64Encode());
    	SMRecordReaderImpl rr = new SMRecordReaderImpl(config);
    	String tableName = sqlUtil.getConglomID(SMRecordReaderImplIT.class.getSimpleName()+".D");
    	HTable htable = new HTable(config,tableName);    	
    	Scan scan = new Scan();
    	rr.setHTable(htable);
    	rr.setScan(scan);
    	SMSplit tableSplit = new SMSplit(new TableSplit(Bytes.toBytes(tableName), scan.getStartRow(),scan.getStopRow(),"sdfsdf"));
    	rr.initialize(tableSplit, null);
       	int i = 0;
    	while (rr.nextKeyValue()) {
    		i++;
    		Assert.assertNotNull("Column 1 is null", rr.getCurrentValue().getColumn(1));
    		Assert.assertNotNull("Column 2 is null", rr.getCurrentValue().getColumn(2));
    		Assert.assertNotNull("Current Key is null", rr.getCurrentKey());    		
    	}
    	Assert.assertEquals("incorrect results returned",1000,i);
    }
    
    @Test
    public void singleRegionScanWithOneStoreFileAndMemstore() throws Exception{
     	List<String> names = new ArrayList<String>();
    	names.add("COL1");
    	names.add("COL2");    	    	
    	config.set(MRConstants.SPLICE_SCAN_INFO, sqlUtil.getTableScannerBuilder(SMRecordReaderImplIT.class.getSimpleName()+".B", names).base64Encode());
    	SMRecordReaderImpl rr = new SMRecordReaderImpl(config);
    	String tableName = sqlUtil.getConglomID(SMRecordReaderImplIT.class.getSimpleName()+".B");
    	HTable htable = new HTable(config,tableName);    	
    	Scan scan = new Scan();
    	rr.setHTable(htable);
    	rr.setScan(scan);
    	SMSplit tableSplit = new SMSplit(new TableSplit(Bytes.toBytes(tableName), scan.getStartRow(),scan.getStopRow(),"sdfsdf"));
    	rr.initialize(tableSplit, null);
       	int i = 0;
    	while (rr.nextKeyValue()) {
    		i++;
    		Assert.assertNotNull("Column 1 is null", rr.getCurrentValue().getColumn(1));
    		Assert.assertNotNull("Column 2 is null", rr.getCurrentValue().getColumn(2));
    		Assert.assertNotNull("Current Key is null", rr.getCurrentKey());    		
    	}
    	Assert.assertEquals("incorrect results returned",1000,i);
    }

    @Test
    public void twoRegionsWithMemstores() throws Exception{
     	List<String> names = new ArrayList<String>();
    	names.add("COL1");
    	names.add("COL2");    	    	
    	config.set(MRConstants.SPLICE_SCAN_INFO, sqlUtil.getTableScannerBuilder(SMRecordReaderImplIT.class.getSimpleName()+".C", names).base64Encode());
    	SMRecordReaderImpl rr = new SMRecordReaderImpl(config);
    	String tableName = sqlUtil.getConglomID(SMRecordReaderImplIT.class.getSimpleName()+".C");
    	HTable htable = new HTable(config,tableName);    	
    	Scan scan = new Scan();
    	rr.setHTable(htable);
    	rr.setScan(scan);
    	SMSplit tableSplit = new SMSplit(new TableSplit(Bytes.toBytes(tableName), scan.getStartRow(),scan.getStopRow(),"sdfsdf"));
    	rr.initialize(tableSplit, null);
       	int i = 0;
    	while (rr.nextKeyValue()) {
    		i++;
    		Assert.assertNotNull("Column 1 is null", rr.getCurrentValue().getColumn(1));
    		Assert.assertNotNull("Column 2 is null", rr.getCurrentValue().getColumn(2));
    		Assert.assertNotNull("Current Key is null", rr.getCurrentKey());    		
    	}
    	Assert.assertEquals("incorrect results returned",10000,i);
    }
    
    @Test
    public void testScanAfterMajorCompaction() throws Exception{
     	List<String> names = new ArrayList<String>();
    	names.add("COL1");
    	names.add("COL2");    	    	
    	config.set(MRConstants.SPLICE_SCAN_INFO, sqlUtil.getTableScannerBuilder(SMRecordReaderImplIT.class.getSimpleName()+".E", names).base64Encode());
    	SMRecordReaderImpl rr = new SMRecordReaderImpl(config);
    	String tableName = sqlUtil.getConglomID(SMRecordReaderImplIT.class.getSimpleName()+".E");
    	HTable htable = new HTable(config,tableName);    	
    	Scan scan = new Scan();
    	rr.setHTable(htable);
    	rr.setScan(scan);
    	SMSplit tableSplit = new SMSplit(new TableSplit(Bytes.toBytes(tableName), scan.getStartRow(),scan.getStopRow(),"sdfsdf"));
    	rr.initialize(tableSplit, null);
       	int i = 0;
    	while (rr.nextKeyValue()) {
    		i++;
    		Assert.assertNotNull("Column 1 is null", rr.getCurrentValue().getColumn(1));
    		Assert.assertNotNull("Column 2 is null", rr.getCurrentValue().getColumn(2));
    		Assert.assertNotNull("Current Key is null", rr.getCurrentKey());    		
    	}
    	Assert.assertEquals("incorrect results returned",5000,i);
    }
    	        
}

