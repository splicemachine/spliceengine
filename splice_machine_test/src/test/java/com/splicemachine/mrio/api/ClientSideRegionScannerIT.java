package com.splicemachine.mrio.api;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.log4j.Logger;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;

import com.splicemachine.constants.SIConstants;
import com.splicemachine.derby.hbase.DerbyFactory;
import com.splicemachine.derby.hbase.DerbyFactoryDriver;
import com.splicemachine.derby.test.framework.SpliceDataWatcher;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceTableWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.mrio.MRConstants;
import com.splicemachine.si.data.api.SDataLib;
import com.splicemachine.si.impl.HTransactorFactory;
import com.splicemachine.si.impl.SITransactor;

public class ClientSideRegionScannerIT extends BaseMRIOTest {
    private static final Logger LOG = Logger.getLogger(ClientSideRegionScannerIT.class);
    protected static String SCHEMA_NAME=ClientSideRegionScannerIT.class.getSimpleName();
	protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher();
	protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(SCHEMA_NAME);	
	protected static SpliceTableWatcher spliceTableWatcherA = new SpliceTableWatcher("A",SCHEMA_NAME,"(col1 int, col2 varchar(56), primary key (col1))");
	SDataLib dataLib = HTransactorFactory.getTransactor().getDataLib();

	@ClassRule 
	public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
		.around(spliceSchemaWatcher)
		.around(spliceTableWatcherA)
		.around(new SpliceDataWatcher(){
			@Override
			protected void starting(Description description) {
				try {
					spliceClassWatcher.setAutoCommit(false);
					PreparedStatement psA = spliceClassWatcher.prepareStatement("insert into "+ SCHEMA_NAME+ ".A (col1,col2) values (?,?)");
					for (int i = 0; i< 500; i++) {
						if (i%10 != 0)
							continue;							
						psA.setInt(1,i);
						psA.setString(2, "dataset"+i);
						psA.executeUpdate();
						if (i==250)
							flushTable(SCHEMA_NAME+".A");
					}
					psA = spliceClassWatcher.prepareStatement("update "+ SCHEMA_NAME+ ".A set col2 = ? where col1 = ?");
					PreparedStatement psB = spliceClassWatcher.prepareStatement("insert into "+ SCHEMA_NAME+ ".A (col1,col2) values (?,?)");
					for (int i = 0; i< 500; i++) {
						if (i%10 == 0) {
							psA.setString(1, "datasetupdate"+i);
							psA.setInt(2,i);
							psA.executeUpdate();
						} else {
							psB.setInt(1,i);
							psB.setString(2, "dataset"+i);
							psB.executeUpdate();
						}
					}
					spliceClassWatcher.commit();
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
	@Ignore
	public void validateAccurateRecordsWithStoreFileAndMemstore() throws SQLException, IOException, InterruptedException {
		int i = 0;
    	HBaseAdmin admin = null;
    	ResultScanner rs = null;
    	HTable htable = null;
		try {
			String tableName = sqlUtil.getConglomID(SCHEMA_NAME+".A");
	    	htable = new HTable(config,tableName);
	    	Scan scan = new Scan();
	    	scan.setCaching(50);
	    	scan.setBatch(50);
	    	scan.setMaxVersions();
	    	scan.setAttribute(MRConstants.SPLICE_SCAN_MEMSTORE_ONLY, SIConstants.EMPTY_BYTE_ARRAY);    	
			ClientSideRegionScanner clientSideRegionScanner = 
					new ClientSideRegionScanner(htable,htable.getConfiguration(),FSUtils.getCurrentFileSystem(htable.getConfiguration()), FSUtils.getRootDir(htable.getConfiguration()),
							htable.getTableDescriptor(),htable.getRegionLocation(scan.getStartRow()).getRegionInfo(),
							scan,null);
			List results = new ArrayList();
			while (dataLib.internalScannerNext(clientSideRegionScanner, results)) {
				i++;
				results.clear();
			}
			clientSideRegionScanner.close();
			Assert.assertEquals("Results Returned Are Not Accurate", 500,i);
		}
    	finally { 
    		if (admin != null)
    			admin.close();
    		if (htable != null)
    			htable.close();
    		if (rs != null)
   			rs.close();
    	}
	}

	
	@Test
	@Ignore
	public void validateAccurateRecordsWithRegionFlush() throws SQLException, IOException, InterruptedException {
		int i = 0;
    	HBaseAdmin admin = null;
    	ResultScanner rs = null;
    	HTable htable = null;
		try {
			String tableName = sqlUtil.getConglomID(SCHEMA_NAME+".A");
	    	htable = new HTable(config,tableName);
	    	Scan scan = new Scan();
	    	admin = new HBaseAdmin(config);
	    	scan.setCaching(50);
	    	scan.setBatch(50);
	    	scan.setMaxVersions();
	    	scan.setAttribute(MRConstants.SPLICE_SCAN_MEMSTORE_ONLY, SIConstants.EMPTY_BYTE_ARRAY);    	
	    	
			ClientSideRegionScanner clientSideRegionScanner = 
					new ClientSideRegionScanner(htable,htable.getConfiguration(),FSUtils.getCurrentFileSystem(htable.getConfiguration()), FSUtils.getRootDir(htable.getConfiguration()),
							htable.getTableDescriptor(),htable.getRegionLocation(scan.getStartRow()).getRegionInfo(),
							scan,null);
			List results = new ArrayList();
			while (dataLib.internalScannerNext(clientSideRegionScanner, results)) {
				i++;
				if (i==100) 
					admin.flush(tableName);
				results.clear();
			}
			clientSideRegionScanner.close();
			Assert.assertEquals("Results Returned Are Not Accurate", 500,i);
		}
    	finally { 
    		if (admin != null)
    			admin.close();
    		if (htable != null)
    			htable.close();
    		if (rs != null)
   			rs.close();
    	}
	}
	
}