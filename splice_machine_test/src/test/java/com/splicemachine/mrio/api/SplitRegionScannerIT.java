package com.splicemachine.mrio.api;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.log4j.Logger;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;

import com.splicemachine.constants.SIConstants;
import com.splicemachine.derby.hbase.DerbyFactoryImpl;
import com.splicemachine.derby.test.framework.SpliceDataWatcher;
import com.splicemachine.derby.test.framework.SpliceNetConnection;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceTableWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.mrio.MRConstants;
import com.splicemachine.si.impl.SIFactoryImpl;

public class SplitRegionScannerIT extends BaseMRIOTest {
    private static final Logger LOG = Logger.getLogger(SplitRegionScannerIT.class);
	protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher();
	protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(SplitRegionScannerIT.class.getSimpleName());	
	protected static SpliceTableWatcher spliceTableWatcherA = new SpliceTableWatcher("A",SplitRegionScannerIT.class.getSimpleName(),"(col1 int, col2 varchar(56), primary key (col1))");
	protected static SpliceTableWatcher spliceTableWatcherB = new SpliceTableWatcher("B",SplitRegionScannerIT.class.getSimpleName(),"(col1 int, col2 varchar(56), primary key (col1))");
	protected static DerbyFactoryImpl derbyFactory = new DerbyFactoryImpl();
	
	@ClassRule 
	public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
		.around(spliceSchemaWatcher)
		.around(spliceTableWatcherA)
		.around(spliceTableWatcherB)
		.around(new SpliceDataWatcher(){
			@Override
			protected void starting(Description description) {
				try {
					PreparedStatement psA = spliceClassWatcher.prepareStatement("insert into "+ SplitRegionScannerIT.class.getSimpleName() + ".A (col1,col2) values (?,?)");
					PreparedStatement psB = spliceClassWatcher.prepareStatement("insert into "+ SplitRegionScannerIT.class.getSimpleName() + ".B (col1,col2) values (?,?)");

					for (int i = 0; i< 10000; i++) {
						psA.setInt(1,i);
						psA.setString(2, "dataset"+i);
						psA.executeUpdate();
						if (i==5000) {
							flushTable(spliceTableWatcherA.toString());
							splitTable(spliceTableWatcherA.toString());
						}
					}

					for (int i = 0; i< 1000; i++) {
						psB.setInt(1,i);
						psB.setString(2, "dataset"+i);
						psB.executeUpdate();
						if (i==500)
							flushTable(spliceTableWatcherB.toString());
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
	@Ignore
	public void validateAccurateRecordsWithStoreFileAndMemstore() throws SQLException, IOException, InterruptedException {
		String tableName = sqlUtil.getConglomID(spliceTableWatcherA.toString());
		HTable table = new HTable(config,tableName);
		Scan scan = new Scan();
		SpliceRegionScanner splitRegionScanner = derbyFactory.getSplitRegionScanner(scan, table);
		List data = new ArrayList();		
	//	for (splitRegionScanner.next(data)) {
			
		//}
		
	}
}
