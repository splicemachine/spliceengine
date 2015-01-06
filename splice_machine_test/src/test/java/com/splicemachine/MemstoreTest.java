package com.splicemachine;
/*
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.FSUtils;
import com.splicemachine.constants.SpliceConstants;
*/
public class MemstoreTest {

	//@Test
	public void testMemstoreScan() throws Exception {
		/*Scan scan = new Scan();
		scan.setAttribute("MR", "y".getBytes());
		scan.addFamily(SpliceConstants.DEFAULT_FAMILY_BYTES);
		Configuration configuration = HBaseConfiguration.create();
		configuration.set("hbase.zookeeper.property.clientPort","2181");
		HBaseAdmin admin = new HBaseAdmin(configuration);
		admin.flush("1232");
		HTable table = new HTable(configuration,"1232");
		
		scan.setCaching(1024);
		ResultScanner rs = table.getScanner(scan);
		HRegionLocation location = table.getRegionLocation(scan.getStartRow());
		location.getRegionInfo().getEncodedName();
		System.out.println("IN here: " + location.getRegionInfo().getEncodedName());
		Path path = FSUtils.getTableDir(new Path(configuration.get("hbase.rootdir") + "/"+location.getRegionInfo().getEncodedName()), TableName.valueOf(table.getTableName()));
		System.out.println("Path ?" + path);
		Result res;
		int count = 0;
		long current = System.currentTimeMillis();
		while ( (res =rs.next()) != null) {
			count++;
		}
		System.out.println("Count: " + count + " time: " + (System.currentTimeMillis() - current));
		rs.close();
		*/
	}
	
}
