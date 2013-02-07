package com.splicemachine.si.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTablePool;

import com.splicemachine.si.utils.SIUtils;

public class SIBaseTest {
	public static final String SI_EXAMPLE = "SI_EXAMPLE";
	public static HBaseAdmin admin = null;
	public static HTablePool pool = new HTablePool();
	public static void startup() throws Exception {
		try {
			admin = new HBaseAdmin(new Configuration());
			if (admin.tableExists(SI_EXAMPLE)) {
				admin.disableTable(SI_EXAMPLE);
				admin.deleteTable(SI_EXAMPLE);
			}
			admin.createTable(SIUtils.generateSIHtableDescriptor(SI_EXAMPLE));
		} catch (Exception e) {
			throw e;
		} finally {
			if (admin != null)
				admin.close();
		}
		
	}

	public static void tearDown() throws Exception {
		try {
			admin = new HBaseAdmin(new Configuration());
			if (admin.tableExists(SI_EXAMPLE)) {
				admin.disableTable(SI_EXAMPLE);
				admin.deleteTable(SI_EXAMPLE);
			}
		} catch (Exception e) {
			throw e;
		} finally {
			if (admin != null)
				admin.close();
		}		
	}
	
}
