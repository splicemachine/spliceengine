package com.splicemachine.si.coprocessor;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.splicemachine.si.utils.SIUtils;

public class FilterTest {
	public static final String SI_EXAMPLE = "SI_EXAMPLE";
	public static HBaseAdmin admin = null;
	public static HTablePool pool = new HTablePool();
	@BeforeClass
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

	@AfterClass
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

	
	@Test
	public void test() throws IOException {
		HTableInterface exampleTable = pool.getTable(SI_EXAMPLE);
	}
	
}
