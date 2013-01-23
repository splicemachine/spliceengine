package com.ir.hbase.test.idx;

import java.io.IOException;
import junit.framework.Assert;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.ir.constants.SchemaConstants;
import com.ir.hbase.test.BaseTest;

public class IndexTest extends BaseTest {
    
	@BeforeClass
	public static void setUp() throws Exception {
		basicConf();
		loadIdxCoprocessor();
		start();
		//create table
		hbaseTestingUtility.getHBaseAdmin().createTable(IdxTestUtils.generateIndexTableDescriptor());
		htable = new HTable(hbaseTestingUtility.getConfiguration(), REGULAR_TABLE1);
		indexTable = new HTable(hbaseTestingUtility.getConfiguration(), Bytes.toBytes(regTable1 + SchemaConstants.INDEX));
	}
	
	
	/**
	 * Delete some of the index key columns, the whole corresponding index row should be deleted. 
	 */
	@Test
	public void testDeleteIndexColumn() throws IOException {
		//Put data into table without transaction function
		IdxTestUtils.putDataToTable(htable, null);
		Delete delete1 = new Delete(Bytes.toBytes("ROW2"));
		delete1.deleteColumns(FAMILY4, COL7);
		htable.delete(delete1);
		Delete delete2 = new Delete(Bytes.toBytes("ROW3"));
		delete2.deleteFamily(FAMILY4);
		htable.delete(delete2);
		Assert.assertFalse(IdxTestUtils.containsRow(INDEX2, "ROW2", indexTable, null));
		Assert.assertFalse(IdxTestUtils.containsRow(INDEX2, "ROW3", indexTable, null));
	}
	/**
	 * Delete some of the additional columns, the corresponding index row should be still there. 
	 */
	@Test
	public void testDeleteAdditionalColumn() throws IOException {
		IdxTestUtils.putDataToTable(htable, null);
		Delete delete1 = new Delete(Bytes.toBytes("ROW2"));
		delete1.deleteColumns(FAMILY3, COL5);
		delete1.deleteColumns(FAMILY3, COL6);
		htable.delete(delete1);
		Delete delete2 = new Delete(Bytes.toBytes("ROW3"));
		delete2.deleteColumns(FAMILY2, COL3);
		delete2.deleteColumns(FAMILY2, COL4);
		htable.delete(delete2);
		Assert.assertTrue(IdxTestUtils.containsRow(INDEX2, "ROW2", indexTable, null));
		Assert.assertTrue(IdxTestUtils.containsRow(INDEX2, "ROW3", indexTable, null));
	}
	
	
	@AfterClass
	public static void tearDown() throws Exception {
		end();
	}
}
