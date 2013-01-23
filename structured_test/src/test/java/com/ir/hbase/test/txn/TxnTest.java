package com.ir.hbase.test.txn;

import junit.framework.Assert;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.ir.constants.HBaseConstants;
import com.ir.hbase.test.BaseTest;
import com.ir.hbase.test.idx.IdxTestUtils;

public class TxnTest extends BaseTest {
	private static HTableInterface htable;
	@Test
	public void testNonTransactionalPutAndScan() throws Exception {
		IdxTestUtils.putDataToTable(htable, null);
		Assert.assertEquals(10, IdxTestUtils.countRowNum(htable, null)); //count row using scan
	}
	
	@Test
	public void testNonTransactionalDelete() throws Exception {
		IdxTestUtils.putDataToTable(htable, null);
		htable.delete(new Delete(ROW2));
		htable.delete(new Delete(ROW3));
		Assert.assertEquals(8, IdxTestUtils.countRowNum(htable, null));
	}
	
	@Test
	public void testNontransactionalGet() throws Exception {
		IdxTestUtils.putDataToTable(htable, null);
		Assert.assertEquals("VAL1", Bytes.toString(htable.get(new Get(ROW3)).getColumnLatest(FAMILY2, COL3).getValue()));
	}
	
	@BeforeClass
	public static void setTest() throws Exception {
		System.out.println(hbaseTestingUtility.getClusterTestDir());
		basicConf();
		loadIdxStructuredTxnCoprocessor();
		start();
		HTableDescriptor desc = new HTableDescriptor(regTable1);
		desc.addFamily(new HColumnDescriptor(HBaseConstants.DEFAULT_FAMILY));
		desc.addFamily(new HColumnDescriptor(colfam1));
		desc.addFamily(new HColumnDescriptor(colfam2));
		desc.addFamily(new HColumnDescriptor(colfam3));
		desc.addFamily(new HColumnDescriptor(colfam4));
		hbaseTestingUtility.getHBaseAdmin().createTable(desc);
		htable = new HTable(hbaseTestingUtility.getConfiguration(), REGULAR_TABLE1);
	}
	@AfterClass
	public static void tearDown() throws Exception {
		end();
	}
}
