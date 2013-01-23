package com.ir.hbase.test.structured;

import java.util.Iterator;

import junit.framework.Assert;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.ir.constants.HBaseConstants;
import com.ir.hbase.client.SchemaManager;
import com.ir.hbase.client.index.Index;
import com.ir.hbase.client.index.IndexTableStructure;
import com.ir.hbase.client.structured.TableStructure;
import com.ir.hbase.test.BaseTest;
import com.ir.hbase.test.idx.IdxTestUtils;
//Run test separately
public class ZkInterfaceTest extends BaseTest {
	private static SchemaManager sm;
	@Test
	public void testCheckExist() throws Exception {
		Assert.assertFalse(sm.hasColumn(regTable1, column1.getFamily(), column1.getColumnName()));
		Assert.assertFalse(sm.hasIndex(regTable1, INDEX1));
		sm.addIndexToTable(regTable1, IdxTestUtils.generateIndex1());
		sm.addColumnToTable(regTable1, column1);
		Assert.assertTrue(sm.hasColumn(regTable1, column1.getFamily(), column1.getColumnName()));
		Assert.assertTrue(sm.hasIndex(regTable1, INDEX1));
	}

	@Test
	public void testAddIndex() throws Exception {
		IndexTableStructure its = sm.getIndexTableStructure(regTable1);
		Assert.assertFalse(its.hasIndexes());
		sm.addIndexToTable(regTable1, IdxTestUtils.generateIndex1());
		sm.addIndexToTable(regTable1, IdxTestUtils.generateIndex2());
		its = sm.getIndexTableStructure(regTable1);
		Assert.assertEquals(2, its.getIndexes().size());
		Iterator<Index> iter = its.getIndexes().iterator();
		Assert.assertEquals(IdxTestUtils.generateIndex1().toJSon(), iter.next().toJSon());
		Assert.assertEquals(IdxTestUtils.generateIndex2().toJSon(), iter.next().toJSon());
	}

	@Test
	public void testDeleteIndex() throws Exception {
		sm.addIndexToTable(regTable1, IdxTestUtils.generateIndex1());
		sm.addIndexToTable(regTable1, IdxTestUtils.generateIndex2());
		IndexTableStructure its = sm.getIndexTableStructure(regTable1);
		Assert.assertEquals(2, its.getIndexes().size());
		sm.deleteIndexFromTable(regTable1, INDEX1);
		its = sm.getIndexTableStructure(regTable1);
		Assert.assertEquals(1, its.getIndexes().size());
		sm.deleteIndexFromTable(regTable1, INDEX2);
		its = sm.getIndexTableStructure(regTable1);
		Assert.assertEquals(0, its.getIndexes().size());
	}

	@Test
	public void testAddColumn() throws Exception {
		TableStructure ts = sm.getTableStructure(regTable1);
		Assert.assertEquals(1, ts.getFamilies().size());
		Assert.assertTrue(ts.hasFamily(HBaseConstants.DEFAULT_FAMILY));
		Assert.assertEquals(0, ts.getFamily(HBaseConstants.DEFAULT_FAMILY).getColumns().size());
		sm.addColumnToTable(regTable1, column1);
		sm.addColumnToTable(regTable1, column2);
		sm.addColumnToTable(regTable1, column3);
		sm.addColumnToTable(regTable1, column4);
		ts = sm.getTableStructure(regTable1);
		Assert.assertEquals(3, ts.getFamilies().size());
		Assert.assertEquals(column2.toJSon(), ts.getFamily(colfam2).getColumns().get(0).toJSon());
		Assert.assertEquals(column1.toJSon(), ts.getFamily(colfam2).getColumns().get(1).toJSon());
		Assert.assertEquals(column4.toJSon(), ts.getFamily(colfam3).getColumns().get(0).toJSon());
		Assert.assertEquals(column3.toJSon(), ts.getFamily(colfam3).getColumns().get(1).toJSon());
	}

	@Test 
	public void testDeleteColumn() throws Exception {
		TableStructure ts = sm.getTableStructure(regTable1);
		sm.addColumnToTable(regTable1, column1);
		sm.addColumnToTable(regTable1, column2);
		sm.addColumnToTable(regTable1, column3);
		sm.addColumnToTable(regTable1, column4);
		ts = sm.getTableStructure(regTable1);
		Assert.assertEquals(3, ts.getFamilies().size());
		Assert.assertEquals(column2.toJSon(), ts.getFamily(colfam2).getColumns().get(0).toJSon());
		Assert.assertEquals(column1.toJSon(), ts.getFamily(colfam2).getColumns().get(1).toJSon());
		Assert.assertEquals(column4.toJSon(), ts.getFamily(colfam3).getColumns().get(0).toJSon());
		Assert.assertEquals(column3.toJSon(), ts.getFamily(colfam3).getColumns().get(1).toJSon());
		//Delete columns
		sm.deleteColumnFromTable(regTable1, column1);
		sm.deleteColumnFromTable(regTable1, column3);
		ts = sm.getTableStructure(regTable1);
		Assert.assertEquals(3, ts.getFamilies().size());
		Assert.assertEquals(1, ts.getFamily(colfam2).getColumns().size());
		Assert.assertEquals(1, ts.getFamily(colfam3).getColumns().size());
		//Delete more columns
		sm.deleteColumnFromTable(regTable1, column2);
		sm.deleteColumnFromTable(regTable1, column4);
		ts = sm.getTableStructure(regTable1);
		Assert.assertEquals(1, ts.getFamilies().size());
		Assert.assertTrue(ts.hasFamily(HBaseConstants.DEFAULT_FAMILY));
	}

	@BeforeClass
	public static void setUp() throws Exception {
		basicConf();
		loadIdxStructuredCoprocessor();
		start();
		hbaseTestingUtility.getHBaseAdmin().createTable(TableStructure.generateDefaultStructuredData(regTable1));
		sm = new SchemaManager(hbaseTestingUtility.getHBaseAdmin());
	}
	@AfterClass
	public static void tearDown() throws Exception {
		end();
	}
}