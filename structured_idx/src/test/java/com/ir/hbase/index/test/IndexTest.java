package com.ir.hbase.index.test;

import java.io.IOException;
import junit.framework.Assert;

import org.apache.hadoop.hbase.HTableDescriptor;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.ir.hbase.client.index.Index;
import com.ir.hbase.client.index.IndexTableStructure;

public class IndexTest extends BaseTest {
	
	@BeforeClass
	public static void setUp() throws Exception {
		basicConf();
		loadIdxStructuredCoprocessor();
		start();
	}
	
	@Test
	public void testSingleIndexColumnPut() throws IOException {
		HTableDescriptor desc = IdxTestUtils.generateDefaultTableDescriptor(regTable1);
		IndexTableStructure its = new IndexTableStructure();
		Index index = new Index(SINGLE_INTEGER_INDEX);
		index.addIndexColumn(icolumn2);
		its.addIndex(index);
		IdxTestUtils.refreshRegularTable1(IndexTableStructure.setIndexStructure(desc, its));
		IdxTestUtils.putSingleColumnToTable(htable, 1, 10, null);
		Assert.assertEquals(10, IdxTestUtils.countRow(htable, null, null));
		Assert.assertEquals(10, IdxTestUtils.countRow(indexTable, null, null));
		Assert.assertTrue(IdxTestUtils.containsColumn(SINGLE_INTEGER_INDEX, colfam1.getBytes(), COL2, indexTable, null));
	}
	
	@Test
	public void testSingleIndexColumnDelete() throws IOException {
		HTableDescriptor desc = IdxTestUtils.generateDefaultTableDescriptor(regTable1);
		IndexTableStructure its = new IndexTableStructure();
		Index index = new Index(SINGLE_INTEGER_INDEX);
		index.addIndexColumn(icolumn2);
		its.addIndex(index);
		IdxTestUtils.refreshRegularTable1(IndexTableStructure.setIndexStructure(desc, its));
		IdxTestUtils.putSingleColumnToTable(htable, 1, 10, null);
		IdxTestUtils.deleteSingleColumnFromTable(htable, 1, 10, null);
		Assert.assertEquals(0, IdxTestUtils.countRow(htable, null, null));
		Assert.assertEquals(0, IdxTestUtils.countRow(indexTable, null, null));
	}
	
	@Test
	public void testMultipleIndexColumnsPut() throws IOException {
		HTableDescriptor desc = IdxTestUtils.generateDefaultTableDescriptor(regTable1);
		IndexTableStructure its = new IndexTableStructure();
		its.addIndex(IdxTestUtils.generateIndex1());
		IdxTestUtils.refreshRegularTable1(IndexTableStructure.setIndexStructure(desc, its));
		IdxTestUtils.putMultipleColumnsToTable(htable, 1, 10, null);
		Assert.assertEquals(10, IdxTestUtils.countRow(htable, null, null));
		Assert.assertEquals(10, IdxTestUtils.countRow(indexTable, null, null));
		Assert.assertTrue(IdxTestUtils.containsColumn(INDEX1, colfam1.getBytes(), COL1, indexTable, null));
		Assert.assertTrue(IdxTestUtils.containsColumn(INDEX1, colfam1.getBytes(), COL2, indexTable, null));
	}
	
	@Test
	public void testMultipleIndexColumnsDelete() throws IOException {
		HTableDescriptor desc = IdxTestUtils.generateDefaultTableDescriptor(regTable1);
		IndexTableStructure its = new IndexTableStructure();
		its.addIndex(IdxTestUtils.generateIndex1());
		IdxTestUtils.refreshRegularTable1(IndexTableStructure.setIndexStructure(desc, its));
		IdxTestUtils.putMultipleColumnsToTable(htable, 1, 10, null);
		IdxTestUtils.deleteSingleColumnFromTable(htable, 1, 10, null);
		Assert.assertEquals(10, IdxTestUtils.countRow(htable, null, null));
		Assert.assertEquals(10, IdxTestUtils.countRow(indexTable, null, null));
	}
	
	@Test
	public void testAdditionalColumn() throws IOException {
		HTableDescriptor desc = IdxTestUtils.generateDefaultTableDescriptor(regTable1);
		IndexTableStructure its = new IndexTableStructure();
		its.addIndex(IdxTestUtils.generateIndex2());
		IdxTestUtils.refreshRegularTable1(IndexTableStructure.setIndexStructure(desc, its));
		IdxTestUtils.putMultipleColumnsToTable(htable, 1, 10, null);
		IdxTestUtils.deleteSingleColumnFromTable(htable, 1, 10, null);
		Assert.assertEquals(10, IdxTestUtils.countRow(htable, null, null));
		Assert.assertEquals(10, IdxTestUtils.countRow(indexTable, null, null));
		Assert.assertTrue(IdxTestUtils.containsColumn(INDEX2, colfam2.getBytes(), COL3, indexTable, null));
		Assert.assertTrue(IdxTestUtils.containsColumn(INDEX2, colfam2.getBytes(), COL4, indexTable, null));
		Assert.assertTrue(IdxTestUtils.containsColumn(INDEX2, colfam3.getBytes(), COL5, indexTable, null));
		Assert.assertTrue(IdxTestUtils.containsColumn(INDEX2, colfam3.getBytes(), COL6, indexTable, null));
	}
	
	@AfterClass
	public static void tearDown() throws Exception {
		end();
	}
	
}
