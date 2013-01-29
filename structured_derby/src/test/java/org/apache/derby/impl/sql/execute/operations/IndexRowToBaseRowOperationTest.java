package org.apache.derby.impl.sql.execute.operations;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

import com.splicemachine.derby.test.DerbyTestRule;

public class IndexRowToBaseRowOperationTest {
	private static final Logger LOG = Logger.getLogger(IndexRowToBaseRowOperationTest.class);
	
	@Rule public DerbyTestRule rule = new DerbyTestRule(Collections.singletonMap("t","i int"),LOG);
	
	@BeforeClass
	public static void startup() throws Exception{
		DerbyTestRule.start();
	}
	
	@AfterClass
	public static void shutdown() throws Exception{
		DerbyTestRule.shutdown();
	}
	
	@Test
	public void testScanSysConglomerates() throws Exception{
		ResultSet s = null;
//		try{
			s = rule.executeQuery("select * from sys.sysconglomerates");
			while(s.next()){
				LOG.info(String.format("schemaId=%s,tableId=%s,conglom#=%d,conglomName=%s,isIndex=%b,isConstraint=%b,conglomId=%s",
									s.getString(1),s.getString(2),s.getInt(3),s.getString(4),s.getBoolean(5),s.getBoolean(7),s.getString(8)));
			}
//		}finally{
//			if(s !=null) s.close();
//		}
	}
	
	@Test
	public void testRestrictScanSysConglomerates() throws Exception{
		ResultSet s = null;
//		try{
			s = rule.executeQuery("select count(*) from sys.sysconglomerates");
			while(s.next()){
				LOG.info(String.format("count=%d",s.getInt(1)));
			}
//		}finally{
//			if(s !=null) s.close();
//		}
	}
	
	@Test
	@Ignore
	public void testJoinSysSchemasAndSysTables() throws Exception{
		ResultSet rs = rule.executeQuery("select s.schemaid, t.tableid from sys.sysschemas s join sys.systables t on s.schemaid=t.schemaid");
		while(rs.next()){
			LOG.info(String.format("schemaid=%s,tableid=%s",rs.getString(1),rs.getString(2)));
		}
	}
	
	@Test
	public void testExportTable() throws Exception{
		LOG.info("Setting up test");
		Statement s = null;
		ResultSet rs = null;
//		try{
			s = rule.getStatement();
			//kept around to help find the table of interest
//			rs = executeQuery("select * from sys.systables");
//			while(rs.next()){
//				LOG.info(String.format("tableId=%s,tableName=%s,tableType=%s,schemaId=%s,lockGranularity=%s",
//									rs.getString(1),rs.getString(2),rs.getString(3),rs.getString(4),rs.getString(5)));
//			}
			s.execute("CALL SYSCS_UTIL.SYSCS_EXPORT_TABLE (null,'T','myfile.del',null,null,null)");
//		}finally{
//			if(s !=null) s.close();
			
//			dropTable();
//		}
	}

	@Test
	@Ignore
	public void testImportTable() throws Exception{
		Statement s = null;
//		try{
//			s = DerbyTestRule.conn.createStatement();
//			s.execute("create table t (i int)");
//			DerbyTestRule.conn.commit();
//		}finally{
//			if(s!=null) s.close();
//		}
		
		LOG.trace("Created import table");
		ResultSet rs = null;
//		try{
			s = rule.getStatement();
			//kept around to help find the table of interest
//			rs = executeQuery("select * from sys.systables");
//			while(rs.next()){
//				LOG.info(String.format("tableId=%s,tableName=%s,tableType=%s,schemaId=%s,lockGranularity=%s",
//									rs.getString(1),rs.getString(2),rs.getString(3),rs.getString(4),rs.getString(5)));
//			}
			s.execute("CALL SYSCS_UTIL.SYSCS_IMPORT_DATA (null,'T',null,null,'myfile.del',null,null,null,0)");
			rule.commit();
			
			rs = rule.executeQuery("select * from t");
			while(rs.next()){
				LOG.info(rs.getInt(1));
			}
	}

	@Test
	@Ignore("Test for Bug #209, but delegated to a lower priority fix")
	public void testScanWithNullQualifier() throws Exception{
		PreparedStatement ps = rule.prepareStatement("select " +
																										"s.schemaname " +
																								 "from " +
																										"sys.sysschemas s " +
																								 "where " +
																										"schemaname is null");
		ResultSet rs = ps.executeQuery();
		List<String> rowsReturned = Lists.newArrayList();
		boolean hasRows = false;
		while(rs.next()){
			hasRows = true;
			rowsReturned.add(String.format("schemaname=%s",rs.getString(1)));
		}
		if(hasRows){
			for(String row:rowsReturned){
				LOG.info(row);
			}
			Assert.fail("rows returned! Expected 0 but was " +rowsReturned.size());
		}
	}
    @Test
    public void testProjectRestrictSysSchemas() throws Exception{
        PreparedStatement stmt = rule.prepareStatement("select " +
																													"s.schemaname,s.schemaid " +
																											 "from " +
																											 		"sys.sysschemas s " +
																											 "where " +
																													"schemaname like ? ");
        stmt.setString(1,"SYS");
        ResultSet rs = stmt.executeQuery();
        List<String> results = Lists.newArrayList();

        while(rs.next()){
            String schemaName = rs.getString(1);
            String schemaId = rs.getString(2);

            Assert.assertEquals("schemaName incorrect!","SYS",schemaName);
            Assert.assertNotNull("schemaId is null!",schemaId);
            results.add(String.format("schemaname=%s,schemaid=%s", schemaName,schemaId));
        }
        for(String result:results){
            LOG.info(result);
        }
        Assert.assertEquals("Incorrect number of rows returned",1,results.size());
    }

	@Test
	public void testQualifiedIndexScan() throws Exception{
		PreparedStatement stmt = rule.prepareStatement("select " +
																											"s.schemaname,s.schemaid " +
																										"from " +
																											"sys.sysschemas s " +
																										"where " +
																											"schemaname = ? ");
		stmt.setString(1,"SYS");
		ResultSet rs = stmt.executeQuery();
		List<String> results = Lists.newArrayList();

		while(rs.next()){
			String schemaName = rs.getString(1);
			String schemaId = rs.getString(2);

			Assert.assertEquals("schemaName incorrect!","SYS",schemaName);
			Assert.assertNotNull("schemaId is null!",schemaId);
			results.add(String.format("schemaname=%s,schemaid=%s", schemaName,schemaId));
		}
		for(String result:results){
			LOG.info(result);
		}
		Assert.assertEquals("Incorrect number of rows returned",1,results.size());
	}

	@Test
	public void testGetWithMissingFieldPreparedStatement() throws Exception{
		/*
		 * Test designed to manage the Execution Plan
		 *
		 * ProjectRestrict:
		 * 	IndexRow:
		 * 		TableScan
		 *
		 * which operates with a PreparedStatement and fields which are used as constraints, but not returned as par
		 * of the set. This ensures that the PreparedStatement works
		 */
		String correctSchemaName = "SYS";
		PreparedStatement ps = rule.prepareStatement("select s.schemaid,s.authorizationid from sys.sysschemas s where s.schemaname = ?");
		ps.setString(1,correctSchemaName);
		ResultSet rs = ps.executeQuery();
		List<String> results = Lists.newArrayList();
		while(rs.next()){
			String schemaid = rs.getString(1);
			String authId = rs.getString(2);
			Assert.assertNotNull("schemaId is null!",schemaid);
			Assert.assertNotNull("authId is null!",authId);
			results.add(String.format("schemaid=%s,authId=%s",schemaid,authId));
		}
		Assert.assertTrue("incorrect number of results returned!",results.size()>0);
		for(String result:results){
			LOG.info(result);
		}
	}

    @Test
    public void testJoinIndexedTablesWithLikePreparedStatement() throws Exception{
				/*
				 * Test for the ExecutionPlan
				 *
				 * ProjectRestrict:
				 * 	NestedLoopJoin:
				 * 		Left:
				 * 			TableScan
				 * 		Right:
				 * 			IndexRow:
				 * 				TableScan
				 */
        String correctSchemaName = "SYS";
        String correctTableName = "SYSSCHEMAS";

        PreparedStatement ps = rule.prepareStatement("select t.tablename,t.schemaid,s.schemaid,s.schemaname " +
                "from sys.systables t inner join sys.sysschemas s on t.schemaid = s.schemaid where s.schemaname like ?" +
                "and t.tablename like ?");
        ps.setString(1,correctSchemaName);
        ps.setString(2,correctTableName);
        ResultSet rs = ps.executeQuery();
        List<String> results = Lists.newArrayList();
        while(rs.next()){
            String tableName = rs.getString(1);
            String tSchemaId = rs.getString(2);
            String sSchemaId = rs.getString(3);
            String schemaName = rs.getString(4);

            Assert.assertEquals("incorrect tablename",correctTableName,tableName);
            Assert.assertEquals("schemaName incorrect!",correctSchemaName,schemaName);
            Assert.assertEquals("t.schemaId!=s.schemaid",sSchemaId,tSchemaId);
            results.add(String.format("tablename=%s,schemaname=%s,t.schemaid=%s,s.schemaid=%s",
                    tableName,schemaName,tSchemaId,sSchemaId));
        }
        for(String result:results){
            LOG.info(result);
        }
		}

	@Test
	public void testJoinSortAndProjectIndexedRows() throws Exception{
		String correctSchemaName = "SYS";
		PreparedStatement ps = rule.prepareStatement("select " +
																										"t.tablename,t.schemaid," +
																										"s.schemaid,s.schemaname " +
																				 				 "from " +
																										"sys.systables t," +
																										"sys.sysschemas s " +
																								 "where " +
																										"s.schemaid = t.schemaid " +
																										"and s.schemaname like ? " +
																								 "order by " +
																										"t.tablename");
		ps.setString(1,correctSchemaName);
		final Map<String,String> correctSort = Maps.newTreeMap();
		List<String> results = Lists.newArrayList();
		ResultSet rs = ps.executeQuery();
		while(rs.next()){
			String tableName = rs.getString(1);
			String tSchemaId = rs.getString(2);
			String sSchemaId = rs.getString(3);
			String schemaName = rs.getString(4);
			Assert.assertNotNull("no table name present!",tableName);
			Assert.assertEquals("t.schemaId != s.schemaId",tSchemaId,sSchemaId);
			Assert.assertEquals("schemaName incorrect",correctSchemaName,schemaName);

			String result = String.format("t.tablename=%s,t.schemaid=%s,s.schemaid=%s,s.schemaname=%s",
					tableName,tSchemaId,sSchemaId,schemaName);
			correctSort.put(tableName,result);
			results.add(result);
		}
		int pos =0;
		for(String correctName:correctSort.keySet()){
			Assert.assertEquals("sort is incorrect!",correctSort.get(correctName),results.get(pos));
			LOG.info(results.get(pos));
			pos++;
		}
		Assert.assertTrue("no rows returned!",results.size()>0);
	}

	@Test
	public void testJoinAndSortIndexedRows() throws Exception{
		String correctSchemaName = "SYS";
		PreparedStatement ps = rule.prepareStatement("select " +
				"t.tablename,t.schemaid," +
				"s.schemaid,s.schemaname " +
				"from " +
				"sys.systables t," +
				"sys.sysschemas s " +
				"where " +
				"s.schemaid = t.schemaid " +
				"and s.schemaname = ? " +
				"order by " +
				"t.tablename");
		ps.setString(1,correctSchemaName);
		final Map<String,String> correctSort = Maps.newTreeMap();
		List<String> results = Lists.newArrayList();
		ResultSet rs = ps.executeQuery();
		while(rs.next()){
			String tableName = rs.getString(1);
			String tSchemaId = rs.getString(2);
			String sSchemaId = rs.getString(3);
			String schemaName = rs.getString(4);
			Assert.assertNotNull("no table name present!",tableName);
			Assert.assertEquals("t.schemaId != s.schemaId",tSchemaId,sSchemaId);
			Assert.assertEquals("schemaName incorrect",correctSchemaName,schemaName);

			String result = String.format("t.tablename=%s,t.schemaid=%s,s.schemaid=%s,s.schemaname=%s",
					tableName,tSchemaId,sSchemaId,schemaName);
			correctSort.put(tableName,result);
			results.add(result);
		}
		int pos =0;
		for(String correctName:correctSort.keySet()){
			Assert.assertEquals("sort is incorrect!",correctSort.get(correctName),results.get(pos));
			LOG.info(results.get(pos));
			pos++;
		}
		Assert.assertTrue("no rows returned!",results.size()>0);
	}

	@Test
	public void testSortIndexedRows() throws Exception{
		String correctTableName = "SYSSCHEMAS";
		PreparedStatement ps = rule.prepareStatement("select " +
				"t.tablename,t.schemaid " +
				"from " +
				"sys.systables t " +
//																									"where " +
//																										"t.tablename = ?" +
				"order by " +
				"t.tablename");

		final Map<String,String> correctSort = new TreeMap<String,String>();
//		ps.setString(1,correctTableName);
		List<String> results = Lists.newArrayList();
		ResultSet rs = ps.executeQuery();
		while(rs.next()){
			String tableName = rs.getString(1);
			String schemaId = rs.getString(2);
//			Assert.assertEquals("Table name incorrect!",correctTableName,tableName);
			Assert.assertNotNull("no table name returned!",tableName);
			Assert.assertNotNull("no schema returned!",schemaId);
			results.add(String.format("tableName=%s,schemaId=%s",tableName,schemaId));
			correctSort.put(tableName, schemaId);
		}
		int pos=0;
		for(String correct:correctSort.keySet()){
			String correctResult = String.format("tableName=%s,schemaId=%s",correct,correctSort.get(correct));
			Assert.assertEquals("sorting is incorrect!",correctResult,results.get(pos));
			LOG.info(results.get(pos));
			pos++;
		}
		Assert.assertTrue("No rows returned!",results.size()>0);
	}

	@Test
	public void testJoinMultipleIndexTablesWithLikeAndSortPreparedStatement() throws Exception{
		String correctSchemaName = "SYS";
		String  correctTableName = "SYSSCHEMAS";
		PreparedStatement ps = rule.prepareStatement("select " +
																										"t.tablename as table_name,t.schemaid as table_schem," +
																										"s.schemaid,s.schemaname," +
																										"c.columnname as column_name," +
																										"c.columnnumber as ordinal_position " +
																								"from " +
																										"sys.systables t," +
																										"sys.sysschemas s," +
																										"sys.syscolumns c " +
																								"where " +
																										"s.schemaname like ? " +
																										"and t.tablename like ? " +
																										"and c.referenceid = t.tableid " +
																										"and s.schemaid = t.schemaid " +
																								"order by " +
																										"table_schem," +
																										"table_name," +
																										"ordinal_position");
		ps.setString(1,correctSchemaName);
		ps.setString(2,correctTableName);
		ResultSet rs = ps.executeQuery();
		List<String> results = Lists.newArrayList();
		while(rs.next()){
			String tableName = rs.getString(1);
			String tSchemaId = rs.getString(2);
			String sSchemaId = rs.getString(3);
			String schemaName = rs.getString(4);
			String columnName = rs.getString(5);

			Assert.assertEquals("schemaName incorrect!",correctSchemaName,schemaName);
			Assert.assertEquals("incorrect tableName",correctTableName,tableName);
			Assert.assertEquals("t.schemaid!=s.schemaid!",tSchemaId,sSchemaId);
			Assert.assertNotNull("columnName is null!",columnName);

			results.add(String.format("t.tableName=%s,t.schemaId=%s,s.schemaId=%s,s.schemaName=%s,c.columnName=%s",
					tableName,tSchemaId,sSchemaId,schemaName,columnName));
		}
		for(String result:results){
			LOG.info(result);
		}
		Assert.assertTrue("no rows returned!",results.size()>0);
	}

	@Test
	public void testJoinMultipleIndexTablesWithLikePreparedStatement() throws Exception{
		String correctSchemaName = "SYS";
		String  correctTableName = "SYSSCHEMAS";
		PreparedStatement ps = rule.prepareStatement("select " +
																										"t.tablename as table_name,t.schemaid as table_schem," +
																										"s.schemaid,s.schemaname," +
																										"c.columnname as column_name " +
																								"from " +
																										"sys.systables t," +
																										"sys.sysschemas s," +
																										"sys.syscolumns c " +
																								"where " +
																										"s.schemaname like ? " +
																										"and t.tablename like ? " +
																										"and c.referenceid = t.tableid " +
																										"and s.schemaid = t.schemaid ");
		ps.setString(1,correctSchemaName);
		ps.setString(2,correctTableName);
		ResultSet rs = ps.executeQuery();
		List<String> results = Lists.newArrayList();
		while(rs.next()){
			String tableName = rs.getString(1);
			String tSchemaId = rs.getString(2);
			String sSchemaId = rs.getString(3);
			String schemaName = rs.getString(4);
			String columnName = rs.getString(5);

			Assert.assertEquals("schemaName incorrect!",correctSchemaName,schemaName);
			Assert.assertEquals("incorrect tableName",correctTableName,tableName);
			Assert.assertEquals("t.schemaid!=s.schemaid!",tSchemaId,sSchemaId);
			Assert.assertNotNull("columnName is null!",columnName);

			results.add(String.format("t.tableName=%s,t.schemaId=%s,s.schemaId=%s,s.schemaName=%s,c.columnName=%s",
					tableName,tSchemaId,sSchemaId,schemaName,columnName));
		}
		for(String result:results){
			LOG.info(result);
		}
		Assert.assertTrue("no rows returned!",results.size()>0);
	}

	@Test
	public void testJoinIndexedTables() throws Exception{
		ResultSet rs = rule.executeQuery("select t.tablename,t.schemaid,s.schemaid,s.schemaname " +
				"from sys.systables t inner join sys.sysschemas s on t.schemaid=s.schemaid");
		List<String> results = Lists.newArrayList();
		while(rs.next()){
			String tableName = rs.getString(1);
			String tSchemaId = rs.getString(2);
			String sSchemaId = rs.getString(3);
			String schemaName = rs.getString(4);

			Assert.assertNotNull("tableName is null!",tableName);
			Assert.assertNotNull("schemaName is null!",schemaName);
			Assert.assertEquals("t.schemaId!=s.schemaid",sSchemaId,tSchemaId);
			results.add(String.format("tablename=%s,schemaname=%s,t.schemaid=%s,s.schemaid=%s",
					tableName,schemaName,tSchemaId,sSchemaId));
		}
		for(String result:results){
			LOG.info(result);
		}
	}

	@Test
	public void testQuerySpecificSchemaIdPreparedStatement() throws Exception{
		String schemaId = "80000000-00d2-b38f-4cda-000a0a412c00";
		PreparedStatement ps = rule.prepareStatement("select s.schemaname,s.schemaid from sys.sysschemas s where s.schemaid =?");
		ps.setString(1,schemaId);
		ResultSet rs = ps.executeQuery();
		int count = 0;
		while(rs.next()){
			String schemaName = rs.getString(1);
			String retSchemaId = rs.getString(2);
			Assert.assertNotNull("schema name is null!",schemaName);
			Assert.assertEquals("incorrect schema id returned!",schemaId,retSchemaId);
			count++;
		}
		Assert.assertEquals("Incorrect number of rows returned!",1,count);
	}



}
