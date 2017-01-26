/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.derby.impl.sql.execute.operations;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.spark_project.guava.collect.Lists;
import org.spark_project.guava.collect.Maps;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceTableWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;

public class IndexRowToBaseRowOperationIT extends SpliceUnitTest { 
	protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher();
	private static final Logger LOG = Logger.getLogger(IndexRowToBaseRowOperationIT.class);

    private static final String CLASSNAME = IndexRowToBaseRowOperationIT.class.getSimpleName();
    protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASSNAME);
	protected static SpliceTableWatcher spliceTableWatcher = new SpliceTableWatcher("ORDER_FACT", CLASSNAME,"(i int)");
	
	@ClassRule 
	public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
		.around(spliceSchemaWatcher)
		.around(spliceTableWatcher);
	
	@Rule public SpliceWatcher methodWatcher = new SpliceWatcher();

	@Test
	public void testScanSysTables() throws Exception{
		ResultSet rs = methodWatcher.executeQuery("select tablename from sys.systables where tablename like 'ORDER_FACT'");
		int count=0;
		while(rs.next()){
			LOG.trace(rs.getString(1));
			count++;
		}
		Assert.assertTrue("incorrect row count returned!",count>0);
	}

	public void testScanSysConglomerates() throws Exception{
		ResultSet rs = methodWatcher.executeQuery("select * from sys.sysconglomerates");
		int count = 0;
		while(rs.next()){ // TODO No Test Here
			LOG.trace(String.format("schemaId=%s,tableId=%s,conglom#=%d,conglomName=%s,isIndex=%b,isConstraint=%b,conglomId=%s",
			rs.getString(1),rs.getString(2),rs.getInt(3),rs.getString(4),rs.getBoolean(5),rs.getBoolean(7),rs.getString(8)));
			count++;
		}
		Assert.assertTrue("incorrect number of rows returned", count > 0);
	}

	@Test
	public void testRestrictScanSysConglomerates() throws Exception{
		ResultSet rs = methodWatcher.executeQuery("select count(*) from sys.sysconglomerates");
		int count = 0;
		while(rs.next()){
			count++;
			LOG.trace(String.format("count=%d",rs.getInt(1)));
		}
		Assert.assertTrue("incorrect number of rows returned", count > 0);
	}
	
	@Test
	public void testJoinSysSchemasAndSysTables() throws Exception{
		ResultSet rs = methodWatcher.executeQuery("select s.schemaid, t.tableid from sys.sysschemas s join sys.systables t on s.schemaid=t.schemaid");
		int count = 0;
		while(rs.next()){
			count++;
			LOG.info(String.format("schemaid=%s,tableid=%s",rs.getString(1),rs.getString(2)));
		}
		Assert.assertTrue("incorrect number of rows returned", count > 0);
	}
	
	@Test
	public void testScanWithNullQualifier() throws Exception{
		PreparedStatement ps = methodWatcher.prepareStatement("select s.schemaname from sys.sysschemas s where schemaname is null");
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
        PreparedStatement stmt = methodWatcher.prepareStatement("select " +
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
        PreparedStatement stmt = methodWatcher.prepareStatement("select " +
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
		PreparedStatement ps = methodWatcher.prepareStatement("select s.schemaid,s.authorizationid from sys.sysschemas s where s.schemaname = ?");
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

        PreparedStatement ps = methodWatcher.prepareStatement("select t.tablename,t.schemaid,s.schemaid,s.schemaname " +
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
        String correctSchemaName = getSchemaName();
        PreparedStatement ps = methodWatcher.prepareStatement("select " +
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
		String correctSchemaName = getSchemaName();
        final String sql = "select " +
                "t.tablename,t.schemaid," +
                "s.schemaid,s.schemaname " +
                "from " +
                "sys.systables t," +
                "sys.sysschemas s " +
                "where " +
                "s.schemaid = t.schemaid " +
                "and s.schemaname = ? " +
                "order by " +
                "t.tablename";
        PreparedStatement ps = methodWatcher.prepareStatement(sql);
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
	@Ignore("Not working when multi-threaed for me - JL")
	public void testSortIndexedRows() throws Exception{
		PreparedStatement ps = methodWatcher.prepareStatement("select " +
				"t.tablename,t.schemaid " +
				"from " +
				"sys.systables t " +
				"order by " +
				"t.tablename");

		final Map<String,String> correctSort = new TreeMap<String,String>();
		List<String> results = Lists.newArrayList();
		ResultSet rs = ps.executeQuery();
		while(rs.next()){
			String tableName = rs.getString(1);
			String schemaId = rs.getString(2);
			Assert.assertNotNull("no table name returned!",tableName);
			Assert.assertNotNull("no schema returned!",schemaId);
			results.add(String.format("tableName=%s,schemaId=%s",tableName,schemaId));
			correctSort.put(tableName, schemaId);
		}
		int pos=0;
		for(String correct:correctSort.keySet()){
			String correctResult = String.format("tableName=%s,schemaId=%s",correct,correctSort.get(correct));
			Assert.assertEquals("sorting is incorrect! " + correctResult + " : " + results.get(pos),correctResult,results.get(pos));
			LOG.info(results.get(pos));
			pos++;
		}
		Assert.assertTrue("No rows returned!",results.size()>0);
	}

	@Test
	public void testRestrictColumnsPreparedStatement() throws Exception{
		PreparedStatement ps = methodWatcher.prepareStatement("select " +
				"t.tablename as table_name," +
				"c.columnname " +
				"from " +
				 "sys.systables t," +
				 "sys.syscolumns c " +
				"where " +
				"c.referenceid = t.tableid " +
				"and c.columnname like ?");
		ps.setString(1,"%");

		ResultSet rs = ps.executeQuery();
		List<String> rows = Lists.newArrayList();
		while(rs.next()){
			rows.add(String.format("table:%s,column:%s",rs.getString(1),rs.getString(2)));
		}
		for(String row:rows){
			LOG.info(row);
		}
		Assert.assertTrue("incorrect rows returned!",rows.size()>0);

	}

    @Test
    public void testRestrictSortedColumns() throws Exception{
        PreparedStatement ps = methodWatcher.prepareStatement("select " +
                "s.schemaname as table_schem," +
                "t.tablename as table_name," +
                "c.columnname as column_name," +
                "c.columnnumber as ordinal_position " +
                "from " +
                "sys.sysschemas s, " +
                "sys.systables t," +
                "sys.syscolumns c " +
                "where " +
                "c.referenceid = t.tableid " +
                "and s.schemaid = t.schemaid " +
                "and ((1=1) or '%' is not null)" +
                "and s.schemaname like 'SYS' " +
                "and t.tablename like 'SYSSCHEMAS' " +
                "and c.columnname like ? " +
                "order by " +
                "table_schem," +
                "table_name," +
                "ordinal_position");
        ps.setString(1,"%");
        ResultSet rs = ps.executeQuery();
        List<String> rows = Lists.newArrayList();
        while(rs.next()){
            rows.add(String.format("schema:%s,table:%s,column:%s,colNumber:%d",rs.getString(1),rs.getString(2),rs.getString(3),rs.getInt(4)));
        }
        for(String row:rows){
            LOG.info(row);
        }
        Assert.assertTrue("incorrect rows returned!",rows.size()>0);
    }

    @Test
    public void testRestrictColumns() throws Exception{
        final String sql = "select " +
                "c.columnname," +
                "t.tablename," +
                "s.schemaname " +
                "from " +
                "sys.syscolumns c," +
                "sys.systables t," +
                "sys.sysschemas s " +
                "where " +
                "c.referenceid = t.tableid " +
                "and s.schemaid = t.schemaid " +
                "and t.tablename = 'ORDER_FACT' " +
                "and s.schemaname like 'INDEX%' " +
                "and c.columnname like '%'";
        ResultSet rs = methodWatcher.executeQuery(sql);
        int count =0;
        while(rs.next()){
            LOG.info(rs.getString(1));
            count++;
        }
        Assert.assertTrue("incorrect rows returned!",count>0);
    }

	@Test
	public void testJoinMultipleIndexTablesWithLikeAndSortPreparedStatement() throws Exception{
		String correctSchemaName = "SYS";
		String  correctTableName = "SYSSCHEMAS";
        PreparedStatement ps = methodWatcher.prepareStatement("select " +
                "cast ('' as varchar(128)) as table_cat," +
                "s.schemaname as table_schem," +
                "t.tablename as table_name," +
                "c.columnname as column_name," +
                "t.schemaid," +
                "c.columnnumber as ordinal_position " +
                "from" +
                " sys.sysschemas s" +
                " ,sys.systables t" +
                " ,sys.syscolumns c" +
                " where " +
                "c.referenceid = t.tableid " +
                "and s.schemaid = t.schemaid " +
                "and ((1=1) or ? is not null) " +
                "and s.schemaname like ? " +
                "and t.tablename like ? " +
                "and c.columnname like ? " +
                "order by " +
                "table_schem," +
                "table_name," +
                "ordinal_position");
        ps.setString(1,"%");
        ps.setString(2,correctSchemaName);
        ps.setString(3,correctTableName);
        ps.setString(4,"%");
        ResultSet rs = ps.executeQuery();
        List<String> results = Lists.newArrayList();
        while(rs.next()){
            rs.getString(1);
            String schemaName = rs.getString(2);
            String tableName = rs.getString(3);
			String columnName = rs.getString(4);
			String tSchemaId = rs.getString(5);
			Integer columnNumber = rs.getInt(6);

			Assert.assertEquals("schemaName incorrect!",correctSchemaName,schemaName);
			Assert.assertEquals("incorrect tableName",correctTableName,tableName);
			Assert.assertNotNull("no schema returned!",tSchemaId);
			Assert.assertNotNull("no columnName!",columnName);
			Assert.assertNotNull("no columnNumber!",columnNumber);

			results.add(String.format("t.tableName=%s,t.schemaId=%s,s.schemaName=%s,c.columnName=%s,c.columnNumber=%d",
					tableName,tSchemaId,schemaName,columnName,columnNumber));
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
        PreparedStatement ps = methodWatcher.prepareStatement("select " +
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
		ResultSet rs = methodWatcher.executeQuery("select t.tablename,t.schemaid,s.schemaid,s.schemaname " +
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


}
