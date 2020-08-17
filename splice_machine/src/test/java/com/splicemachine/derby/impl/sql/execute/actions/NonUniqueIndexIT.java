/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
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

package com.splicemachine.derby.impl.sql.execute.actions;

import splice.com.google.common.collect.Lists;
import com.splicemachine.derby.test.framework.*;
import com.splicemachine.homeless.TestUtils;
import com.splicemachine.test.SerialTest;
import com.splicemachine.test.SlowTest;
import com.splicemachine.test_tools.TableCreator;

import com.splicemachine.utils.Pair;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Time;
import java.util.List;

import static com.splicemachine.test_tools.Rows.row;
import static com.splicemachine.test_tools.Rows.rows;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Scott Fines
 *         Created on: 3/8/13
 */
//@Category(SerialTest.class)
public class NonUniqueIndexIT extends SpliceUnitTest {
    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher();

    public static final String CLASS_NAME = NonUniqueIndexIT.class.getSimpleName().toUpperCase();
    public static final String TABLE_NAME_1 = "A";
    public static final String TABLE_NAME_2 = "B";
    public static final String TABLE_NAME_3 = "C";
    public static final String TABLE_NAME_4 = "D";
    public static final String TABLE_NAME_5 = "E";
    public static final String TABLE_NAME_6 = "F";
    public static final String TABLE_NAME_7 = "G";
    public static final String TABLE_NAME_8 = "H";
    public static final String TABLE_NAME_9 = "I";
    public static final String TABLE_NAME_10 = "J";

    public static final String INDEX_11 = "IDX_A1";
    public static final String INDEX_21 = "IDX_B1";
    public static final String INDEX_31 = "IDX_C1";
    public static final String INDEX_41 = "IDX_D1";
    public static final String INDEX_51 = "IDX_E1";
    public static final String INDEX_61 = "IDX_F1";
    public static final String INDEX_62 = "IDX_F2";
    public static final String INDEX_81 = "IDX_H2";
    public static final String INDEX_91 = "IDX_I1";
    public static final String INDEX_J = "IDX_J1";

    protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);
    protected static SpliceTableWatcher spliceTableWatcher1 = new SpliceTableWatcher(TABLE_NAME_1,spliceSchemaWatcher.schemaName,"(name varchar(40), val int)");
    protected static SpliceTableWatcher spliceTableWatcher2 = new SpliceTableWatcher(TABLE_NAME_2,spliceSchemaWatcher.schemaName,"(n_1 varchar(40),n_2 varchar(30),val int)");
    protected static SpliceTableWatcher spliceTableWatcher3 = new SpliceTableWatcher(TABLE_NAME_3,spliceSchemaWatcher.schemaName,"(name varchar(40), val int)");
    protected static SpliceTableWatcher spliceTableWatcher4 = new SpliceTableWatcher(TABLE_NAME_4,spliceSchemaWatcher.schemaName,"(name varchar(40), val int)");
    protected static SpliceTableWatcher spliceTableWatcher5 = new SpliceTableWatcher(TABLE_NAME_5,spliceSchemaWatcher.schemaName,"(name varchar(40), val int)");
    protected static SpliceTableWatcher spliceTableWatcher6 = new SpliceTableWatcher(TABLE_NAME_6,spliceSchemaWatcher.schemaName,"(name varchar(40), val int)");
    protected static SpliceTableWatcher spliceTableWatcher7 = new SpliceTableWatcher(TABLE_NAME_7,spliceSchemaWatcher.schemaName,"(name varchar(40), val int)");
    protected static SpliceTableWatcher spliceTableWatcher8 = new SpliceTableWatcher(TABLE_NAME_8,spliceSchemaWatcher.schemaName,"(oid decimal(5),name varchar(40))");
    protected static SpliceTableWatcher spliceTableWatcher9 = new SpliceTableWatcher(TABLE_NAME_9,spliceSchemaWatcher.schemaName,"(c1 int, c2 int, c3 int)");
    protected static SpliceTableWatcher spliceTableWatcher10 = new SpliceTableWatcher(TABLE_NAME_10,spliceSchemaWatcher.schemaName,"(i int, j int)");
    //table for DB-1315 regression test
    private static SpliceTableWatcher toUpdateWatcher = new SpliceTableWatcher("DB_1315",spliceSchemaWatcher.schemaName,
            "(cint int, cchar char(10),ctime time, cdec dec(6,2), ccharForBitData char(1) for bit data, unindexed int)");

    @Override
    public String getSchemaName() {
        return spliceSchemaWatcher.schemaName;
    }

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(spliceSchemaWatcher)
            .around(spliceTableWatcher1)
            .around(spliceTableWatcher2)
            .around(spliceTableWatcher3)
            .around(spliceTableWatcher4)
            .around(spliceTableWatcher5)
            .around(spliceTableWatcher6)
            .around(spliceTableWatcher7)
            .around(spliceTableWatcher8)
            .around(spliceTableWatcher9)
            .around(toUpdateWatcher)
            .around(spliceTableWatcher10);


    @Rule public SpliceWatcher methodWatcher = new SpliceWatcher(CLASS_NAME);

    @Test(timeout=10000)
    public void testCanCreateIndexWithMultipleEntries() throws Exception{
        new SpliceIndexWatcher(TABLE_NAME_2,spliceSchemaWatcher.schemaName,INDEX_21,spliceSchemaWatcher.schemaName,"(n_1,n_2)").starting(null);
        PreparedStatement ps = methodWatcher.prepareStatement(format("insert into %s (n_1,n_2,val) values (?,?,?)",this.getTableReference(TABLE_NAME_2)));
        String n1 = "sfines";
        ps.setString(1,n1);
        String n2 = "mathematician";
        ps.setString(2,n2);
        int value = 2;
        ps.setInt(3,2);
        ps.execute();
        //now check that we can get data out for the proper key
        PreparedStatement query = methodWatcher.prepareStatement(format("select * from %s where n_1 = ? and n_2 = ?",this.getTableReference(TABLE_NAME_2)));
        query.setString(1,n1);
        query.setString(2,n2);
        ResultSet resultSet = query.executeQuery();
        List<String> results = Lists.newArrayListWithExpectedSize(1);
        while(resultSet.next()){
            String retN1 = resultSet.getString(1);
            String retN2 = resultSet.getString(2);
            int val = resultSet.getInt(3);
            Assert.assertEquals("Incorrect n1 returned!", n1, retN1);
            Assert.assertEquals("Incorrect n2 returned!", n2, retN2);
            Assert.assertEquals("Incorrect value returned!",value,val);
            results.add(String.format("n1:%s,n2:%s,value:%d",retN1,retN2,val));
        }
        Assert.assertEquals("Incorrect number of rows returned!",1,results.size());

    }
    /**
     * Basic test to ensure that a Unique Index can be used
     * to perform lookups.
     *
     * We create the Index BEFORE we add data, to ensure that
     * we don't deal with any kind of situation which might
     * arise from adding the index after data exists
     *
     * Basically, create an index, then add some data to the table,
     * then scan for data through the index and make sure that the
     * correct data returns.
     */
    @Test(timeout=10000)
    public void testCanUseIndex() throws Exception{
        new SpliceIndexWatcher(TABLE_NAME_1,spliceSchemaWatcher.schemaName,INDEX_11,spliceSchemaWatcher.schemaName,"(name)").starting(null);
        String name = "sfines";
        int value = 2;
        methodWatcher.getStatement().execute(format("insert into %s (name,val) values ('"+name+"',"+value+")",this.getTableReference(TABLE_NAME_1)));
        //now check that we can get data out for the proper key
        ResultSet resultSet = methodWatcher.executeQuery(format("select * from %s where name = '" + name + "'",this.getTableReference(TABLE_NAME_1)));
        List<String> results = Lists.newArrayListWithExpectedSize(1);
        while(resultSet.next()){
            String retName = resultSet.getString(1);
            int val = resultSet.getInt(2);
            Assert.assertEquals("Incorrect name returned!", name, retName);
            Assert.assertEquals("Incorrect value returned!",value,val);
            results.add(String.format("name:%s,value:%d",retName,val));
        }
        Assert.assertEquals("Incorrect number of rows returned!",1,results.size());
    }

    /**
     * Tests that adding an index to an existing data set will
     * result in a correct and consistent index
     *
     * Basically, add some data to the table, then create the index,
     * then perform a lookup on that same data via the index to ensure
     * that the index will find those values.
     */
    @Test(timeout=30000)
    public void testCanCreateIndexFromExistingData() throws Exception{
        String name = "sfines";
        int value =2;
        PreparedStatement ps = methodWatcher.prepareStatement("insert into "+spliceTableWatcher3+" (name,val) values (?,?)");
        ps.setString(1, name);
        ps.setInt(2, value);
        int numRowsUpdated  = ps.executeUpdate();
        Assert.assertEquals("Incorrect number of rows inserted!",1,numRowsUpdated);

        new SpliceIndexWatcher(TABLE_NAME_3,spliceSchemaWatcher.schemaName,INDEX_31,spliceSchemaWatcher.schemaName,"(name)").starting(null);
        //now check that we can get data out for the proper key
        ps = methodWatcher.prepareStatement("select * from "+ spliceTableWatcher3+ " where name = ?");
        ps.setString(1,name);
        ResultSet resultSet = ps.executeQuery();
        try{
            List<String> results = Lists.newArrayListWithExpectedSize(1);
            while(resultSet.next()){
                String retName = resultSet.getString(1);
                int val = resultSet.getInt(2);
                Assert.assertEquals("Incorrect name returned!",name,retName);
                Assert.assertEquals("Incorrect value returned!",value,val);
                results.add(String.format("name:%s,value:%d",retName,val));
            }
            Assert.assertEquals("Incorrect number of rows returned!",1,results.size());
        }finally{
            resultSet.close();
        }
    }
    /**
     * Tests that adding an index to an existing data set will
     * result in a correct and consistent index
     *
     * Basically, add some data to the table, then create the index,
     * then perform a lookup on that same data via the index to ensure
     * that the index will find those values.
     */
    @Test(timeout=15000)
    public void testCanCreateIndexFromExistingDataWithDecimalType() throws Exception{
        BigDecimal oid = BigDecimal.valueOf(2);
        String name = "sfines";
        PreparedStatement preparedStatement = methodWatcher.prepareStatement("insert into " + spliceTableWatcher8 + " values (?,?)");
        preparedStatement.setBigDecimal(1,oid);
        preparedStatement.setString(2,name);
        preparedStatement.execute();
        new SpliceIndexWatcher(TABLE_NAME_8,spliceSchemaWatcher.schemaName,INDEX_81,spliceSchemaWatcher.schemaName,"(name)").starting(null);
        //now check that we can get data out for the proper key
        preparedStatement = methodWatcher.prepareStatement("select * from "+ spliceTableWatcher8+" where oid = ?");
        preparedStatement.setBigDecimal(1,oid);

        ResultSet resultSet = preparedStatement.executeQuery();
        try{
            List<String> results = Lists.newArrayListWithExpectedSize(1);
            while(resultSet.next()){
                BigDecimal retOid = resultSet.getBigDecimal(1);
                String retName = resultSet.getString(2);
                Assert.assertEquals("Incorrect name returned!",name,retName);
                Assert.assertEquals("Incorrect oid returned!",oid,retOid);
                results.add(String.format("name:%s,value:%s",retName,retOid));
            }
            Assert.assertEquals("Incorrect number of rows returned!",1,results.size());
        }finally{
            resultSet.close();
        }
    }

    /**
     * Tests that adding an index to an existing data set will
     * result in a correct and consistent index, that we can safely add data to
     *
     * Basically, add some data, create an index off of that, and then
     * add some more data, and check to make sure that the new data shows up as well
     */
    @Test(timeout=10000)
    public void testCanCreateIndexFromExistingDataAndThenAddData() throws Exception{
        methodWatcher.getStatement().execute(format("insert into %s (name,val) values ('sfines',2)",this.getTableReference(TABLE_NAME_4)));
        new SpliceIndexWatcher(TABLE_NAME_4,spliceSchemaWatcher.schemaName,INDEX_41,spliceSchemaWatcher.schemaName,"(name)").starting(null);
        //add some more data
        String name = "jzhang";
        int value =2;
        methodWatcher.getStatement().execute(format("insert into %s (name,val) values ('"+name+"',"+value+")",this.getTableReference(TABLE_NAME_4)));

        //now check that we can get data out for the proper key
        ResultSet resultSet = methodWatcher.executeQuery(format("select * from %s where name = '" + name + "'",this.getTableReference(TABLE_NAME_4)));
        List<String> results = Lists.newArrayListWithExpectedSize(1);
        while(resultSet.next()){
            String retName = resultSet.getString(1);
            int val = resultSet.getInt(2);
            Assert.assertEquals("Incorrect name returned!",name,retName);
            Assert.assertEquals("Incorrect value returned!",value,val);
            results.add(String.format("name:%s,value:%d",retName,val));
        }
        Assert.assertEquals("Incorrect number of rows returned!",1,results.size());
    }

    //    @Test(timeout=10000))
    @Test
    public void testCanAddDuplicateAndDelete() throws Exception{
    	new SpliceIndexWatcher(TABLE_NAME_5,spliceSchemaWatcher.schemaName,INDEX_51,spliceSchemaWatcher.schemaName,"(name)").starting(null);
        String name = "uniqueTestTableYifu";
        addDuplicateAndDeleteTest(name);
    }

    @Test
    @Category(SlowTest.class)
    public void testRepeatedAddDuplicateAndDelete() throws Exception {
        new SpliceIndexWatcher(TABLE_NAME_5,spliceSchemaWatcher.schemaName,INDEX_51,spliceSchemaWatcher.schemaName,"(name)").starting(null);
        for (int i = 0; i < 50; i++) {
            System.out.println(i);
            String name = Integer.toString(i);
            addDuplicateAndDeleteTest(name);
        }
    }

    private void addDuplicateAndDeleteTest(String name) throws Exception {
        PreparedStatement ps = methodWatcher.prepareStatement(format("insert into %s (name,val) values (?,?)",spliceTableWatcher5));
        ps.setString(1,name);
        ps.setInt(2, 2);
        Assert.assertEquals("Incorrect insertion count!", 1, ps.executeUpdate());
        ps.setString(1, name);
        ps.setInt(2, 3);
        Assert.assertEquals("Incorrect insertion count!", 1, ps.executeUpdate());

        ResultSet resultSet = methodWatcher.executeQuery(format("select * from %s where name = '%s'",spliceTableWatcher5,name));
        List<String> results = Lists.newArrayListWithExpectedSize(1);
        int count=0;
        while(resultSet.next()){
            count++;
            String retName = resultSet.getString(1);
            int val = resultSet.getInt(2);
            Assert.assertEquals("Incorrect name returned!", name, retName);
            results.add(String.format("name:%s,value:%d", retName, val));
           
        }
        Assert.assertEquals("Incorrect number of rows returned!",2,results.size());
        for(String result:results){
            System.out.println(result);
        }

        int deleted = methodWatcher.getStatement().executeUpdate(format("delete from %s where name = '%s' and val = 2", spliceTableWatcher5, name));
        Assert.assertEquals("Incorrect number of rows were deleted!",1,deleted);
        assertSelectCorrect(spliceSchemaWatcher.schemaName,TABLE_NAME_5,name,1);
    }

    @Test(timeout=10000)
    public void testCanDeleteEverything() throws Exception {
        new SpliceIndexWatcher(TABLE_NAME_5,spliceSchemaWatcher.schemaName,INDEX_51,spliceSchemaWatcher.schemaName,"(name)").starting(null);
        methodWatcher.getStatement().execute(format("insert into %s.%s (name,val) values ('sfines',2)",spliceSchemaWatcher.schemaName,TABLE_NAME_5));
        ResultSet resultSet1 = methodWatcher.executeQuery("select * from "+spliceTableWatcher5.toString());
        methodWatcher.getStatement().execute(format("delete from %s",spliceTableWatcher5.toString()));
        ResultSet resultSet = methodWatcher.executeQuery("select * from "+spliceTableWatcher5.toString());
        Assert.assertTrue("Results returned!",!resultSet.next());
    }

    @Test
    public void testCanUpdateEntryIndexChanges() throws Exception{
        methodWatcher.getStatement().execute(format("insert into %s (name,val) values ('sfines',2)",this.getTableReference(TABLE_NAME_6)));
        new SpliceIndexWatcher(TABLE_NAME_6,spliceSchemaWatcher.schemaName,INDEX_61,spliceSchemaWatcher.schemaName,"(name)").starting(null);

        String oldName = "sfines";
        String newName = "jzhang";
        methodWatcher.getStatement().execute(String.format("update %s set name = '%s' where name = '%s'", this.getTableReference(TABLE_NAME_6),newName,oldName));

        ResultSet rs = methodWatcher.executeQuery(format("select * from %s where name = '%s'",this.getTableReference(TABLE_NAME_6),oldName));
        Assert.assertTrue("Rows returned incorrectly",!rs.next());
        assertSelectCorrect(spliceSchemaWatcher.schemaName,TABLE_NAME_6,newName,1);
    }

    @Test
    public void testSelectAfterUpdateWithIndexAfterInsert() throws Exception {
    	// Regression coverage for DB-3680

    	String table = "updatewithindex1";
    	String index = "updatewithindex1_idx";
    	methodWatcher.executeUpdate(format("create table %s (col1 int, col2 int, col3 int)", table));
    	methodWatcher.executeUpdate(format("insert into %s values (1,1,1)", table));
    	methodWatcher.executeUpdate(format("create index %s on %s (col2)", index, table));
    	methodWatcher.executeUpdate(format("update %s set col3 = 4", table));

    	String expected =
    		"COL1 |COL2 |COL3 |\n" +
    		"------------------\n" +
    		"  1  |  1  |  4  |";
    	
    	// Query without the index
    	ResultSet rs = methodWatcher.executeQuery(format("select * from %s --splice-properties index=%s", table, "null"));
        assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        
        // Query with index hint - results should be the same
    	rs = methodWatcher.executeQuery(format("select * from %s --splice-properties index=%s", table, index));
        assertEquals("Select returns different results with index than without", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
    }

    @Test
    public void testSelectAfterUpdateWithIndexBeforeInsert() throws Exception {
    	// Regression coverage for DB-3680

    	String table = "updatewithindex2";
    	String index = "updatewithindex2_idx";
    	methodWatcher.executeUpdate(format("create table %s (col1 int, col2 int, col3 int)", table));
    	methodWatcher.executeUpdate(format("create index %s on %s (col2)", index, table));
    	methodWatcher.executeUpdate(format("insert into %s values (1,1,1)", table));
    	methodWatcher.executeUpdate(format("update %s set col3 = 4", table));

    	String expected =
    		"COL1 |COL2 |COL3 |\n" +
    		"------------------\n" +
    		"  1  |  1  |  4  |";
    	
    	// Query without the index
    	ResultSet rs = methodWatcher.executeQuery(format("select * from %s --splice-properties index=%s", table, "null"));
        assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        
        // Query with index hint - results should be the same
    	rs = methodWatcher.executeQuery(format("select * from %s --splice-properties index=%s", table, index));
        assertEquals("Select returns different results with index than without", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
    }

    @Test
    public void testUpdateEveryRowCorrect() throws Exception {
        methodWatcher.getStatement().execute(format("insert into %s (name,val) values ('sfines',2)",spliceTableWatcher7));
        new SpliceIndexWatcher(TABLE_NAME_7,spliceSchemaWatcher.schemaName,INDEX_62,spliceSchemaWatcher.schemaName,"(name)").starting(null);

        String newName = "jzhang";

        methodWatcher.getStatement().execute(String.format("update %s set name = '%s'", this.getTableReference(TABLE_NAME_7),newName));

        ResultSet rs = methodWatcher.executeQuery(format("select * from %s where name = '%s'",this.getTableReference(TABLE_NAME_7),"sfines"));
        Assert.assertTrue("Rows returned incorrectly",!rs.next());

        assertSelectCorrect(spliceSchemaWatcher.schemaName,TABLE_NAME_7,newName,1);
    }

    /**
     * Regression test for Bug 149. Confirm that we can add a view and then add an index
     * and stuff
     */
    @Test(timeout=10000)
    public void testCanAddIndexToViewedTable() throws Exception{
/*        rule.getStatement().execute("create view t_view as select name,val from t where val > 1");
        try{
            testCanUseIndex();
        }finally{
            rule.getStatement().execute("drop view t_view");
        }
        */
    }

    @Test
    public void testCanUpdateSecondFieldWithCompoundIndex() throws Exception {
        //Regression test for Bug 867.
        PreparedStatement ps = methodWatcher.prepareStatement("insert into "+spliceTableWatcher9+" (c1,c2,c3) values (?,?,?)");
        ps.setInt(1,6);
        ps.setInt(2,2);
        ps.setInt(3,8);
        ps.execute();

        SpliceIndexWatcher indexWatcher = new SpliceIndexWatcher(spliceTableWatcher9.tableName,spliceSchemaWatcher.schemaName,INDEX_91,spliceSchemaWatcher.schemaName,"(c1,c2 desc, c3)");
        indexWatcher.starting(null);
        try{
            methodWatcher.prepareStatement("update "+ spliceTableWatcher9+" set c2 = 11 where c3 = 7").executeUpdate();
        }finally{
            indexWatcher.drop();
        }
    }

    /* DB-1644: When source table contained nulls in indexed columns selects from indexes would result in non-null values
     * showing up in wrong column. */
    @Test
    public void testIndexWithNullValuesInSourceTable() throws Exception {
        TestConnection conn = methodWatcher.getOrCreateConnection();
        try(Statement s = conn.createStatement()){
            s.executeUpdate("insert into "+TABLE_NAME_10+" values (1, null), (2,3), (4, null)");
            s.execute("create index "+INDEX_J+" on "+TABLE_NAME_10+" (j,i)");
            List<Pair<Integer, Integer>> rows=Lists.newArrayList();
            try(ResultSet rs=s.executeQuery("select j,i from "+TABLE_NAME_10+"  --SPLICE-PROPERTIES index="+INDEX_J)){
                while(rs.next()){
                    Pair<Integer, Integer> par=new Pair<>();
                    par.setFirst((Integer)rs.getObject(1));
                    par.setSecond((Integer)rs.getObject(2));
                    rows.add(par);
                }
            }
            assertEquals(3,rows.size());
            assertTrue(rows.contains(new Pair<Integer, Integer>(null,1)));
            assertTrue(rows.contains(new Pair<Integer, Integer>(null,4)));
            assertTrue(rows.contains(new Pair<>(3,2)));
        }
    }

    @Test
    @Ignore("Ignored until DB-1315 is resolved")
    public void testUpdateBitDataReturnsCorrectRowCount() throws Exception {
        /*
         * Regression test for DB-1315. This adds a bunch of indices,
         * then performs a bunch of basic updates to make sure that the updates
         * report the correct number of updated rows.
         *
         */
        TestConnection connection = methodWatcher.getOrCreateConnection();
        String indexBaseFormat = "create index %s on %s (%s)";
        Statement statement = connection.createStatement();
        statement.execute(String.format(indexBaseFormat,"b1",toUpdateWatcher,"cchar,ccharForBitData,cint"));
        statement.execute(String.format(indexBaseFormat,"b2",toUpdateWatcher,"ctime"));
        statement.execute(String.format(indexBaseFormat,"b3",toUpdateWatcher,"ctime,cint"));
        statement.execute(String.format(indexBaseFormat,"b4",toUpdateWatcher,"cint"));

        //insert some data into the data
        int numRows = 1;
        PreparedStatement ps = connection.prepareStatement(String.format("insert into %s values (?,?,?,?,?,?)",toUpdateWatcher));
        ps.setInt(1,11);ps.setString(2, "11");ps.setTime(3, new Time(11,11,11)); ps.setBigDecimal(4, new BigDecimal("1.1")); ps.setBytes(5, new byte[]{0x11});ps.setInt(6,11);ps.executeUpdate();
        //now do some update statements and make sure that they report the correct values

        String updateBaseFormat = "update %1$s set %2$s = %2$s";
        int count = statement.executeUpdate(String.format(updateBaseFormat,toUpdateWatcher,"cint"));
        Assert.assertEquals("Incorrect modified count",numRows,count);
        count = statement.executeUpdate(String.format(updateBaseFormat,toUpdateWatcher,"cchar"));
        Assert.assertEquals("Incorrect modified count", numRows, count);
        count = statement.executeUpdate(String.format(updateBaseFormat,toUpdateWatcher,"ctime"));
        Assert.assertEquals("Incorrect modified count", numRows, count);
        count = statement.executeUpdate(String.format(updateBaseFormat,toUpdateWatcher,"cdec"));
        Assert.assertEquals("Incorrect modified count", numRows, count);
        count = statement.executeUpdate(String.format(updateBaseFormat,toUpdateWatcher,"ccharForBitData"));
        Assert.assertEquals("Incorrect modified count", numRows, count);
        count = statement.executeUpdate(String.format(updateBaseFormat,toUpdateWatcher,"cint"));
        Assert.assertEquals("Incorrect modified count", numRows, count);
        count = statement.executeUpdate(String.format(updateBaseFormat,toUpdateWatcher,"cchar"));
        Assert.assertEquals("Incorrect modified count", numRows, count);
        count = statement.executeUpdate(String.format(updateBaseFormat,toUpdateWatcher,"time"));
        Assert.assertEquals("Incorrect modified count", numRows, count);
        count = statement.executeUpdate(String.format(updateBaseFormat,toUpdateWatcher,"cdec"));
        Assert.assertEquals("Incorrect modified count", numRows, count);
        count = statement.executeUpdate(String.format(updateBaseFormat,toUpdateWatcher,"ccharForBitData"));
        Assert.assertEquals("Incorrect modified count", numRows, count);
    }

    private void assertSelectCorrect(String schemaName, String tableName, String name, int size) throws Exception{
        ResultSet resultSet = methodWatcher.executeQuery(format("select * from %s.%s where name = '%s'", schemaName,tableName,name));
        List<String> results = Lists.newArrayListWithExpectedSize(1);
        while(resultSet.next()){
            String retName = resultSet.getString(1);
            int val = resultSet.getInt(2);
            Assert.assertEquals("Incorrect name returned!", name, retName);
            results.add(String.format("name:%s,value:%d",retName,val));
        }
        Assert.assertEquals("Incorrect number of rows returned!",size,results.size());
    }
}
