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
import com.splicemachine.db.shared.common.reference.SQLState;
import com.splicemachine.derby.test.framework.*;
import com.splicemachine.homeless.TestUtils;
import com.splicemachine.test.SerialTest;
import org.apache.log4j.Logger;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.sql.*;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

/**
 * @author Scott Fines
 *         Created on: 3/7/13
 */
//@Category(SerialTest.class) //left serial until DB-1777 is resolved
public class UniqueIndexIT extends SpliceUnitTest{

    private static final String CLASS_NAME=UniqueIndexIT.class.getSimpleName().toUpperCase();

    private static final String TABLE_A="A", TABLE_B="B", TABLE_C="C", TABLE_D="D", TABLE_E="E",
            TABLE_F="F", TABLE_G="G", TABLE_H="H", TABLE_I="I", TABLE_J="J", TABLE_K="K",
            TABLE_M="M", TABLE_2="T2", TABLE_3="Tab3",

    INDEX_A="IDX_A1", INDEX_B="IDX_B1", INDEX_C="IDX_C1", INDEX_D="IDX_D1", INDEX_E="IDX_E1",
            INDEX_F="IDX_F1", INDEX_G="IDX_G1", INDEX_K="IDX_K1", INDEX_M="IDX_M1", INDEX_2="IDX_2",
            INDEX_3="IDX_3";

    @Override
    public String getSchemaName(){
        return CLASS_NAME;
    }

    @ClassRule
    public static TestRule chain=RuleChain.outerRule(new SpliceWatcher())
            .around(new SpliceSchemaWatcher(CLASS_NAME))
            .around(new SpliceTableWatcher(TABLE_A,CLASS_NAME,"(name varchar(40), val int)"))
            .around(new SpliceTableWatcher(TABLE_B,CLASS_NAME,"(name varchar(40), val int)"))
            .around(new SpliceTableWatcher(TABLE_C,CLASS_NAME,"(name varchar(40), val int)"))
            .around(new SpliceTableWatcher(TABLE_D,CLASS_NAME,"(name varchar(40), val int)"))
            .around(new SpliceTableWatcher(TABLE_E,CLASS_NAME,"(name varchar(40), val int)"))
            .around(new SpliceTableWatcher(TABLE_F,CLASS_NAME,"(name varchar(40), val int)"))
            .around(new SpliceTableWatcher(TABLE_G,CLASS_NAME,"(name varchar(40), val int)"))
            .around(new SpliceTableWatcher(TABLE_H,CLASS_NAME,"(name varchar(40), val int, constraint FOO unique(val))"))
            .around(new SpliceTableWatcher(TABLE_I,CLASS_NAME,"(name varchar(40), val int)"))
            .around(new SpliceTableWatcher(TABLE_J,CLASS_NAME,"(name varchar(40), val int)"))
            .around(new SpliceTableWatcher(TABLE_K,CLASS_NAME,"(name varchar(40), val int)"))
            .around(new SpliceTableWatcher(TABLE_M,CLASS_NAME,"(name varchar(40), val int)"));


    @Rule
    public SpliceWatcher methodWatcher=new SpliceWatcher(CLASS_NAME);

    private Connection conn;

    @Before
    public void setUpTest() throws Exception{
        conn=methodWatcher.getOrCreateConnection();
        conn.setAutoCommit(false);
        conn.setSchema(CLASS_NAME);
    }

    @After
    public void tearDownTest() throws Exception{
        conn.rollback();
    }

    /**
     * Basic test to ensure that a Unique Index can be used
     * to perform lookups.
     * <p/>
     * We create the Index BEFORE we add data, to ensure that
     * we don't deal with any kind of situation which might
     * arise from adding the index after data exists
     * <p/>
     * Basically, create an index, then add some data to the table,
     * then scan for data through the index and make sure that the
     * correct data returns.
     */
    @Test(timeout=10000)
    public void testCanUseUniqueIndex() throws Exception{
        try(Statement s=conn.createStatement()){
            s.execute("create unique index "+INDEX_A+" on "+TABLE_A+"(name)");
//        new SpliceIndexWatcher(TABLE_A,CLASS_NAME, INDEX_A,CLASS_NAME,"(name)",true).starting(null);
            //now add some data
            String name="sfines";
            int value=2;
            s.execute(format("insert into %s (name,val) values ('%s',%s)",TABLE_A,name,value));

            //now check that we can get data out for the proper key
            try(ResultSet resultSet=s.executeQuery(format("select * from %s where name = '%s'",TABLE_A,name))){
                List<String> results=Lists.newArrayListWithExpectedSize(1);
                while(resultSet.next()){
                    String retName=resultSet.getString(1);
                    int val=resultSet.getInt(2);
                    Assert.assertEquals("Incorrect name returned!",name,retName);
                    Assert.assertEquals("Incorrect value returned!",value,val);
                    results.add(String.format("name:%s,value:%d",retName,val));
                }
                Assert.assertEquals("Incorrect number of rows returned!",1,results.size());
            }
        }
    }

    /**
     * Tests that adding an index to an existing data set will
     * result in a correct and consistent index
     * <p/>
     * Basically, add some data to the table, then create the index,
     * then perform a lookup on that same data via the index to ensure
     * that the index will find those values.
     */
    @Test(timeout=20000)
    public void testCanCreateIndexFromExistingData() throws Exception{
        String name="sfines";
        int value=2;
        try(Statement s=conn.createStatement()){
            s.execute(format("insert into %s (name,val) values ('%s',%s)",TABLE_B,name,value));
            //create the index
            s.execute("create unique index "+INDEX_B+" on "+TABLE_B+"(name)");

            //now check that we can get data out for the proper key
            try(ResultSet resultSet=s.executeQuery(format("select * from %s where name = '%s'",TABLE_B,name))){
                List<String> results=Lists.newArrayListWithExpectedSize(1);
                while(resultSet.next()){
                    String retName=resultSet.getString(1);
                    int val=resultSet.getInt(2);
                    Assert.assertEquals("Incorrect name returned!",name,retName);
                    Assert.assertEquals("Incorrect value returned!",value,val);
                    results.add(String.format("name:%s,value:%d",retName,val));
                }
                Assert.assertEquals("Incorrect number of rows returned!",1,results.size());
            }
        }
    }

    /**
     * Tests that adding an index to an existing data set will
     * result in a correct and consistent index, that we can safely add data to
     * <p/>
     * Basically, add some data, create an index off of that, and then
     * add some more data, and check to make sure that the new data shows up as well
     */
    @Test(timeout=10000)
    public void testCanCreateIndexFromExistingDataAndThenAddData() throws Exception{
        try(Statement s=conn.createStatement()){
            s.execute(format("insert into %s (name,val) values ('%s',%s)",TABLE_C,"sfines",2));
            //create the index
            s.execute("create unique index "+INDEX_C+" on "+TABLE_C+"(name)");
            //add some more data
            String name="jzhang";
            int value=2;
            s.execute(format("insert into %s (name,val) values ('%s',%s)",TABLE_C,name,value));

            //now check that we can get data out for the proper key
            try(ResultSet resultSet=s.executeQuery(format("select * from %s where name = '%s'",TABLE_C,name))){
                List<String> results=Lists.newArrayListWithExpectedSize(1);
                while(resultSet.next()){
                    String retName=resultSet.getString(1);
                    int val=resultSet.getInt(2);
                    Assert.assertEquals("Incorrect name returned!",name,retName);
                    Assert.assertEquals("Incorrect value returned!",value,val);
                    results.add(String.format("name:%s,value:%d",retName,val));
                }
                Assert.assertEquals("Incorrect number of rows returned!",1,results.size());
            }
        }
    }

    /**
     * Tests that the uniqueness constraint is correctly managed.
     * <p/>
     * Basically, create an index, add some data, then try and
     * add some duplicate data, and validate that the duplicate
     * data cannot succeed.
     */
    @Test(expected=SQLException.class, timeout=10000)
    public void testViolateUniqueConstraint() throws Exception{
        try(Statement s=conn.createStatement()){
            s.execute("create unique index "+INDEX_D+" on "+TABLE_D+"(name)");
            String name="sfines";
            int value=2;
            s.execute(format("insert into %s (name,val) values ('%s',%s)",TABLE_D,name,value));
            try{
                s.execute(format("insert into %s (name,val) values ('%s',%s)",TABLE_D,name,value));
            }catch(SQLException se){
                Logger.getLogger(UniqueIndexIT.class).error(se);
                if(se.getMessage().contains("unique"))
                    throw se;
            }
            fail("Did not report a duplicate key violation!");
        }
    }

    /**
     * Tests that we can safely drop the index, and constraints
     * will also be dropped.
     * <p/>
     * Basically, create an index, add some data, validate
     * that the uniqueness constraint holds, then drop the index
     * and validate that A) the uniqueness constraint doesn't hold
     * and B) the index is no longer used in the lookup.
     */
    @Test(timeout=10000)
    public void testCanDropIndex() throws Exception{

        //ensure that the uniqueness constraint holds
        try(Statement s=conn.createStatement()){
            s.execute("create unique index "+INDEX_E+" on "+TABLE_E+"(name)");
            String name="sfines";
            int value=2;
            s.execute(format("insert into %s (name,val) values ('%s',%s)",TABLE_E,name,value));
            try{
                s.execute(format("insert into %s (name,val) values ('%s',%s)",TABLE_E,name,value));
                fail("Uniqueness constraint violated");
            }catch(SQLException se){
                Assert.assertEquals(SQLState.LANG_DUPLICATE_KEY_CONSTRAINT,se.getSQLState());
            }
            s.execute("drop index "+INDEX_E);

            //validate that we can add duplicates now
            s.execute(format("insert into %s (name,val) values ('%s',%s)",TABLE_E,name,value));

            try(ResultSet resultSet=s.executeQuery(format("select * from %s where name = '%s'",TABLE_E,name))){
                List<String> results=Lists.newArrayListWithExpectedSize(1);
                while(resultSet.next()){
                    String retName=resultSet.getString(1);
                    int val=resultSet.getInt(2);
                    Assert.assertEquals("Incorrect name returned!",name,retName);
                    Assert.assertEquals("Incorrect value returned!",value,val);
                    results.add(String.format("name:%s,value:%d",retName,val));
                }
                Assert.assertEquals("Incorrect number of rows returned!",2,results.size());
            }
        }
    }

    /**
     * Tests that we can safely delete a record, and have it
     * percolate through to the index.
     * <p/>
     * Basically, create an index, add some data, check if its
     * present, then delete some data and check that it's not
     * there anymore
     */
//    @Test(timeout=10000)
    @Test
    public void testCanDeleteEntry() throws Exception{
        try(Statement s=conn.createStatement()){
            // given
            s.executeUpdate("create table L (name varchar(40), val int)");
            s.executeUpdate("create index L_INDEX on L (name)");
            s.executeUpdate("insert into L (name,val) values ('testName',2)");
            assertCountEquals(1L,s,"select * from L");
            assertCountEquals(1L,s,"select * from L --SPLICE-PROPERTIES index=L_INDEX");
            assertCountEquals(1l,s,format("select * from L where name = 'testName'"));
            assertCountEquals(1l,s,format("select * from L --SPLICE-PROPERTIES index=L_INDEX\n where name = 'testName'"));
            // when
            s.executeUpdate("delete from L where name = 'testName'");
            // then
            assertCountEquals(0L,s,"select * from L");
            assertCountEquals(0L,s,"select * from L --SPLICE-PROPERTIES index=L_INDEX");
            assertCountEquals(0l,s,format("select * from L where name = 'testName'"));
            assertCountEquals(0l,s,format("select * from L --SPLICE-PROPERTIES index=L_INDEX\n where name = 'testName'"));
        }
    }

    @Test
    @Ignore("still failing as of Feb 29 2016")
    public void testRepeatedCanDeleteEntry() throws Exception{
        for(int i=0;i<100;i++){
            testCanDeleteEntry();
        }
    }

    /**
     * DB-1020
     * Tests that we can safely delete a record, and have it
     * percolate through to the index.
     * <p/>
     * NULL values should NOT be deleted.
     */
    @Test
    public void testCanDeleteIndexWithNulls() throws Exception{
        try(Statement s=conn.createStatement()){
            s.execute("create unique index "+INDEX_F+" on "+TABLE_I+"(val)");
        }

        try(PreparedStatement ps=conn.prepareStatement(String.format("insert into %s (name,val) values (?,?)",TABLE_I))){
            ps.setString(1,"sfines"); ps.setInt(2,2); ps.addBatch();
            ps.setString(1,"lfines"); ps.setInt(2,-2); ps.addBatch();
            ps.setNull(1,Types.VARCHAR); ps.setNull(2,Types.INTEGER); ps.addBatch();
            ps.setString(1,"0"); ps.setInt(2,0); ps.addBatch();
            int[] updated=ps.executeBatch();
            Assert.assertEquals("Incorrect update number!",4,updated.length);
            System.out.println(Arrays.toString(updated));
        }

        String query=format("select * from %s",TABLE_I);
        try(Statement s=conn.createStatement()){
            try(ResultSet rs=s.executeQuery(query)){
                TestUtils.FormattedResult fr=TestUtils.FormattedResult.ResultFactory.convert(query,rs);
                Assert.assertEquals(fr.toString(),4,fr.size());
            }

            s.execute(format("delete from %s where val > 0",TABLE_I));
            try(ResultSet rs=s.executeQuery(query)){
                TestUtils.FormattedResult fr=TestUtils.FormattedResult.ResultFactory.convert(query,rs);
                Assert.assertEquals(fr.toString(),3,fr.size());
            }

            s.execute(format("delete from %s where val < 0",TABLE_I));
            try(ResultSet rs=s.executeQuery(query)){
                TestUtils.FormattedResult fr=TestUtils.FormattedResult.ResultFactory.convert(query,rs);
                Assert.assertEquals(fr.toString(),2,fr.size());
            }
        }
    }

    /**
     * DB-1020
     * Tests that we can safely delete a record, and have it
     * percolate through to the unique index.
     * <p/>
     * NULL values should NOT be deleted.
     */
    @Test
    public void testCanDeleteUniqueIndexWithoutNulls() throws Exception{
        try(Statement s=conn.createStatement()){
            s.execute("create unique index "+INDEX_F+" on "+TABLE_J+"(val)");
            s.execute(format("insert into %s (name,val) values ('%s',%s)",TABLE_J,"sfines",-2));
            s.execute(format("insert into %s (name,val) values ('%s',%s)",TABLE_J,"lfines",2));
            s.execute(format("insert into %s (name,val) values (null,null)",TABLE_J));
            s.execute(format("insert into %s (name,val) values ('0',0)",TABLE_J));

            String query=format("select * from %s",TABLE_J);
            try(ResultSet rs=methodWatcher.executeQuery(query)){
                TestUtils.FormattedResult fr=TestUtils.FormattedResult.ResultFactory.convert(query,rs);
                Assert.assertEquals(fr.toString(),4,fr.size());
            }

            s.execute(format("delete from %s where val > 0",TABLE_J));
            try(ResultSet rs=s.executeQuery(query)){

                TestUtils.FormattedResult fr=TestUtils.FormattedResult.ResultFactory.convert(query,rs);
                Assert.assertEquals(fr.toString(),3,fr.size());
            }

            s.execute(format("delete from %s where val < 0",TABLE_J));
            try(ResultSet rs=methodWatcher.executeQuery(query)){
                TestUtils.FormattedResult fr=TestUtils.FormattedResult.ResultFactory.convert(query,rs);
                Assert.assertEquals(fr.toString(),2,fr.size());
            }
        }
    }

    /**
     * DB-1092
     * Tests that we can delete a row with a non-null value in unique index column then insert same row.
     */
    @Test
    public void testCanDeleteUniqueIndexWithoutNulls2() throws Exception{
        String indexName="IDX2";
        String tableName="T2";
        try(Statement s=conn.createStatement()){
            s.execute("create table "+tableName+"(name varchar(40),val int)");
            s.execute("create unique index "+indexName+" on "+tableName+"(val)");
            s.execute(format("insert into %s (name,val) values ('%s',%s)",
                    tableName,"sfines",-2));
            s.execute(format("insert into %s (name,val) values ('%s',%s)",
                    tableName,"lfines",2));
            s.execute(format("insert into %s (name,val) values ('%s',%s)",
                    tableName,"MyNull",null));
            s.execute(format("insert into %s (name,val) values ('0',0)",
                    tableName));

            String query=format("select * from %s",tableName);
            try(ResultSet rs=s.executeQuery(query)){
                TestUtils.FormattedResult fr=TestUtils.FormattedResult.ResultFactory.convert(query,rs);
                Assert.assertEquals(fr.toString(),4,fr.size());
            }

            s.execute(format("delete from %s",tableName));
            try(ResultSet rs=methodWatcher.executeQuery(query)){
                TestUtils.FormattedResult fr=TestUtils.FormattedResult.ResultFactory.convert(query,rs);
                Assert.assertEquals(fr.toString(),0,fr.size());
            }

            s.execute(format("insert into %s (name,val) values ('%s',%s)",tableName,"0",0));
            try(ResultSet rs=s.executeQuery(query)){
                TestUtils.FormattedResult fr=TestUtils.FormattedResult.ResultFactory.convert(query,rs);
                Assert.assertEquals(fr.toString(),1,fr.size());
            }

            s.execute(format("insert into %s (name,val) values ('%s',%s)",tableName,"sfines",-2));
            try(ResultSet rs=s.executeQuery(query)){
                TestUtils.FormattedResult fr=TestUtils.FormattedResult.ResultFactory.convert(query,rs);
                Assert.assertEquals(fr.toString(),2,fr.size());
            }
        }
    }

    /**
     * DB-1092
     * Tests that we can delete a row with null in unique index column then insert same row.
     */
    @Test
    public void testCanDeleteUniqueIndexWithNulls() throws Exception{
        String indexName="IDX1";
        String tableName="T1";
        try(Statement s = conn.createStatement()){

            s.execute("create table "+ tableName+" (name varchar(40),val int)");
            s.execute("create unique index "+indexName+" on "+ tableName+"(val)");
            s.execute(format("insert into %s (name,val) values ('%s',%s)", tableName,"sfines",-2));
            s.execute(format("insert into %s (name,val) values ('%s',%s)", tableName,"lfines",2));
            s.execute(format("insert into %s (name,val) values ('%s',%s)", tableName,"MyNull",null));
            s.execute(format("insert into %s (name,val) values ('0',0)", tableName));

            String query=format("select * from %s",tableName);
            try(ResultSet rs=s.executeQuery(query)){
                TestUtils.FormattedResult fr=TestUtils.FormattedResult.ResultFactory.convert(query,rs);
                Assert.assertEquals(fr.toString(),4,fr.size());
            }

            s.execute(format("delete from %s where val is null",tableName));
            try(ResultSet rs=s.executeQuery(query)){
                TestUtils.FormattedResult fr=TestUtils.FormattedResult.ResultFactory.convert(query,rs);
                Assert.assertEquals(fr.toString(),3,fr.size());
            }

            s.execute(format("insert into %s (name,val) values ('%s',%s)", tableName,"MyNull",null));
            try(ResultSet rs=methodWatcher.executeQuery(query)){
                TestUtils.FormattedResult fr=TestUtils.FormattedResult.ResultFactory.convert(query,rs);
                Assert.assertEquals(fr.toString(),4,fr.size());
            }
        }
    }

    /**
     * DB-1110
     * Tests create a descending index.
     */
    @Test
    public void testCanCreateDescendingIndex() throws Exception{
        String indexName="descinx";
        String tableName="T";
        try(Statement s = conn.createStatement()){
            s.execute("create table "+ tableName+"(c1 int, c2 smallint)");
            s.execute(format("insert into %s (c1,c2) values (%s,%s)", tableName,8,12));
            s.execute(format("insert into %s (c1,c2) values (%s,%s)", tableName,56,-3));

            String query=format("select min(c2) from %s",tableName);
            try(ResultSet rs=s.executeQuery(query)){
                TestUtils.FormattedResult fr=TestUtils.FormattedResult.ResultFactory.convert(query,rs);
                Assert.assertEquals(fr.toString(),1,fr.size());
            }


            s.execute("create index "+ indexName+" on "+tableName+"(c2 desc, c1)");

            try(ResultSet rs=s.executeQuery(query)){
                TestUtils.FormattedResult fr=TestUtils.FormattedResult.ResultFactory.convert(query,rs);
                Assert.assertEquals(fr.toString(),1,fr.size());
            }
        }
    }

    @Test()
    public void testCanDeleteThenInsertEntryInTransaction() throws Exception{
        try(Statement s = conn.createStatement()){
            s.execute("create unique index "+INDEX_F+" on "+ TABLE_F+"(name)");
            String name="sfines";
            int value=2;
            s.execute(format("insert into %s (name, val) values ('%s', %s)",TABLE_F,name, value));
            s.execute(format("delete from %s",TABLE_F,name));
            s.execute(format("insert into %s (name, val) values ('%s', %s)",TABLE_F,name,value));
            try(ResultSet rs=s.executeQuery(format("select * from %s where name = '%s'",TABLE_F,name))){
                List<String> results=Lists.newArrayListWithExpectedSize(1);
                while(rs.next()){
                    String retName=rs.getString(1);
                    int val=rs.getInt(2);
                    results.add(String.format("name:%s,value:%d",retName,val));
                }
                Assert.assertEquals("Incorrect number of rows returned!",1,results.size());
            }
        }
    }

    @Test(timeout=10000)
    public void testCanInsertThenDeleteEntryInTransaction() throws Exception{
        try(Statement s = conn.createStatement()){
            s.execute("create unique index "+INDEX_M+" on "+ TABLE_M+"(name)");
            insertThenDelete(TABLE_M);
        }
    }


    @Test(timeout=10000)
    public void testCanInsertThenDeleteThenInsertAgainInTransaction() throws Exception{
        try(Statement s = conn.createStatement()){
            s.execute("create unique index "+INDEX_K+" on "+TABLE_K+"(name)");
        }
        insertThenDelete(TABLE_K);
        insertThenDelete(TABLE_K);
    }

    //    @Test(timeout= 10000)
    @Test
    public void testCanUpdateEntryIndexChanges() throws Exception{
        try(Statement s = conn.createStatement()){
            s.execute("create unique index "+INDEX_G+" on "+ TABLE_G+"(name)");
            String name="sfines";
            int value=2;
            s.execute(format("insert into %s (name,val) values ('%s',%s)",TABLE_G,name,value));
            try(ResultSet rs1=s.executeQuery(format("select * from %s where name = '%s'",TABLE_G,name))){
                while(rs1.next()){
                    System.out.println("The result set before update is "+rs1.getString(1)+" and "+rs1.getInt(2));
                }
            }
            String newName="jzhang";
            s.execute(format("update %s set name = '%s' where name = '%s'",TABLE_G,newName,name));

            try(ResultSet rs=s.executeQuery(format("select * from %s where name = '%s'",TABLE_G,name))){
                while(rs.next()){
                    System.out.println("The result set after update is "+rs.getString(1)+" and "+rs.getInt(2));
                }
                Assert.assertTrue("Rows are returned incorrectly",!rs.next());
            }

            try(ResultSet rs=s.executeQuery(format("select * from %s where name = '%s'",TABLE_G,newName))){
                List<String> results=Lists.newArrayListWithExpectedSize(1);
                while(rs.next()){
                    String retName=rs.getString(1);
                    int val=rs.getInt(2);
                    Assert.assertEquals("Incorrect name returned!",newName,retName);
                    results.add(String.format("name:%s,value:%d",retName,val));
                }
                Assert.assertEquals("Incorrect number of rows returned!",1,results.size());
            }
        }
    }

    @Test(timeout=10000, expected=SQLException.class)
    public void testUniqueInTableCreationViolatesPrimaryKey() throws Exception{
        String name="sfines";
        int value=2;
        try(Statement s = conn.createStatement()){
            s.execute(format("insert into %s (name,val) values ('%s',%s)",TABLE_H,name,value));
            s.execute(format("insert into %s (name,val) values ('%s',%s)",TABLE_H,name,value));
            fail("Did not report a duplicate key violation!");
        }catch(SQLException se){
            Assert.assertEquals("Incorrect SQL State returned",SQLState.LANG_DUPLICATE_KEY_CONSTRAINT,se.getSQLState());
            Assert.assertTrue("Incorrect message <"+se.getMessage()+">",se.getMessage().contains("identified by 'FOO' defined on 'H'"));
            throw se;
        }
    }

    @Test
    public void testUpdateUniqueIndex() throws Exception{
        // DB-2578: Update unique index causes 0 length row error
        // also "uniqueWithDuplicateNulls" index column became non-unique when null value updated to non-null
        try(Statement s = conn.createStatement()){
            s.execute("create table "+TABLE_2+"(id int, name varchar(10))");
            s.execute("create unique index "+ INDEX_2+" on "+TABLE_2+"(id)");
            s.execute("insert into t2 values(null, 'able')");
            s.execute("insert into t2 values(null, 'baker')");

            // update a previously null unique column to non-null value
            s.execute("update t2 set id=1 where name='able'");
            // make sure we can see it via the index
            try(ResultSet rs2=s.executeQuery("select * from t2 --SPLICE-PROPERTIES index="+INDEX_2+" \n where name='able'")){

                String expected=""+
                        "ID |NAME |\n"+
                        "----------\n"+
                        " 1 |able |";

                assertEquals("verify using index",expected,TestUtils.FormattedResult.ResultFactory.toString(rs2));
            }

            // try update that would be a unique index violation
            try{
                s.execute("update t2 set id=1 where name='baker'");
                fail("Should have failed with unique constraint violation.");
            }catch(SQLException e){
                // expected
                assertEquals("Expected SQLIntegrityConstraintViolationException",
                        SQLIntegrityConstraintViolationException.class.getSimpleName(),e.getClass().getSimpleName());
                assertEquals(SQLState.LANG_DUPLICATE_KEY_CONSTRAINT,e.getSQLState());
            }

            try(ResultSet rs=s.executeQuery("select * from t2 where name='baker'")){
                List<String> results=Lists.newArrayListWithExpectedSize(1);
                while(rs.next()){
                    Object id=rs.getObject(1);
                    String name=rs.getString(2);
                    assertEquals("Incorrect name returned!","baker",name);
                    assertNull("Expected 'baker' id to remain null",id);
                    results.add(String.format("id:%s,name:%s",null,name));
                }
                assertEquals("Incorrect number of rows returned!",1,results.size());
            }
        }
    }

    @Test
    public void testUpdateCompoundIndex() throws Exception{
        // DB-2578: Update unique index causes 0 length row error
        // not really a unique index test but this was broken with first pass to fix DB-2578
        try(Statement s = conn.createStatement()){
            s.execute("create table "+TABLE_3+"(a varchar(5) primary key, b varchar(5), c varchar(5), d varchar(5))");
            s.execute("create unique index "+INDEX_3+" on "+TABLE_3+"(a, b desc, c)");
            s.execute("insert into "+TABLE_3+" values('A','A','A','A'), ('B','B','B','B'), ('C','C','C','C')");
            assertCountEquals(3,s, "select * from "+TABLE_3+"--SPLICE-PROPERTIES index="+INDEX_3+"\n");

            int c = s.executeUpdate("update "+TABLE_3+" set a='M',b='M',c='M' where d='C'");
            Assert.assertEquals("Incorrect number of rows updated!",1,c);
            // make sure we can see it via the index
            try(ResultSet rs2=s.executeQuery("select * from "+TABLE_3+" --SPLICE-PROPERTIES index="+INDEX_3+" \n where a='M'")){
                String expected=""+
                        "A | B | C | D |\n"+
                        "----------------\n"+
                        " M | M | M | C |";

                assertEquals("verify using index",expected,TestUtils.FormattedResult.ResultFactory.toString(rs2));
            }
        }
    }

    /* ****************************************************************************************************************/
    /*private helper methods*/
    private void assertCountEquals(long expected,Statement s,String query) throws SQLException{
        try(ResultSet rs=s.executeQuery(query)){
            long c=0l;
            while(rs.next()){
                c++;
            }
            Assert.assertEquals("Incorrect count for query <"+query+">",expected,c);
        }
    }

    private void insertThenDelete(String tableName) throws Exception{
        String name="sfines";
        int value=2;
        try(Statement s = conn.createStatement()){
            s.execute(format("insert into %s (name, val) values ('%s', %s)",tableName,name,value));
            s.execute(format("delete from %s where name = '%s'",tableName,name));
            try(ResultSet rs=s.executeQuery(format("select * from %s where name = '%s'",tableName,name))){
                List<String> results=Lists.newArrayListWithExpectedSize(0);
                while(rs.next()){
                    String retName=rs.getString(1);
                    int val=rs.getInt(2);
                    results.add(String.format("name:%s,value:%d",retName,val));
                }
                Assert.assertEquals("Incorrect number of rows returned!",0,results.size());
            }
        }
    }
}
