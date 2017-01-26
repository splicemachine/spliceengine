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

package com.splicemachine.derby.transactions;

import com.splicemachine.db.shared.common.reference.SQLState;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.derby.test.framework.TestConnection;
import com.splicemachine.test.Transactions;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.sql.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * @author Scott Fines
 *         Date: 9/4/14
 */
@Category({Transactions.class})
public class DropTableTransactionIT{

    private static final SpliceSchemaWatcher schemaWatcher=new SpliceSchemaWatcher(DropTableTransactionIT.class.getSimpleName().toUpperCase());

//    private static final SpliceTableWatcher table = new SpliceTableWatcher("A",schemaWatcher.schemaName,"(a int, b int)");
    private static final String tableStructure= "(a int, b int)";

    private static final SpliceWatcher classWatcher=new SpliceWatcher();

    @ClassRule
    public static TestRule chain=RuleChain.outerRule(classWatcher).around(schemaWatcher);

    private static TestConnection conn1;
    private static TestConnection conn2;

    @BeforeClass
    public static void setUpClass() throws Exception{
        conn1=classWatcher.getOrCreateConnection();
        conn2=classWatcher.createConnection();
        conn1.setSchema(schemaWatcher.schemaName);
        conn2.setSchema(schemaWatcher.schemaName);
    }

    @AfterClass
    public static void tearDownClass() throws Exception{
        conn1.close();
        conn2.close();
    }

    @After
    public void tearDown() throws Exception{
        conn1.rollback();
        conn1.reset();
        conn2.rollback();
        conn2.reset();
    }

    @Before
    public void setUp() throws Exception{
        conn1.setAutoCommit(false);
        conn2.setAutoCommit(false);
    }

    @Test
    public void testDropTableIgnoredByOtherTransactions() throws Exception{
        //create the table here to make sure that it exists
//        String table=schemaWatcher.schemaName+"."+createTable(conn1,1);
        String table=createTable(conn1,1);
        //roll forward the transactions in case they were created before the creation
        conn1.commit();
        conn2.commit();

        //verify that the table is present for both --these will blow up if the create didn't work propertly
        try(Statement s = conn1.createStatement()){
            assertEquals("Data is mysteriously present!",0l,conn1.count(s,"select * from " + table));
        }

        try(Statement s = conn2.createStatement()){
            assertEquals("Data is mysteriously present!",0l,conn2.count(s,"select * from " + table));

            //make sure we can insert data BEFORE we drop the table
            s.execute("insert into "+table+" values (1, 1)");
            assertEquals("Unable to read data from dropped table!",1L,conn2.count(s,"select * from "+table));//+" where a=1"));
        }

        // CONN1: issue the drop statement
        try(Statement s=conn1.createStatement()){
            s.execute("drop table "+table);
        }

        // CONN2: now confirm that you can keep writing and reading data from the table
        try(Statement s=conn2.createStatement()){
            s.execute("insert into "+table+" values (1, 1)");
            assertEquals("Unable to read data from dropped table!",2L,conn2.count(s,"select * from "+table+" where a=1"));
        }
//
//        // CONN1: commit
        conn1.commit();
//
//        // CONN2: confirm we still can write and read from it
        try(Statement s=conn2.createStatement()){
            s.execute("insert into "+table+" values (1, 1)");
            assertEquals("Unable to read data from dropped table!",3L,conn2.count("select * from "+table+" where a=1"));
        }
//
//        // CONN2: Commit. Table should be dropped to conn2 now
        conn2.commit();
//
//        // CONN2: we shouldn't be able to see the table now
        assertQueryFail(conn2,"insert into "+table+" values (1,1)",SQLState.LANG_TABLE_NOT_FOUND);
        assertQueryFail(conn2,"select * from "+table,SQLState.LANG_TABLE_NOT_FOUND);
    }

    private String createTable(TestConnection conn,int initialTableNumber) throws SQLException{
        try(Statement s=conn.createStatement()){
            int tn=initialTableNumber;
            String tableName;
            while(true){
                tableName = "t"+tn;
                try{
                    s.executeUpdate("create table "+tableName+tableStructure);
                    return tableName;
                }catch(SQLException se){
                    if("X0Y68".equals(se.getSQLState())){
                        tn+=7;
                    }else throw se;
                }
            }
        }
    }

    @Test
    public void testDropTableRollback() throws Exception{
        //create the table here to make sure that it exists
        String table=createTable(conn1,2);
        //roll forward the transactions in case they were created before the creation
        conn1.commit();
        conn2.commit();

        try(Statement s=conn1.createStatement()){
            s.execute("drop table "+table);
        }

        conn1.rollback();

        //confirm that the table is still visible
        int aInt=2;
        int bInt=2;
        try(PreparedStatement ps=conn1.prepareStatement("insert into "+table+"(a,b) values (?,?)")){
            ps.setInt(1,aInt);
            ps.setInt(2,bInt);
            ps.execute();
        }

        long count=conn1.count("select * from "+table+" where a="+aInt);
        assertEquals("Unable to read data from dropped table!",1l,count);
    }

    @Test
    public void testCanDropUnrelatedTablesConcurrently() throws Exception{
        String t1=createTable(conn1,3);
        String t2=createTable(conn1,4);
//        new SpliceTableWatcher("t",schemaWatcher.schemaName,"(a int unique not null, b int)").start();
//        new SpliceTableWatcher("t2",schemaWatcher.schemaName,"(a int unique not null, b int)").start();
        conn1.commit();
        conn2.commit(); //roll both connections forward to ensure visibility

        /*
         * Now try and drop both tables, one table in each transaction, and make sure that they do not conflict
         * with each other
         */
        try(Statement s=conn1.createStatement()){
            s.execute("drop table "+t1);
        }

        try(Statement s=conn2.createStatement()){
            s.execute("drop table "+t2);
        }

        //commit the two transactions to make sure that the tables no longer exist
        conn1.commit();
        conn2.commit();
    }

    @Test
    public void testDroppingSameTableGivesWriteWriteConflict() throws Exception{
        String t=createTable(conn1,5);
//        new SpliceTableWatcher("t3",schemaWatcher.schemaName,"(a int unique not null, b int)").start();
        conn1.commit();
        conn2.commit(); //roll both connections forward to ensure visibility

        /*
         * Now try and drop both tables, one table in each transaction, and make sure that they do not conflict
         * with each other
         */
        try(Statement s=conn1.createStatement()){
            s.execute("drop table "+t);
        }

        try(Statement s=conn2.createStatement()){
            s.execute("drop table "+t);
            fail("Did not throw a Write/Write conflict");
        }catch(SQLException se){
            //SE014 = ErrorState.WRITE_WRITE_CONFLICT
            assertEquals("Incorrect error message!","SE014",se.getSQLState());
        }finally{
            //commit the two transactions to make sure that the tables no longer exist
            conn1.commit();
        }
    }

    @Test
    public void testDropRollbackGetCurrentTransaction() throws Exception{
        /*
         * Sequence:
         * 1. create table
         * 2. commit
         * 3. drop table
         * 4. call SYSCS_UTIL.GET_CURRENT_TRANSACTION();
         * 5. rollback;
         * 6. call SYSCS_UTIL.GET_CURRENT_TRANSACTION();
         *
         * and make sure that there is no error.
         */
        String table= createTable(conn1,9);
        conn1.commit();

        try(Statement s =conn1.createStatement()){
            s.execute("drop table "+ table);
        }

        long txnId = conn1.getCurrentTransactionId();

        conn1.rollback();
        long txnId2 = conn1.getCurrentTransactionId();

        Assert.assertNotEquals("Transaction id did not advance!",txnId,txnId2);
    }

    @Test
    public void testDropTableDropsMetadataCommit() throws Exception{
        /*
         * Sequence:
         * 1. create table
         * 2. commit
         * 3. drop table
         * 4. commit;
         * 5. make sure metadata does not reveal it
         *
         * and make sure that there is no error.
         */
        String table= createTable(conn1,10);

        try(Statement s =conn1.createStatement()){
            s.execute("drop table "+ table);
        }

        try(ResultSet rs = conn1.getMetaData().getTables(null,schemaWatcher.schemaName,null,null)){
            String uTable = table.toUpperCase();
            while(rs.next()){
                Assert.assertNotEquals("Table still found!",uTable,rs.getString("TABLE_NAME"));
            }
        }
    }

    private static void assertQueryFail(Connection connection,String sql,String expected){
        try(Statement s=connection.createStatement()){
            s.execute(sql);
            fail("didn't fail as expected: "+sql);
        }catch(SQLException e){
            assertEquals("Unexpected exception message upon failure",expected,e.getSQLState());
        }
    }
}
