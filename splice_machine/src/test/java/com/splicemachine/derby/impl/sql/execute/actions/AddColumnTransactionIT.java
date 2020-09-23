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

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceTableWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.derby.test.framework.TestConnection;
import com.splicemachine.homeless.TestUtils;
import com.splicemachine.pipeline.ErrorState;
import com.splicemachine.test.Transactions;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.Assert.assertEquals;

/**
 * @author Scott Fines
 *         Date: 9/3/14
 */
@Category(Transactions.class)
public class AddColumnTransactionIT {
    public static final SpliceSchemaWatcher schemaWatcher = new SpliceSchemaWatcher(AddColumnTransactionIT.class.getSimpleName().toUpperCase());

    public static final SpliceTableWatcher commitTable = new SpliceTableWatcher("A",schemaWatcher.schemaName,"(a int, b int)");
    public static final SpliceTableWatcher commitTable2 = new SpliceTableWatcher("B",schemaWatcher.schemaName,"(a int, b int)");
    public static final SpliceTableWatcher commitTable3 = new SpliceTableWatcher("C",schemaWatcher.schemaName,"(a int, b int)");
    public static final SpliceTableWatcher commitTable4 = new SpliceTableWatcher("D",schemaWatcher.schemaName,"(a int, b int)");
    public static final SpliceTableWatcher addedTable = new SpliceTableWatcher("E",schemaWatcher.schemaName,"(a int, b int)");
    public static final SpliceTableWatcher addedTable2 = new SpliceTableWatcher("F",schemaWatcher.schemaName,"(a int, b int)");
    public static final SpliceTableWatcher addedTable3 = new SpliceTableWatcher("G",schemaWatcher.schemaName,"(a int, b int)");
    public static final SpliceTableWatcher addedTable4 = new SpliceTableWatcher("H",schemaWatcher.schemaName,"(name char(14) not null primary key, age int)");
    public static final SpliceTableWatcher addedTable5 = new SpliceTableWatcher("I",schemaWatcher.schemaName,"(a int, b int)");

    public static final SpliceWatcher classWatcher = new SpliceWatcher();

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(classWatcher)
                                            .around(schemaWatcher)
                                            .around(commitTable)
                                            .around(commitTable2)
                                            .around(commitTable3)
                                            .around(commitTable4)
                                            .around(addedTable)
                                            .around(addedTable2)
                                            .around(addedTable3)
                                            .around(addedTable4)
                                            .around(addedTable5);

    private static TestConnection conn1;
    private static TestConnection conn2;

    private long conn1Txn;
    private long conn2Txn;

    @BeforeClass
    public static void setUpClass() throws Exception {
        conn1 = classWatcher.getOrCreateConnection();
        conn2 = classWatcher.createConnection();
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
        conn1.close();
        conn2.close();
    }

    @After
    public void tearDown() throws Exception {
        conn1.rollback();
        conn1.reset();
        conn2.rollback();
        conn2.reset();
    }

    @Before
    public void setUp() throws Exception {
        conn1.setAutoCommit(false);
        conn2.setAutoCommit(false);
        conn1Txn = conn1.getCurrentTransactionId();
        conn2Txn = conn2.getCurrentTransactionId();
    }

    @Test
    public void testAddColumnWorksWithOneConnectionAndCommitDefaultNull() throws Exception {
        int aInt = 1;
        int bInt = 1;
        PreparedStatement preparedStatement = conn1.prepareStatement("insert into " + commitTable + " (a,b) values (?,?)");
        preparedStatement.setInt(1,aInt);
        preparedStatement.setInt(2,bInt);
        preparedStatement.execute();
        conn1.commit();

        conn1.createStatement().execute("alter table " + commitTable + " add column c int");
        conn1.commit();

        ResultSet rs = conn1.query("select * from " + commitTable);

        int count = 0;
        while(rs.next()){
            rs.getInt("C");
            Assert.assertTrue("Column C is not null!",rs.wasNull());
            count++;
        }
        rs.close();
        assertEquals("Incorrect returned row count",1,count);
    }

    @Test
    public void testAddColumnWorksWithOneConnectionAndCommitDefaultValue() throws Exception {
        int aInt = 2;
        int bInt = 2;
        PreparedStatement preparedStatement = conn1.prepareStatement("insert into " + commitTable2 + " (a,b) values (?,?)");
        preparedStatement.setInt(1,aInt);
        preparedStatement.setInt(2,bInt);
        preparedStatement.execute();
        conn1.commit();

        conn1.createStatement().execute("alter table "+ commitTable2+" add column d int with default 2");
        conn1.commit();

        ResultSet rs = conn1.query("select * from " + commitTable2 +" where a = "+aInt);
        assertEquals("Incorrect metadata reporting!",3,rs.getMetaData().getColumnCount());

        int count = 0;
        while(rs.next()){
            int anInt = rs.getInt("D");
            assertEquals("Incorrect value for column D", 2, anInt);
            Assert.assertTrue("Column D is null!",!rs.wasNull());
            count++;
        }
        rs.close();
        assertEquals("Incorrect returned row count",1,count);
    }

    @Test
    public void testAddColumnWorksWithOneTransaction() throws Exception {
        int aInt = 3;
        int bInt = 3;
        PreparedStatement preparedStatement = conn1.prepareStatement("insert into " + commitTable3 + " (a,b) values (?,?)");
        preparedStatement.setInt(1, aInt);
        preparedStatement.setInt(2, bInt);
        preparedStatement.execute();

        assertEquals("Incorrect returned row count", 1, conn1.count("select * from "+ commitTable3));

        conn1.createStatement().execute("alter table " + commitTable3 + " add column e int with default 2");

        ResultSet rs = conn1.query("select * from " + commitTable3 +" where a = "+aInt);

        int count = 0;
        while(rs.next()){
            int eInt = rs.getInt("E");
            assertEquals("Incorrect value for column E",2,eInt);
            count++;
        }
        rs.close();
        assertEquals("Incorrect returned row count",1,count);
    }

    @Test
    public void testAddColumnRemovedWhenRolledBack() throws Exception {
        int aInt = 3;
        int bInt = 3;
        PreparedStatement preparedStatement = conn1.prepareStatement("insert into " + commitTable3 + " (a,b) values (?,?)");
        preparedStatement.setInt(1, aInt);
        preparedStatement.setInt(2, bInt);
        preparedStatement.execute();
        conn1.commit();

        conn1.createStatement().execute("alter table " + commitTable3 + " add column e int with default 2");

        // roll back the alter table only
        conn1.rollback();
        // should still see the inserted row
        assertEquals("Incorrect returned row count", 1, conn1.count("select * from "+ commitTable3));

        ResultSet rs = conn1.query("select * from " + commitTable3+ " where a = "+ aInt);

        int count = 0;
        if(rs.next()){
            try{
                rs.getInt("E");
                Assert.fail("did not fail!");
            }catch(SQLException se){
                assertEquals("Incorrect SQL state! "+ se.getSQLState()+":"+se.getLocalizedMessage(),
                             TestUtils.trimSQLState(ErrorState.INVALID_COLUMN_NAME.getSqlState()),se.getSQLState());
            }
            count++;
        }
        rs.close();
        assertEquals("Incorrect returned row count",1,count);
    }

    @Test
    public void testAddColumnIgnoredByOtherTransaction() throws Exception {
        int aInt = 4;
        int bInt = 4;
        PreparedStatement preparedStatement = conn1.prepareStatement("insert into " + commitTable4 + " (a,b) values (?,?)");
        preparedStatement.setInt(1,aInt);
        preparedStatement.setInt(2,bInt);
        preparedStatement.execute();

        conn1.createStatement().execute("alter table "+ commitTable4+" add column f int with default 2");

        ResultSet rs = conn1.query("select * from " + commitTable4 +" where a = "+aInt);

        int count = 0;
        while(rs.next()){
            int anInt = rs.getInt("F");
            assertEquals("Incorrect value for column f",2,anInt);
            Assert.assertTrue("Column f is null!",!rs.wasNull());
            count++;
        }
        rs.close();
        assertEquals("Incorrect returned row count",1,count);

        rs = conn2.query("select * from " + commitTable4 + " where a = " + aInt);

        if(rs.next()){
            try{
                rs.getInt("F");
                Assert.fail("did not fail!");
            }catch(SQLException se){
                assertEquals("Incorrect error message!", ErrorState.COLUMN_NOT_FOUND.getSqlState(), se.getSQLState());
            }
        }

        rs.close();

    }

    @Test
    public void testAddColumnCannotProceedWithOpenDMLOperations() throws Exception {
        TestConnection a;
        TestConnection b;
        if(conn1Txn>conn2Txn){
            a = conn1;
            b = conn2;
        }else{
            a = conn2;
            b = conn1;
        }

        PreparedStatement ps = b.prepareStatement("alter table "+ addedTable3+" add column d int with default 5");
        ps.execute();

        try{
            a.createStatement().execute("alter table "+ addedTable3+" add column c int with default 2");
            Assert.fail("Did not catch an exception!");
        }catch(SQLException se){
            System.err.printf("%s:%s%n",se.getSQLState(),se.getMessage());
            assertEquals("Incorrect error message!",ErrorState.DDL_ACTIVE_TRANSACTIONS.getSqlState(),se.getSQLState());
        }
    }

    @Test
    public void testAddColumnAfterInsertionIsCorrect() throws Exception {
        TestConnection a;
        TestConnection b;
        if(conn1Txn>conn2Txn){
            a = conn2;
            b = conn1;
        }else{
            a = conn1;
            b = conn2;
        }

        int aInt = 8;
        int bInt = 8;
        PreparedStatement ps = b.prepareStatement("insert into "+addedTable+" (a,b) values (?,?)");
        ps.setInt(1,aInt);ps.setInt(2,bInt);
        ps.execute();
        b.commit();
        // a must commit before it can see b's writes
        assertEquals("Incorrect returned row count", 0, a.count("select * from "+ addedTable));
        a.commit();
        assertEquals("Incorrect returned row count", 1, a.count("select * from "+ addedTable));

        a.createStatement().execute("alter table "+ addedTable+" add column FF int not null with default 2");
        a.commit();

        ResultSet rs = a.query("select * from "+ addedTable+" where a = "+ aInt);
        int count=0;
        while(rs.next()){
            int f = rs.getInt("FF");
            Assert.assertFalse("Got a null value for FF!",rs.wasNull());
            assertEquals("Incorrect default value!",2,f);
            count++;
        }
        rs.close();
        assertEquals("Incorrect returned row count", 1, count);
    }

    @Test
    public void testAddColumnBeforeInsertionIsCorrect() throws Exception {
        TestConnection a;
        TestConnection b;
        if(conn1Txn>conn2Txn){
            a = conn2;
            b = conn1;
        }else{
            a = conn1;
            b = conn2;
        }

        int aInt = 10;
        int bInt = 10;

        //alter the table
        a.createStatement().execute("alter table " + addedTable2 + " add column f int not null default 2");
        a.commit();

        // Note: b has to commit to see the new column since b's txn was started before a's alter table
        b.commit();
        ResultSet gs = b.query(String.format("select * from %s", addedTable2));
        Assert.assertEquals("Metadata returning incorrect column count!", 3, gs.getMetaData().getColumnCount());

        //now insert some data
        PreparedStatement ps = b.prepareStatement("insert into "+addedTable2+" (a,b) values (?,?)");
        ps.setInt(1,aInt);
        ps.setInt(2, bInt);
        ps.execute();
        b.commit();

        ResultSet rs = a.query("select * from "+ addedTable2+" where a = "+ aInt);
        int count=0;
        while(rs.next()){
            int f = rs.getInt("F");
            Assert.assertFalse("Got a null value for f!",rs.wasNull());
            assertEquals("Incorrect default value!",2,f);
            count++;
        }
        rs.close();
        assertEquals("Incorrect returned row count",1,count);
    }

    @Test
    public void testAddUniqueColumnToTableWithPrimaryKey() throws Exception {
        // test DB-3113: NPE when adding column to table with primary key

        PreparedStatement ps = conn2.prepareStatement("insert into "+addedTable4+" (name,age) values (?,?)");
        ps.setString(1, "Ralph");ps.setInt(2, 22);
        ps.execute();
        conn2.commit();

        // add a column with a default
        conn1.createStatement().execute("alter table " + addedTable4 + " add column num char(11) not null default '000001'");
        conn1.commit();

        // insert with value for new column
        ps = conn2.prepareStatement("insert into "+addedTable4+" values (?,?,?)");
        ps.setString(1, "Jeff");ps.setInt(2, 13); ps.setString(3, "9999");
        ps.execute();
        conn2.commit();

        // add a column with a unique constraint
        conn1.createStatement().execute("alter table " + addedTable4 + " add column id int constraint uid unique");
        conn1.commit();

        // insert without value for new column
        ps = conn2.prepareStatement("insert into "+addedTable4+" (name, age) values (?,?)");
        ps.setString(1, "Joe");ps.setInt(2, 11); ps.execute();

        // insert with value for new column
        ps = conn2.prepareStatement("insert into "+addedTable4+" values (?,?,?,?)");
        ps.setString(1, "Fred");ps.setInt(2, 20); ps.setString(3, "121212"); ps.setInt(4, 123); ps.execute();
        conn2.commit();

        long count = conn1.count("select * from " + addedTable4 + " where id = 123");
        assertEquals("incorrect row count!", 1, count);

        count = conn1.count("select * from " + addedTable4 + " where id is not null");
        assertEquals("incorrect row count!", 1, count);

        ps = conn2.prepareStatement("insert into "+addedTable4+" values (?,?,?,?)");
        ps.setString(1, "Terry");ps.setInt(2, 26); ps.setString(3, "777777"); ps.setInt(4, 1); ps.execute();
        conn2.commit();

        try {
            conn2.createStatement().execute(String.format("insert into %s values ('Henry', 214, '45454545', 1)", addedTable4));
            Assert.fail("Expected unique key violation");
        } catch (SQLException e) {
            Assert.assertTrue(e.getLocalizedMessage(),e.getLocalizedMessage().startsWith(
                "The statement was aborted because it would have caused a " +
                    "duplicate key value in a unique or primary key " +
                    "constraint or unique index " +
                    "identified by 'SQL"));
        }

    }

    @Test
    public void testAlterTableAddColumn() throws Exception {
        String tableName = "alterTableAddColumn".toUpperCase();
        String tableRef = schemaWatcher.schemaName+"."+tableName;

        try(Statement s1 = conn1.createStatement()){
            s1.execute("drop table if exists "+ tableRef);

            s1.execute(String.format("create table %s(num int, addr varchar(50), zip char(5))",tableRef));


            s1.execute(String.format("insert into %s values(100, '100F: 101 California St', '94114')",tableRef));
            s1.execute(String.format("insert into %s values(200, '200F: 908 Glade Ct.', '94509')",tableRef));
            s1.execute(String.format("insert into %s values(300, '300F: my addr', '34166')",tableRef));
            s1.execute(String.format("insert into %s values(400, '400F: 182 Second St.', '94114')",tableRef));
            s1.execute(String.format("insert into %s(num) values(500)",tableRef));


            s1.execute(String.format("Alter table %s add column salary float default 0.0",tableRef));

            conn1.commit();
            conn2.commit(); //move both connection's transactions forward

            conn2.setAutoCommit(false);
            try(Statement s2=conn2.createStatement()){

                s1.execute(String.format("update %s set salary=1000.0 where zip='94114'",tableRef));
                s1.execute(String.format("update %s set salary=5000.85 where zip='94509'",tableRef));

                try(ResultSet rs=s1.executeQuery(String.format("select zip, salary from %s where salary > 0",tableRef))){
                    int count=0;
                    while(rs.next()){
                        count++;
                        Assert.assertNotNull("Salary is null!",rs.getFloat(2));
                    }
                    assertEquals("Salary Cannot Be Queried after added!",3,count);
                }

                try(ResultSet rs=s2.executeQuery(String.format("select zip, salary from %s where salary > 0",tableRef))){
                    int count=0;
                    while(rs.next()){
                        count++;
                        Assert.assertNotNull("Salary is null!",rs.getFloat(2));
                    }
                    assertEquals("Salary Cannot Be Queried after added!",0,count);
                }

                // updates will not be seen by c2 until both have committed
                conn1.commit();
                conn2.commit();
                try(ResultSet rs=s2.executeQuery(String.format("select zip, salary from %s where salary > 0",tableRef))){
                    int count=0;
                    while(rs.next()){
                        count++;
                        Assert.assertNotNull("Salary is null!",rs.getFloat(2));
                    }
                    assertEquals("Salary Cannot Be Queried after added!",3,count);
                }
            }
        }
    }

    @Test
    public void testAddColToConstrainedTable() throws Exception {
        // DB-3711: if UC on a col, can't update added col
        String tableName = "fred".toUpperCase();
        String tableRef = schemaWatcher.schemaName+"."+tableName;

        try(Statement s1 = conn1.createStatement()){
            s1.execute("drop table if exists "+tableRef);
            s1.execute(String.format("create table %s(id int unique)",tableRef));

            s1.execute(String.format("insert into %s values(1)",tableRef));
            s1.execute(String.format("insert into %s values(2)",tableRef));

            s1.execute(String.format("alter table %s add column loc varchar(3) default 'ZZZ'",tableRef));
            conn1.commit();

            s1.execute(String.format("update %s set loc = 'AAA'",tableRef));
            s1.execute(String.format("update %s set loc = 'MMM' where id = 1",tableRef));
            conn1.commit();
            conn2.commit();

            try(ResultSet rs=s1.executeQuery(String.format("select id from %s where id = 1",tableRef))){
                int count=0;
                while(rs.next()){
                    count++;
                    assertEquals("Expected id = 1",1,rs.getInt(1));
                }
                assertEquals("Expected one id equal to 1",1,count);
            }

            try(ResultSet rs=s1.executeQuery(String.format("select * from %s where id = 1",tableRef))){
                int count=0;
                while(rs.next()){
                    count++;
                    assertEquals("Expected id = 1",1,rs.getInt(1));
                    assertEquals("Expected loc = 'MMM'","MMM",rs.getString(2));
                }
                assertEquals("Expected one id equal to 1",1,count);
            }
        }
    }

    @Test
    public void testAddColAfterUniqueConstraint() throws Exception {
        // DB-3711: add UC on a col, can't add another col
        String tableName = "employees".toUpperCase();
        String tableRef = schemaWatcher.schemaName+"."+tableName;

        try(Statement s1 = conn1.createStatement()){
            s1.execute(String.format("create table %s(emplid INTEGER NOT NULL, lastname VARCHAR(25) NOT NULL, firstname "+
                    "VARCHAR(25) NOT NULL, reportsto INTEGER)",tableRef));

            s1.execute(String.format("insert into %s values(7725070,'Anuradha','Kottapalli',8852090)",tableRef));
            conn1.commit();

            s1.execute(String.format("alter table %s add constraint emp_uniq unique(emplid)",tableRef));
            s1.execute(String.format("alter table %s add column foo int",tableRef));
            conn1.commit();

            try(ResultSet rs=s1.executeQuery(String.format("select * from %s",tableRef))){
                int count=0;
                while(rs.next()){
                    count++;
                    assertEquals("FOO col add!",0,rs.getInt(5));
                }
                assertEquals("Expected one employee!",1,count);
            }

            s1.execute(String.format("update %s set foo = 9 where emplid = 7725070",tableRef));
            conn1.commit();

            try(ResultSet rs=s1.executeQuery(String.format("select * from %s where emplid = 7725070",tableRef))){
                int count=0;
                while(rs.next()){
                    count++;
                    assertEquals("FOO update!",9,rs.getInt(5));
                }
                assertEquals("Expected one employee!",1,count);
            }
        }
    }
}
