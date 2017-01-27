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

package com.splicemachine.derby.impl.sql.execute.actions;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceTableWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.derby.test.framework.TestConnection;
import com.splicemachine.test_tools.TableCreator;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import static com.splicemachine.test_tools.Rows.row;
import static com.splicemachine.test_tools.Rows.rows;

/**
 * @author Scott Fines
 *         Date: 9/2/14
 */
public class TruncateTableIT {

    public static String CLASS_NAME = TruncateTableIT.class.getSimpleName().toUpperCase();

    public static final SpliceSchemaWatcher schemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);

    public static final SpliceTableWatcher table = new SpliceTableWatcher("A",schemaWatcher.schemaName,"(a int, b int, primary key (a, b))");

    public static final SpliceWatcher classWatcher = new SpliceWatcher(CLASS_NAME);

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(CLASS_NAME);

    public static final String query = "select * from " + table+" where a = ";
    @ClassRule
    public static TestRule chain = RuleChain.outerRule(classWatcher)
            .around(schemaWatcher)
            .around(table);

    private static TestConnection conn1;
    private static TestConnection conn2;

    private long conn1Txn;
    private long conn2Txn;

    @BeforeClass
    public static void setUpClass() throws Exception {
        conn1 = classWatcher.getOrCreateConnection();
        conn2 = classWatcher.createConnection();

        new TableCreator(conn1)
                .withCreate("create table pk1 (a int primary key, b char(20))")
                .withInsert("insert into pk1 values(?,?)")
                .withRows(rows(
                        row(1, "San Francisco"),
                        row(2, "San Jose")))
                .create();

        new TableCreator(conn1)
                .withCreate("create table fk1( a int , b varchar(10), foreign key(a) references pk1(a))")
                .withInsert("insert into fk1 values(?,?)")
                .withRows(rows(
                        row(1, "SFO"),
                        row(2, "SJC")))
                .create();
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
    public void testTruncateWorksWithinSingleTransactionWithCommit() throws Exception {
        int a = 2;
        int b = 2;
        PreparedStatement ps = conn1.prepareStatement("insert into " + table + "(a,b) values (?,?)");
        ps.setInt(1,a);
        ps.setInt(2,b);
        ps.execute();

        long count = conn1.count(query+a);
        Assert.assertEquals("incorrect count!",1l,count);

        conn1.commit();

        //truncate the table
        conn1.createStatement().execute("truncate table "+ table);

        count = conn1.count(query+a);
        Assert.assertEquals("Truncate does not work correctly!",0l,count);

        /*
         * Committing here ensures that, in the case where truncate is WORKING, that the data is flushed
         * from the table for future transactions (e.g. no contamination). However, if truncate isn't
         * working transactionally, then this commit won't necessarily help, and we may get contaminated
         * runs, so be careful!
         */
        conn1.commit();
    }

    @Test
    public void testTruncateWorksWithinSingleTransaction() throws Exception {
        int a = 1;
        int b = 1;
        PreparedStatement ps = conn1.prepareStatement("insert into " + table + "(a,b) values (?,?)");
        ps.setInt(1,a);
        ps.setInt(2,b);
        ps.execute();

        long count = conn1.count(query+a);
        Assert.assertEquals("incorrect count!",1l,count);

        //truncate the table
        conn1.createStatement().execute("truncate table "+ table);

        count = conn1.count(query+a);
        Assert.assertEquals("Truncate does not work correctly!", 0l, count);
    }

    @Test
    public void testTruncateIsNotVisibleToOtherTransaction() throws Exception {
        int a = 3;
        int b = 3;
        PreparedStatement ps = conn1.prepareStatement("insert into " + table + "(a,b) values (?,?)");
        ps.setInt(1,a);
        ps.setInt(2,b);
        ps.execute();
        conn1.commit(); //make the data visible to both transactions
        conn2.rollback(); //push the other connection to a new transaction, so that it sees the writes

        long count = conn2.count(query+a);
        Assert.assertEquals("incorrect count!",1l,count);

        //truncate the table
        conn1.createStatement().execute("truncate table "+ table);

        //make sure it's invisible
        count = conn2.count(query+a);
        Assert.assertEquals("Truncate is not transactionally correct!",1l,count);

        //commit the truncate and make sure that conn2 can see the results
        conn1.commit();
        conn2.rollback();

        count = conn2.count(query+a);
        Assert.assertEquals("Truncate does not work correctly!", 0l, count);
    }

    @Test
    public void testTruncateRollbackIsRolledBack() throws Exception {
        int a = 4;
        int b = 4;
        PreparedStatement ps = conn1.prepareStatement("insert into " + table + "(a,b) values (?,?)");
        ps.setInt(1,a);
        ps.setInt(2,b);
        ps.execute();
        conn1.commit(); //make the data visible to both transactions

        long count = conn1.count(query+a);
        Assert.assertEquals("incorrect count!",1l,count);

        //truncate the table
        conn1.createStatement().execute("truncate table "+ table);

        //make sure it's invisible
        count = conn1.count(query+a);
        Assert.assertEquals("Truncate is not transactionally correct!",0l,count);

        conn1.rollback();

        count = conn1.count(query+a);
        Assert.assertEquals("Truncate is not being rolled back correctly!",1l,count);
    }

    @Test
    public void testTruncateTableWithMoreTHan2PKColumns() throws Exception {

        conn1.createStatement().execute("truncate table "+ table);
        int a = 2;
        int b = 4;
        PreparedStatement ps = conn1.prepareStatement("insert into " + table + "(a,b) values (?,?)");
        ps.setInt(1,a);
        ps.setInt(2,b);
        ps.execute();

        ps = conn1.prepareStatement("select a, b from " + table);
        ResultSet rs = ps.executeQuery();

        Assert.assertTrue(rs.next());
        Assert.assertEquals(2, rs.getInt(1));
        Assert.assertEquals(4, rs.getInt(2));

        conn1.commit();
    }


    @Test
    public void testTruncateDeleteParentTable() throws Exception {
        methodWatcher.executeUpdate("truncate table fk1");
        int n = methodWatcher.executeUpdate("delete from pk1 where a < 5");
        //DB-5539: truncate table should remove backing foreign key index and write handler and re-create them. After
        // child table is truncated, all rows in parent table should be allowed to delete
        Assert.assertTrue("wrong number of rows deleted n = " + n, n == 2);
    }
}
