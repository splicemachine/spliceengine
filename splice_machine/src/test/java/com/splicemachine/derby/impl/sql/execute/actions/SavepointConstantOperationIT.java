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
import com.splicemachine.si.constants.SIConstants;
import com.splicemachine.test.SerialTest;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.sql.Statement;

import static org.junit.Assert.assertEquals;

/**
 *
 * @author Jeff Cunningham
 *         Date: 7/17/13
 */
@Category({SerialTest.class}) // Needed to check transaction distance
public class SavepointConstantOperationIT { 
    public static final String CLASS_NAME = SavepointConstantOperationIT.class.getSimpleName().toUpperCase();

    @ClassRule
    public static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);

    @ClassRule
    public static SpliceWatcher classWatcher = new SpliceWatcher(CLASS_NAME);

    public static final String TABLE_NAME_1 = "B";

    private static String tableDef = "(TaskId INT NOT NULL)";
    protected static SpliceTableWatcher b= new SpliceTableWatcher(TABLE_NAME_1, CLASS_NAME, tableDef);
    protected static SpliceTableWatcher t= new SpliceTableWatcher("T", CLASS_NAME, "(a int)");
    protected static SpliceTableWatcher c= new SpliceTableWatcher("C", CLASS_NAME, "(i int primary key)");
    protected static SpliceTableWatcher d= new SpliceTableWatcher("D", CLASS_NAME, "(i int primary key)");
    protected static SpliceTableWatcher e= new SpliceTableWatcher("E", CLASS_NAME, "(i int primary key)");
    protected static SpliceTableWatcher f= new SpliceTableWatcher("F", CLASS_NAME, "(i int primary key)");
    protected static SpliceTableWatcher g= new SpliceTableWatcher("G", CLASS_NAME, "(i int)");

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(classWatcher)
            .around(spliceSchemaWatcher)
            .around(b)
            .around(t).around(c).around(d)
            .around(e).around(f).around(g);

    private static TestConnection conn1;
    private static TestConnection conn2;

    @BeforeClass
    public static void setUpClass() throws Exception {
        // we check the number of transactions created during these tests, we have to wait a few seconds to make sure
        // all previous connections have been properly closed, otherwise they might create some transactions during the
        // execution of this test
        Thread.sleep(10000);
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
    }

    @Test
    public void testCanCommitOverActiveSavepoint() throws Exception{
        Savepoint s = conn1.setSavepoint("pish");
        try(Statement statement =conn1.createStatement()){
            statement.executeUpdate("insert into "+t+" values 1,2,3");
        }
        conn1.commit();
        boolean[] found = new boolean[3];
        try(Statement statement = conn1.createStatement()){
            try(ResultSet rs = statement.executeQuery("select * from "+t)){
                while(rs.next()){
                    int i=rs.getInt(1);
                    Assert.assertFalse("Returned a null value!",rs.wasNull());
                    Assert.assertFalse("Already seen value:"+i,found[i-1]);
                    found[i-1]=true;
                }
            }
        }

        for(int i=0;i<found.length;i++){
            Assert.assertTrue("row:"+(i+1)+" is missing!",found[i]);
        }
    }

    @Test
    public void testCanSetAndReleaseASavepoint() throws Exception {
        Savepoint savepoint = conn1.setSavepoint("test");
        int value = 1;
        conn1.execute(String.format("insert into %s (taskid) values (%d)",b,value));
        conn1.releaseSavepoint(savepoint);
        long count = conn1.count(String.format("select * from %s where taskid=%d",b,value));
        assertEquals("Incorrect count after savepoint release!",1l,count);
    }

    @Test
    public void testReleasingASavepointDoesNotCommitData() throws Exception {
        Savepoint savepoint = conn1.setSavepoint("test");
        int value = 6;
        conn1.execute(String.format("insert into %s (taskid) values (%d)",b,value));

        long count = conn2.count(String.format("select * from %s where taskid=%d",b,value));
        assertEquals("Data is visible to another transaction!",0l,count);

        conn1.releaseSavepoint(savepoint);
        count = conn2.count(String.format("select * from %s where taskid=%d",b,value));
        assertEquals("Data was committed during savepoint release!",0l,count);
    }

    @Test
    public void testRollingBackASavepointMakesDataInvisibleToMyself() throws Exception {
        Savepoint savepoint = conn1.setSavepoint("test");
        int value = 2;
        conn1.execute(String.format("insert into %s (taskid) values (%d)",b,value));
        long count = conn1.count(String.format("select * from %s where taskid=%d",b,value));
        assertEquals("Incorrect count after savepoint release!",1l,count);

        conn1.rollback(savepoint);
        count = conn1.count(String.format("select * from %s where taskid=%d",b,value));
        assertEquals("Incorrect count after savepoint release!",0l,count);
    }

    @Test
    public void testCanReleaseNonImmediateSavepoint() throws Exception {
        Savepoint s1 = conn1.setSavepoint("test");
        int value = 3;
        conn1.execute(String.format("insert into %s (taskid) values (%d)",b, value));

        Savepoint s2 = conn1.setSavepoint("test2");
        conn1.execute(String.format("insert into %s (taskid) values (%d)",b, value));

        //try releasing the first savepoint without first releasing the second, and make sure that it still works
        conn1.releaseSavepoint(s1);
        long count = conn1.count(String.format("select * from %s where taskid=%d",b,value));
        assertEquals("Incorrect count after savepoint release!", 2l, count);
    }

    @Test
    public void testRollingBackANonImmediateSavepointMakesDataInvisible() throws Exception {
        Savepoint s1 = conn1.setSavepoint("test");
        int value = 4;
        conn1.execute(String.format("insert into %s (taskid) values (%d)",b, value));

        Savepoint s2 = conn1.setSavepoint("test2");
        conn1.execute(String.format("insert into %s (taskid) values (%d)",b, value));

        //make sure data looks like what we expect
        long count = conn1.count(String.format("select * from %s where taskid=%d",b,value));
        assertEquals("Incorrect count after savepoint release!", 2l, count);

        //rollback s1 and make sure that all data is invisible
        conn1.rollback(s1);
        count = conn1.count(String.format("select * from %s where taskid=%d",b,value));
        assertEquals("Incorrect count after savepoint release!", 0l, count);
    }

    @Test
    public void testRollingBackANonImmediateSavepointMakesDataInvisibleEvenIfOtherSavepointIsReleased() throws Exception {
        Savepoint s1 = conn1.setSavepoint("test");
        int value = 4;
        conn1.execute(String.format("insert into %s (taskid) values (%d)",b, value));

        Savepoint s2 = conn1.setSavepoint("test2");
        conn1.execute(String.format("insert into %s (taskid) values (%d)",b, value));

        conn1.releaseSavepoint(s2);
        //make sure data looks like what we expect
        long count = conn1.count(String.format("select * from %s where taskid=%d",b,value));
        assertEquals("Incorrect count after savepoint release!", 2l, count);

        //rollback s1 and make sure that all data is invisible
        conn1.rollback(s1);
        count = conn1.count(String.format("select * from %s where taskid=%d",b,value));
        assertEquals("Incorrect count after savepoint release!", 0l, count);
    }

    @Test
    public void testCanRollbackThenReleaseASavepointAndDataIsCorrect() throws Exception {
        Savepoint savepoint = conn1.setSavepoint("test");
        int value = 7;
        conn1.execute(String.format("insert into %s (taskid) values (%d)",b,value));
        long count = conn1.count(String.format("select * from %s where taskid=%d",b,value));
        assertEquals("Incorrect count after savepoint release!",1l,count);

        conn1.rollback(savepoint);
        count = conn1.count(String.format("select * from %s where taskid=%d",b,value));
        assertEquals("Incorrect count after savepoint release!",0l,count);

        //insert some data again
        conn1.execute(String.format("insert into %s (taskid) values (%d)",b,value));

        //now release the savepoint
        conn1.releaseSavepoint(savepoint);
        count = conn1.count(String.format("select * from %s where taskid=%d",b,value));
        assertEquals("Incorrect count after savepoint release!",1l,count);


    }

    @Test
    public void testSomePersistedSavepoints() throws Exception {
        ResultSet rs = conn1.query("call SYSCS_UTIL.SYSCS_GET_CURRENT_TRANSACTION()");
        rs.next();
        PreparedStatement ps = conn1.prepareStatement(String.format("insert into %s.c (i) values (?)", CLASS_NAME));
        long txnId = rs.getLong(1);
        long iterations = (SIConstants.TRASANCTION_INCREMENT - 2) ;
        for (int i = 0; i < iterations; ++i) {
            ps.setInt(1, i);
            ps.execute();
        }
        conn1.commit();
        long count = conn1.count("select * from c");
        assertEquals("Incorrect count after savepoint release!",iterations,count);

        rs = conn1.query("call SYSCS_UTIL.SYSCS_GET_CURRENT_TRANSACTION()");
        rs.next();
        long txnIdLater = rs.getLong(1);

        // The difference should be 0x400: the batch created one or two persisted txn, that's up to 4 timestamps (begin + commit),
        // the user transaction committed (1 ts) and then the new user transaction started (1 ts)
        Assert.assertTrue("Created more persisted transactions than expected, difference = " + (txnIdLater - txnId), txnIdLater <= txnId + 0x600);
    }


    @Test
    public void testMorePersistedSavepoints() throws Exception {
        ResultSet rs = conn1.query("call SYSCS_UTIL.SYSCS_GET_CURRENT_TRANSACTION()");
        rs.next();
        PreparedStatement ps = conn1.prepareStatement(String.format("insert into %s.d (i) values (?)", CLASS_NAME));
        long txnId = rs.getLong(1);
        long iterations = (SIConstants.TRASANCTION_INCREMENT - 2) * 4;
        for (int i = 0; i < iterations; ++i) {
            ps.setInt(1, i);
            ps.execute();
        }
        conn1.commit();
        long count = conn1.count("select * from d");
        assertEquals("Incorrect count after savepoint release!",iterations,count);

        rs = conn1.query("call SYSCS_UTIL.SYSCS_GET_CURRENT_TRANSACTION()");
        rs.next();
        long txnIdLater = rs.getLong(1);

        // The difference should be 0x1000: the batch created up to 14 persisted txns, that's 28 timestamps (begin + commit),
        // the user transaction committed (1 ts) and then the new user transaction started (1 ts)
        Assert.assertTrue("Created more persisted transactions than expected, difference = " + (txnIdLater - txnId), txnIdLater <= txnId + 0x1E00);
    }


    @Test
    public void testMorePersistedSavepointsInBatch() throws Exception {
        ResultSet rs = conn1.query("call SYSCS_UTIL.SYSCS_GET_CURRENT_TRANSACTION()");
        rs.next();
        PreparedStatement ps = conn1.prepareStatement(String.format("insert into %s.e (i) values (?)", CLASS_NAME));
        long txnId = rs.getLong(1);
        long iterations = (SIConstants.TRASANCTION_INCREMENT - 2) * 4;
        for (int i = 0; i < iterations; ++i) {
            ps.setInt(1, i);
            ps.addBatch();
        }
        ps.executeBatch();
        conn1.commit();
        long count = conn1.count("select * from e");
        assertEquals("Incorrect count after savepoint release!",iterations,count);

        rs = conn1.query("call SYSCS_UTIL.SYSCS_GET_CURRENT_TRANSACTION()");
        rs.next();
        long txnIdLater = rs.getLong(1);

        // The difference should be 0x1000: the batch created up to 14 persisted txns, that's 28 timestamps (begin + commit),
        // the user transaction committed (1 ts) and then the new user transaction started (1 ts)
        Assert.assertTrue("Created more persisted transactions than expected, difference = " + (txnIdLater - txnId), txnIdLater <= txnId + 0x1E00);
    }

    @Test
    public void testSomePersistedSavepointsRollbacks() throws Exception {
        Savepoint first = conn1.setSavepoint("test");
        for (int i = 0; i < SIConstants.TRASANCTION_INCREMENT; ++i) {
            Savepoint savepoint = conn1.setSavepoint("test" + i);
            conn1.execute(String.format("insert into f (i) values (%d)", i));
        }
        conn1.rollback(first);
        long count = conn1.count("select * from f");
        assertEquals("Incorrect count after savepoint rollback!",0,count);

        for (int i = 0; i < SIConstants.TRASANCTION_INCREMENT; ++i) {
            Savepoint savepoint = conn1.setSavepoint("test" + i);
            conn1.execute(String.format("insert into f (i) values (%d)", i));
        }
        conn1.commit();
        count = conn1.count("select * from f");
        assertEquals("Incorrect count after savepoint release!",SIConstants.TRASANCTION_INCREMENT,count);
    }

    @Test
    public void testRollbackToSavepointDoesntCloseResultsets() throws SQLException {
        try (Statement st = conn1.createStatement()) {
            st.execute("insert into g values 1,2");
            for (int i = 0; i < 12; ++i) {
                st.execute("insert into g select * from g");
            }
            try (ResultSet rs = st.executeQuery("select * from g")) {
                int n = 0;
                Savepoint s = null;
                while (rs.next()) {
                    if (n == 0) s = conn1.setSavepoint("p0");
                    if (n == 10) {
                        conn1.rollback(s);
                    }
                    n++;
                }
                conn1.commit();
                assertEquals(8192, n);
            }
        }
    }
}
