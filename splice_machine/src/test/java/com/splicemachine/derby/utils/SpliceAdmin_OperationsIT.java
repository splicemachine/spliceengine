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
 *
 */

package com.splicemachine.derby.utils;

import com.splicemachine.derby.test.framework.*;
import com.splicemachine.homeless.TestUtils;
import com.splicemachine.test.HBaseTest;
import com.splicemachine.test.SerialTest;
import org.apache.log4j.Logger;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.*;

@Category(SerialTest.class)
public class SpliceAdmin_OperationsIT extends SpliceUnitTest{
    private static final Logger LOG = Logger.getLogger(SpliceAdmin_OperationsIT.class);
    public static final String CLASS_NAME = SpliceAdmin_OperationsIT.class.getSimpleName().toUpperCase();
    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher(CLASS_NAME);
    protected static SpliceTableWatcher bigTableWatcher = new SpliceTableWatcher("TEST_BIG",CLASS_NAME,"(a int)");
    protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);
    protected static final String USER_NAME = CLASS_NAME+"_USER";
    private SpliceTestDataSource dataSource;

    @BeforeClass
    public static void setUpClass() throws Exception {
        spliceClassWatcher.execute("GRANT ACCESS ON SCHEMA " + CLASS_NAME + " TO PUBLIC");
    }

    @Before
    public void startup() {
        dataSource = new SpliceTestDataSource();
    }

    @After
    public void shutdown() {
        dataSource.shutdown();
    }

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher).
            around(spliceSchemaWatcher).around(bigTableWatcher)
            .around(new SpliceDataWatcher(){
                @Override
                protected void starting(Description description){
                    try {
                        spliceClassWatcher.execute(String.format("call SYSCS_UTIL.SYSCS_CREATE_USER('%s', '%s')", USER_NAME, USER_NAME));
                    } catch (Exception e) {
                        // ignore if user already exists
                    }

                    try {
                        spliceClassWatcher.execute("GRANT EXECUTE ON PROCEDURE SYSCS_UTIL.SYSCS_GET_RUNNING_OPERATIONS TO " + USER_NAME);
                        spliceClassWatcher.execute("GRANT EXECUTE ON PROCEDURE SYSCS_UTIL.SYSCS_KILL_OPERATION TO " + USER_NAME);
                        spliceClassWatcher.execute("GRANT EXECUTE ON PROCEDURE SYSCS_UTIL.SYSCS_GET_RUNNING_OPERATIONS_LOCAL TO " + USER_NAME);
                        spliceClassWatcher.execute("GRANT EXECUTE ON PROCEDURE SYSCS_UTIL.SYSCS_KILL_OPERATION_LOCAL TO " + USER_NAME);
                        spliceClassWatcher.execute("GRANT ALL PRIVILEGES ON TABLE " + bigTableWatcher + " TO " + USER_NAME);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                    try{
                        PreparedStatement ps=spliceClassWatcher.prepareStatement(format("insert into %s (a) values (?)",bigTableWatcher));
                        for(int i=0;i<10;i++){
                            ps.setInt(1,i);
                            ps.executeUpdate();
                        }
                        ps=spliceClassWatcher.prepareStatement(format("insert into %s select * from %s",bigTableWatcher, bigTableWatcher));
                        for(int i=0;i<12;i++){
                            ps.executeUpdate();
                        }
                    }catch(Exception e){
                        throw new RuntimeException(e);
                    }finally{
                        spliceClassWatcher.closeAll();
                    }
                }

            });

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(spliceSchemaWatcher.schemaName);


    @BeforeClass
    public static void killRunningOperations() throws Exception {
        TestUtils.killRunningOperations(spliceClassWatcher);
    }

    @Test
    public void testRunningOperations() throws Exception {
        String sql= "call SYSCS_UTIL.SYSCS_GET_RUNNING_OPERATIONS()";

        PreparedStatement ps = methodWatcher.getOrCreateConnection().prepareStatement(sql);
        ResultSet rs = ps.executeQuery();
        // we should have 1 running operation, the call itself
        assertTrue(rs.next());
        assertEquals("SPLICE", rs.getString(2)); // check user
        assertEquals(sql, rs.getString(5)); // check sql
    }

    @Test
    @Category(HBaseTest.class) // No Spark in MEM mode
    public void testRunningOperation_DB6478() throws Exception {
        String sql= "select count(*) from TEST_BIG --splice-properties useSpark=true" + "\n" +
                     "natural join TEST_BIG b";

        AtomicReference<Exception> result = new AtomicReference<>();

        Thread thread = new Thread(new Runnable() {
            @Override
            public void run() {
                PreparedStatement ps = null;
                try (TestConnection connection = methodWatcher.createConnection()) {
                    ps = connection.prepareStatement(sql);
                    ResultSet rs = ps.executeQuery();
                    assertTrue(rs.next());
                    while(rs.next()) {
                    }
                } catch (Exception e) {
                    LOG.error("Unexpected exception", e);
                    result.set(e);
                }
            }
        });
        thread.start();
        // wait for the query to be submitted
        Thread.sleep(1000);

        String opsCall= "call SYSCS_UTIL.SYSCS_GET_RUNNING_OPERATIONS()";

        try (TestConnection connection = methodWatcher.createConnection()) {
            String submitted = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date());
            ResultSet opsRs = connection.query(opsCall);
            while (opsRs.next()) {
                if (opsRs.getString(5).equals(sql)) {
                    assertEquals("OLAP",opsRs.getString(8)); // check engine "OLAP" (=Spark)
                    assertEquals("Produce Result Set", opsRs.getString(9)); // check job type
                } else if (opsRs.getString(5).equals(opsCall)) {
                    assertEquals(submitted, opsRs.getString(6)); // check submitted time
                    assertEquals("SYSTEM", opsRs.getString(8));
                    assertEquals("Call Procedure", opsRs.getString(9)); // check job type
                }
            }
            // wait for Thread termination
            thread.join();
            assertNull(result.get());
        }
    }

    @Test
    public void testKillOpenCursorControl() throws Exception {
        testKillOpenCursor(false);
    }

    @Test
    public void testKillOpenCursorSpark() throws Exception {
        testKillOpenCursor(true);
    }

    @Test
    public void testLongRunningControl() throws Exception {
        testKillLongRunningQuery(false);
    }

    @Test
    public void testLongRunningSpark() throws Exception {
        testKillLongRunningQuery(true);
    }

    public void testKillOpenCursor(boolean useSpark) throws Exception {
        String sql= "select * from "+bigTableWatcher + " --splice-properties useSpark="+useSpark;

        PreparedStatement ps = methodWatcher.getOrCreateConnection().prepareStatement(sql);
        ResultSet rs = ps.executeQuery();
        assertTrue(rs.next());

        String opsCall= "call SYSCS_UTIL.SYSCS_GET_RUNNING_OPERATIONS()";

        ResultSet opsRs = methodWatcher.getOrCreateConnection().query(opsCall);

        int count = 0;
        String uuid = null;
        while(opsRs.next()) {
            count++;
            if (opsRs.getString(5).equals(sql)) {
                uuid = opsRs.getString(1);
            }
        }
        assertEquals(2, count); // 2 running operations, cursor + the procedure call itself

        // kill the cursor
        String killCall = "call SYSCS_UTIL.SYSCS_KILL_OPERATION('"+uuid+"')";
        methodWatcher.getOrCreateConnection().execute(killCall);

        // iterate over the cursor, should raise exception
        int rows = 0;
        try {
            while(rs.next()) {
                rows++;
            }
            fail("Should have raised exception");
        } catch (SQLTimeoutException se) {
            LOG.debug("Raised exception after " + rows + " rows.");
            assertEquals("57014", se.getSQLState());
        }
    }

    public void testKillLongRunningQuery(boolean useSpark) throws Exception {
        String sql= "select count(*) from TEST_BIG --splice-properties useSpark="+useSpark + "\n" +
                "natural join TEST_BIG b natural join TEST_BIG c natural join TEST_BIG c natural join TEST_BIG d";

        AtomicReference<Exception> result = new AtomicReference<>();

        Thread thread = new Thread(new Runnable() {
            @Override
            public void run() {
                PreparedStatement ps = null;
                try (TestConnection connection = methodWatcher.createConnection()) {
                    ps = connection.prepareStatement(sql);
                    ResultSet rs = ps.executeQuery();
                    assertTrue(rs.next());
                    while(rs.next()) {
                    }
                } catch (Exception e) {
                    result.set(e);
                }
            }
        });
        thread.start();
        
        // wait for the query to be submitted
        Thread.sleep(1000);

        String opsCall= "call SYSCS_UTIL.SYSCS_GET_RUNNING_OPERATIONS()";

        try (TestConnection connection = methodWatcher.createConnection()) {
            ResultSet opsRs = connection.query(opsCall);

            int count = 0;
            String uuid = null;
            while (opsRs.next()) {
                count++;
                if (opsRs.getString(5).equals(sql)) {
                    uuid = opsRs.getString(1);
                }
            }
            assertEquals(2, count); // 2 running operations, cursor + the procedure call itself

            // kill the cursor
            String killCall = "call SYSCS_UTIL.SYSCS_KILL_OPERATION('" + uuid + "')";
            connection.execute(killCall);

            // wait for Thread termination
            thread.join();
            assertNotNull(result.get());
            Exception e = result.get();
            assertTrue(e instanceof SQLTimeoutException);
            assertEquals("57014", ((SQLException) e).getSQLState());
        }
    }

    @Test
    public void testOtherUsersCantKillOperation() throws Exception {
        TestConnection connection1 = methodWatcher.getOrCreateConnection();
        TestConnection connection2 = methodWatcher.connectionBuilder().user(USER_NAME).password(USER_NAME).build();


        String sql= "select * from "+bigTableWatcher;

        PreparedStatement ps = connection1.prepareStatement(sql);
        ResultSet rs = ps.executeQuery();
        assertTrue(rs.next());


        // View from conn1
        String opsCall= "call SYSCS_UTIL.SYSCS_GET_RUNNING_OPERATIONS()";

        ResultSet opsRs = connection1.query(opsCall);

        int count = 0;
        String uuid = null;
        while(opsRs.next()) {
            count++;
            if (opsRs.getString(5).equals(sql)) {
                uuid = opsRs.getString(1);
            }
        }
        assertEquals(2, count); // 2 running operations, cursor + the procedure call itself
        opsRs.close();


        // View from conn2
        opsRs = connection2.query(opsCall);

        count = 0;
        while(opsRs.next()) {
            count++;
        }
        assertEquals(1, count); // the open cursor should not be visible


        // try to kill the cursor
        String killCall = "call SYSCS_UTIL.SYSCS_KILL_OPERATION('"+uuid+"')";
        try {
            connection2.execute(killCall);
            fail("Should have raised exception");
        } catch (SQLException se) {
            assertEquals("4251P", se.getSQLState());
        }


        // kill the cursor
        connection1.execute(killCall);

        // iterate over the cursor, should raise exception
        int rows = 0;
        try {
            while(rs.next()) {
                rows++;
            }
            fail("Should have raised exception");
        } catch (SQLTimeoutException se) {
            LOG.debug("Raised exception after " + rows + " rows.");
            assertEquals("57014", se.getSQLState());
        }
    }

    @Test
    public void testDbOwnerCanKillOperation() throws Exception {
        TestConnection connection1 = methodWatcher.connectionBuilder().user(USER_NAME).password(USER_NAME).build();
        TestConnection connection2 = methodWatcher.getOrCreateConnection();


        String sql= "select * from "+bigTableWatcher;

        PreparedStatement ps = connection1.prepareStatement(sql);
        ResultSet rs = ps.executeQuery();
        assertTrue(rs.next());


        // View from conn1
        String opsCall= "call SYSCS_UTIL.SYSCS_GET_RUNNING_OPERATIONS()";

        ResultSet opsRs = connection1.query(opsCall);

        int count = 0;
        String uuid = null;
        while(opsRs.next()) {
            count++;
            if (opsRs.getString(5).equals(sql)) {
                uuid = opsRs.getString(1);
            }
        }
        assertEquals(2, count); // 2 running operations, cursor + the procedure call itself
        opsRs.close();


        // View from conn2
        opsRs = connection2.query(opsCall);

        count = 0;
        while(opsRs.next()) {
            count++;
        }
        assertEquals(2, count); // the open cursor should also be visible to the db owner


        // kill the cursor
        String killCall = "call SYSCS_UTIL.SYSCS_KILL_OPERATION('"+uuid+"')";
        connection2.execute(killCall);

        // iterate over the cursor, should raise exception
        int rows = 0;
        try {
            while(rs.next()) {
                rows++;
            }
            fail("Should have raised exception");
        } catch (SQLTimeoutException se) {
            LOG.debug("Raised exception after " + rows + " rows.");
            assertEquals("57014", se.getSQLState());
        }

        // try to kill the cursor again (it should fail)
        try {
            connection1.execute(killCall);
            fail("Should have raised exception");
        } catch (SQLException se) {
            assertEquals("4251P", se.getSQLState());
        }

    }

    @Test
    public void testKillBadUUID() throws Exception {
        try {
            // wrong format for the UUID
            String killCall = "call SYSCS_UTIL.SYSCS_KILL_OPERATION('')";
            methodWatcher.getOrCreateConnection().execute(killCall);
            fail("Should have raised exception");
        } catch (SQLException se) {
            assertEquals("4251P", se.getSQLState());
        }

        try {
            // Invented UUID
            String killCall = "call SYSCS_UTIL.SYSCS_KILL_OPERATION('00000000-0000-0000-0000-000000000000')";
            methodWatcher.getOrCreateConnection().execute(killCall);
            fail("Should have raised exception");
        } catch (SQLException se) {
            assertEquals("4251P", se.getSQLState());
        }
    }

    @Test
    @Category(HBaseTest.class) // we require a distributed environment for this test
    public void testOtherServersCanKillOperation() throws Exception {
        Connection connection1 = dataSource.getConnection("localhost", 1527);
        Connection connection2 = dataSource.getConnection("localhost", 1528);


        String sql= "select * from "+bigTableWatcher;

        PreparedStatement ps = connection1.prepareStatement(sql);
        ResultSet rs = ps.executeQuery();
        assertTrue(rs.next());


        // View from conn1
        String opsCall= "call SYSCS_UTIL.SYSCS_GET_RUNNING_OPERATIONS()";

        ResultSet opsRs = connection1.createStatement().executeQuery(opsCall);

        int count = 0;
        String uuid = null;
        while(opsRs.next()) {
            count++;
            if (opsRs.getString(5).equals(sql)) {
                uuid = opsRs.getString(1);
            }
        }
        assertEquals(2, count); // 2 running operations, cursor + the procedure call itself
        opsRs.close();


        // View from conn2
        opsRs = connection2.createStatement().executeQuery(opsCall);

        count = 0;
        while(opsRs.next()) {
            count++;
        }
        assertEquals(2, count); // 2 running operations, cursor + the procedure call itself


        // try to kill the cursor
        String killCall = "call SYSCS_UTIL.SYSCS_KILL_OPERATION('"+uuid+"')";
        connection2.createStatement().execute(killCall);

        // iterate over the cursor, should raise exception
        int rows = 0;
        try {
            while(rs.next()) {
                rows++;
            }
            fail("Should have raised exception");
        } catch (SQLTimeoutException se) {
            LOG.debug("Raised exception after " + rows + " rows.");
            assertEquals("57014", se.getSQLState());
        }
    }

}
