/*
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
 * Some parts of this source code are based on Apache Derby, and the following notices apply to
 * Apache Derby:
 *
 * Apache Derby is a subproject of the Apache DB project, and is licensed under
 * the Apache License, Version 2.0 (the "License"); you may not use these files
 * except in compliance with the License. You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * Splice Machine, Inc. has modified the Apache Derby code in this file.
 *
 * All such Splice Machine modifications are Copyright 2012 - 2017 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */
package com.splicemachine.dbTesting.functionTests.tests.store;

import com.splicemachine.dbTesting.junit.BaseJDBCTestCase;
import com.splicemachine.dbTesting.junit.TestConfiguration;
import com.splicemachine.dbTesting.junit.JDBC;
import com.splicemachine.dbTesting.junit.SystemPropertyTestSetup;

import junit.framework.Test;
import junit.framework.TestSuite;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.SQLException;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.Random;
import java.lang.Math;
import java.util.Properties;

/**
 * This test started out as a test reproduce and verify fix for DERBY-151.
 * Later evolved into test for DERBY-4741.
 * <p/>
 * The use of stored procedures was done to make the tests meaningful in client
 * server mode as well, but be removed/simplified as long as we only make
 * claims about the resilience of embedded Derby.
 */

public class InterruptResilienceTest extends BaseJDBCTestCase
{

    public InterruptResilienceTest(String name)
    {
        super(name);
    }

    // Share the main thread's configuration with the server side threads.
    private static volatile TestConfiguration thisConf;

    protected static Test makeSuite(String name)
    {
        TestSuite suite = new TestSuite(name);

        Test est = TestConfiguration.embeddedSuite(
            InterruptResilienceTest.class);
        Test cst = TestConfiguration.clientServerSuite(
            InterruptResilienceTest.class);

        est = TestConfiguration.singleUseDatabaseDecorator(est);
        cst = TestConfiguration.singleUseDatabaseDecorator(cst);
        // Cut down on running time:
        Properties p = new Properties();
        p.put("derby.system.durability", "test");
        p.put("derby.infolog.append", "true");
        // we'll force interrupts and thus serious errors, which with
        // ibm jvms would result in javacore files, which aren't of 
        // interest if the test passes. Setting the stream error level 
        // so we don't get those javacores.
        p.put("derby.stream.error.extendedDiagSeverityLevel", "50000");

        suite.addTest(
                new SystemPropertyTestSetup(est, p, true));

        suite.addTest(
                new SystemPropertyTestSetup(cst, p, true));
        return suite;
    }

    public static Test suite()
    {
        String testName = "InterruptResilienceTest";

        if (isIBMJVM()) {
            if (getSystemProperty("java.version").startsWith("1.4.2"))
            {
                println("InterruptResilienceTest skipped for this VM, cf. DERBY-5074/5109");
                return new TestSuite(testName);
            }
        }

        if (!JDBC.vmSupportsJDBC3()) {
            println("Test skipped for this VM, " +
                    "DriverManager is not supported with JSR169");
            return new TestSuite(testName);
        }

        if (hasInterruptibleIO()) {
            println("Test skipped due to interruptible IO.");
            println("This is default on Solaris/Sun Java <= 1.6, use " +
                    "-XX:-UseVMInterruptibleIO if available.");
            return new TestSuite(testName);
        }

        return makeSuite(testName);
    }

    protected void setUp()
            throws java.lang.Exception {
        try {
            Class.forName("com.splicemachine.db.jdbc.EmbeddedDriver").newInstance();
        } catch (Exception e) {
        }
        super.setUp();
        Statement stmt = createStatement();
        stmt.executeUpdate("create table t1(x int primary key)");
        stmt.executeUpdate("create table mtTab(i bigint, " +
                           "inserter varchar(40), " +
                           "primary key(i, inserter))");
        stmt.close();

        thisConf = TestConfiguration.getCurrent();
        threadNo = 0;    // counter for multiple threads tests
        allDone = false; // flag for threads to terminate
    }

    /**
     * Clean up the connection maintained by this test.
     */
    protected void tearDown()
            throws java.lang.Exception {

        // Forget about uncommitted changes
        rollback();

        // Drop the tables created in setUp() if they still exist
        dropTable("t1");
        dropTable("mtTab");
        commit();

        super.tearDown();
    }

    // We do the actual test inside a stored procedure so we can test this for
    // client/server as well, otherwise we would just interrupt the client
    // thread. This SP correposnds to #testRAFWriteInterrupted.
    public static void tstRAFwriteInterrupted() throws SQLException {
        Connection c = DriverManager.getConnection("jdbc:default:connection");
        c.setAutoCommit(false);
        PreparedStatement insert = null;
        long seen = 0;
        long lost = 0;
        try {
            insert = c.prepareStatement("insert into t1 values (?)");

            // About 75000 iterations is needed to see any concurrency
            // wait on RawDaemonThread during recovery, cf.
            // running with debug flag "RAF4" for RAFContainer4.
            for (int i = 0; i < 100000; i++) {
                if (i % 1000 == 0) {
                    c.commit();
                }

                // Make sure to interrupt after commit, since log writing isn't
                // safe for interrupts (on Solaris only) yet.
                Thread.currentThread().interrupt();

                insert.setLong(1, i);
                insert.executeUpdate();

                assertTrue("interrupt flag lost", Thread.interrupted());
            }
        } finally {
            // always clear flag
            Thread.interrupted();

            if (insert != null) {
                try {
                    insert.close(); // already closed by error
                } catch (SQLException e) {
                }
            }

            c.close();
        }
    }

    public void testRAFWriteInterrupted () throws SQLException {
        Statement s = createStatement();
        s.executeUpdate(
            "create procedure tstRAFWriteInterrupted () modifies sql data " +
            "external name 'com.splicemachine.dbTesting.functionTests" +
            ".tests.store.InterruptResilienceTest.tstRAFwriteInterrupted' " +
            "language java parameter style java");

        s.executeUpdate("call tstRAFWriteInterrupted()");
    }


    // We do the actual test inside a stored procedure so we can test this for
    // client/server as well, otherwise we would just interrupt the client
    // thread. This SP correponds to #testRAFReadWriteMultipleThreads.
    public static void tstRAFReadWriteMultipleThreads() throws Exception {

        //--------------------
        // part 1
        //--------------------

        Connection c = DriverManager.getConnection("jdbc:default:connection");

        ArrayList workers = new ArrayList();

        ArrayList interruptors = new ArrayList();

        for (int i = 0; i < NO_OF_THREADS; i++) {
            WorkerThread w = new WorkerThread(
                thisConf.openDefaultConnection(),
                false /* read */,
                NO_OF_MT_OPS);

            workers.add(w);

            w.start();
            try {
                Thread.sleep(1000);
            } catch (Exception e) {
            }

            InterruptorThread it = new InterruptorThread(w, 500);
            interruptors.add(it);
            it.start();
        }

        for (int i = 0; i < workers.size(); i++) {
            WorkerThread w = (WorkerThread)workers.get(i);
            w.join();

            if (w.e != null) {
                fail("WorkerThread " + i, w.e);
            }
        }

        allDone = true;

        for (int i = 0; i < interruptors.size(); i++) {
            ((Thread)interruptors.get(i)).join();
        }

        try {
            Thread.sleep(1000);
        } catch (Exception e) {
        }

        Statement s = c.createStatement();
        ResultSet rs = s.executeQuery("select count(*) from mtTab");

        JDBC.assertSingleValueResultSet(
            rs, Long.toString(NO_OF_THREADS * NO_OF_MT_OPS));

        //--------------------
        // part 2
        //--------------------

        // Reset thread state variables
        allDone = false;
        threadNo = 0;

        workers.clear();
        interruptors.clear();

        for (int i = 0; i < NO_OF_THREADS; i++) {
            WorkerThread w = new WorkerThread(
                // This will be an embedded connection always since for the
                // server thread current cf will be JUNIT_CONFIG.
                thisConf.openDefaultConnection(),
                true,
                NO_OF_MT_OPS);

            workers.add(w);

            try {
                Thread.sleep(1000);
            } catch (Exception e) {
            }

            InterruptorThread it = new InterruptorThread(w, 500);
            interruptors.add(it);
            it.start();
        }

        // Wait till here to start works, so interruptors don't get too late to
        // the game
        for (int i = 0; i < workers.size(); i++) {
            ((Thread)workers.get(i)).start();
        }

        for (int i = 0; i < workers.size(); i++) {
            WorkerThread w = (WorkerThread)workers.get(i);
            w.join();

            if (w.e != null) {
                fail("WorkerThread " + i, w.e);
            }
        }

        allDone = true;

        for (int i = 0; i < interruptors.size(); i++) {
            ((Thread)interruptors.get(i)).join();
        }

        c.close();
    }


    static class InterruptorThread extends Thread {
        private WorkerThread myVictim;
        private int millisBetweenShots;

        public InterruptorThread(WorkerThread v, int m){
            super();
            myVictim = v;
            millisBetweenShots = m;
        }

        public void run() {
            setName("InterruptorThread. Thread #" + getThreadNo());
            println("Running " + getName() +
                    " with victim " + myVictim.getName());

            int shots = 0;

            while (!allDone) {
                try {
                    Thread.sleep(millisBetweenShots);
                    myVictim.interrupt();
                    shots++;
                } catch (Exception e) {
                }
            }

            println(getName() + " shot " + shots +
                    " interrupts at " + myVictim.getName());
        }
    }


    static class WorkerThread extends Thread {
        private final boolean readertest;
        private final long noOps;
        public Throwable e; // if any seen
        private Connection c;

        public WorkerThread(Connection c, boolean readertest, long noOps) {
            super();
            this.readertest = readertest;
            this.noOps = noOps;
            this.c = c;
        }

        public void run() {
            int threadNo = getThreadNo();
            int interruptsSeen = 0;

            setName("WorkerThread. Thread#" + threadNo);
            println("Running " + getName());

            try {
                c.setAutoCommit(false);

                String pStmtText =
                    readertest ?
                    "select * from mtTab where i=?" :
                    "insert into mtTab values (?,?)";
                PreparedStatement s = c.prepareStatement(pStmtText);

                Random rnd = new Random();

                int retries = 0;

                for (long ops = 0; ops < noOps + retries; ops++) {

                    if (readertest) {
                        // Arbitrarily select one of the rows int the tables to
                        // read
                        long candidate = randAbs(rnd.nextLong()) % noOps;
                        s.setLong(1, candidate);

                        // Since when we query, we might see 08000 if the
                        // interrupt flag is set when the rs.getNextRow calls
                        // checkCancellationFlag, we must be prepared to
                        // reestablish connection.

                        try {
                            ResultSet rs = s.executeQuery();
                            rs.next();
                            if (interrupted()) {
                                interruptsSeen++;
                            }

                            assertEquals("wrong row content",
                                         candidate, rs.getLong(1));

                            rs.close();
                        } catch (SQLException e) {
                            if ("08000".equals(e.getSQLState())) {
                                c = thisConf.openDefaultConnection();
                                s = c.prepareStatement(pStmtText);
                                assertTrue(interrupted());
                                interruptsSeen++;
                                retries++;
                                continue;
                            } else {
                                fail("expected 08000", e);
                            }
                        }

                        c.commit();

                        if (interrupted()) {
                            interruptsSeen++;
                        }
                    } else {
                        s.setLong(1, ops);
                        s.setString(2, getName());

                        try {
                            s.executeUpdate();
                        } catch (SQLException e) {
                            // Occasionally we could see a lock wait being
                            // interrupted: reconnect and continue. DERBY-5001.
                            // See also LockInterruptTest.
                            if ("08000".equals(e.getSQLState())) {
                                c = thisConf.openDefaultConnection();
                                s = c.prepareStatement(pStmtText);
                                assertTrue(interrupted());
                                interruptsSeen++;
                                retries++;
                                continue;
                            } else {
                                fail("expected 08000", e);
                            }
                        }


                        if (interrupted()) {
                            interruptsSeen++;
                        }

                        c.commit();

                        if (interrupted()) {
                            interruptsSeen++;
                        }
                    }
                }
                s.close();
            } catch (Throwable e) {
                this.e = e;
            } finally {
                try { c.close(); } catch (Exception e) {}
            }

            println("Thread " + getName() + " saw " + interruptsSeen +
                    " interrupts");
        }
    }

    // Number of parallel threads to use
    static int NO_OF_THREADS = 3;

    static long NO_OF_MT_OPS = 10000;

    // Counter to enumerate threads for tests employing several threads.  Reset
    // for each test in setUp.
    private static int threadNo;

    synchronized static int getThreadNo() {
        return ++threadNo;
    }

    private static long randAbs(long l) {
        if (l == Long.MIN_VALUE) {
            return Long.MAX_VALUE; // 2's complement, so no way to make value
                                   // positive
        } else {
            return Math.abs(l);
        }
    }

    // Signal to threads to stop whatever they are doing. Reset
    // for each test in setUp.
    static volatile boolean allDone;

    /**
     * MT write (part 1) and read (part 2) test under interrupt shower.  This
     * stess tests the NIO random access file interrupt channel recovery in
     * RAFContainer4.
     */
    public void testRAFReadWriteMultipleThreads () throws SQLException {
        Statement s = createStatement();

        s.executeUpdate(
            "create procedure tstRAFReadWriteMultipleThreads () " +
            "modifies sql data " +
            "external name 'com.splicemachine.dbTesting.functionTests" +
            ".tests.store.InterruptResilienceTest" +
            ".tstRAFReadWriteMultipleThreads' " +
            "language java parameter style java");

        s.executeUpdate("call tstRAFReadWriteMultipleThreads()");
    }

    // We do the actual test inside a stored procedure so we can test this for
    // client/server as well, otherwise we would just interrupt the client
    // thread. This SP correponds to #testLongQueryInterrupt
    public static void tstInterruptLongQuery() throws Exception {
        Connection c = DriverManager.getConnection("jdbc:default:connection");
        Statement s = c.createStatement();

        try {
            Thread.currentThread().interrupt();
            ResultSet rs = s.executeQuery(
                "select * from sys.syscolumns");
            while (rs.next()) {};
            fail("expected CONN_INTERRUPT");
        } catch (SQLException e) {
            assertSQLState("expected CONN_INTERRUPT", "08000", e);
            // assertTrue(c.isClosed()); // DERBY-4993
            assertTrue(Thread.interrupted());
        }
    }

    // Test that query if interrupted will get stopped as expected in
    // BasicNoPutResultSetImpl#checkCancellationFlag
    public void testLongQueryInterrupt() throws SQLException {
        Connection c = getConnection();
        Statement s = createStatement();
        s.executeUpdate(
            "create procedure tstInterruptLongQuery() " +
            "reads sql data " +
            "external name 'com.splicemachine.dbTesting.functionTests" +
            ".tests.store.InterruptResilienceTest" +
            ".tstInterruptLongQuery' " +
            "language java parameter style java");
        try {
            s.executeUpdate("call tstInterruptLongQuery()");
            fail("expected 40XC0 exception");
        } catch (SQLException e) {
            assertSQLState("expected 40XC0", "40XC0", e); // dead statement
            assertTrue(c.isClosed());
        }

    }


    // We do the actual test inside a stored procedure so we can test this for
    // client/server as well, otherwise we would just interrupt the client
    // thread. This SP correponds to #testInterruptBatch
    public static void tstInterruptBatch() throws Exception {
        Connection c = DriverManager.getConnection("jdbc:default:connection");
        Statement s = c.createStatement();
        s.executeUpdate("create table tmp(i int)");
        PreparedStatement ps = c.prepareStatement("insert into tmp values (?)");

        // fill batch:
        for (int i=0; i < 10; i++) {
            s.addBatch("insert into tmp values (" + i + ")");
        }

        s.executeBatch(); // should work OK, since no interrupt present

        // refill batch:
        for (int i=0; i < 10; i++) {
            s.addBatch("insert into tmp values (" + i + ")");
        }

        try {
            Thread.currentThread().interrupt();
            s.executeBatch();
            fail("expected CONN_INTERRUPT");
        } catch (SQLException e) {
            assertSQLState("expected CONN_INTERRUPT", "08000", e);
            // assertTrue(c.isClosed()); // DERBY-4993
            assertTrue(Thread.interrupted());
        }
    }


    // Test that batched statements, if interrupted, will get stopped as
    // expected.
    public void testInterruptBatch() throws SQLException {
        Connection c = getConnection();
        Statement s = createStatement();
        setAutoCommit(false);

        s.executeUpdate(
            "create procedure tstInterruptBatch() " +
            "modifies sql data " +
            "external name 'com.splicemachine.dbTesting.functionTests" +
            ".tests.store.InterruptResilienceTest" +
            ".tstInterruptBatch' " +
            "language java parameter style java");
        try {
            s.executeUpdate("call tstInterruptBatch()");
            fail("expected 40XC0 exception");
        } catch (SQLException e) {
            assertSQLState("expected 40XC0", "40XC0", e); // dead statement
            assertTrue(c.isClosed());
        }

        setAutoCommit(false);
        s = createStatement();
        // The table created inside stored routine should be gone:
        s.executeUpdate("create table tmp(i int)");
        rollback();

    }


    public void testInterruptShutdown() throws SQLException {
        if (!usingEmbedded()) {
            // Only meaningful for embedded.
            return;
        }

        setAutoCommit(false);

        try {
            Statement s = createStatement();
            s.executeUpdate("create table foo (i int)");
            PreparedStatement ps =
                prepareStatement("insert into foo values ?");

            for (int i = 0; i < 1000; i++) {
                ps.setInt(1,i);
                ps.executeUpdate();
            }

            Thread.currentThread().interrupt();

            TestConfiguration.getCurrent().shutdownDatabase();

            // Assert thread's flag:
            // DERBY-5152: Fails before fix due to lcc going away.
            assertTrue(Thread.currentThread().isInterrupted());

        } finally {
            Thread.interrupted(); // clear flag
        }
    }

    /**
     * DERBY-5233: verify that CREATE TABLE (i.e. container creation) survives
     * interrupts with NIO.
     */
    public void testCreateDropInterrupted() throws SQLException {

        if (!usingEmbedded()) {
            // Only meaningful for embedded.
            return;
        }

        setAutoCommit(false);

        Statement s = createStatement();

        try {
            Thread.currentThread().interrupt();

            s.executeUpdate("create table foo (i int)");
            s.executeUpdate("insert into foo values 1");
            s.executeUpdate("drop table foo");

            // Assert thread's flag:
            assertTrue(Thread.currentThread().isInterrupted());

        } finally {
            Thread.interrupted(); // clear flag
        }
    }
}
