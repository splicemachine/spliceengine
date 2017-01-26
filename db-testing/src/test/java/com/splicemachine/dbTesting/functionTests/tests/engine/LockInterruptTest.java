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

package com.splicemachine.dbTesting.functionTests.tests.engine;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import junit.framework.Test;
import junit.framework.TestSuite;

import com.splicemachine.dbTesting.junit.BaseJDBCTestCase;
import com.splicemachine.dbTesting.junit.CleanDatabaseTestSetup;
import com.splicemachine.dbTesting.junit.DatabasePropertyTestSetup;
import com.splicemachine.dbTesting.junit.JDBC;
import com.splicemachine.dbTesting.junit.SystemPropertyTestSetup;
import com.splicemachine.dbTesting.junit.TestConfiguration;

/**
 * Test that other threads are able to proceed if one thread is interrupted
 * when it's waiting for a lock. Regression test case for DERBY-4711.
 */
public class LockInterruptTest extends BaseJDBCTestCase {
    /** SQLState for exception thrown because of interrupts. */
    private final static String INTERRUPTED = "08000";

    /** Lock timeout in seconds used in this test. */
    private final static int LOCK_TIMEOUT = 60;

    /** Deadlock timeout in seconds used in this test. */
    private final static int DEADLOCK_TIMEOUT = LOCK_TIMEOUT / 2;

    public LockInterruptTest(String name) {
        super(name);
    }

    public static Test suite() {
        
        // Only run in embedded mode since we cannot interrupt the engine
        // thread from the network client.
        Test test = TestConfiguration.embeddedSuite(LockInterruptTest.class);

        // Set the lock timeout to a known value so that we know what to
        // expect for timeouts.
        test = DatabasePropertyTestSetup.setLockTimeouts(
                test, DEADLOCK_TIMEOUT, LOCK_TIMEOUT);
        Properties syspros = new Properties();
        //Derby-4856 interrupt error create thread dump and diagnostic
        //info. Add property to avoid the information.
        syspros.put("derby.stream.error.extendedDiagSeverityLevel", "50000");
        test = new SystemPropertyTestSetup(test, syspros, true);
        

        return new CleanDatabaseTestSetup(test);
    }

    public void testInterruptLockWaiter() throws Exception {
        setAutoCommit(false);
        Statement s = createStatement();
        s.executeUpdate("create table derby4711(x int)");
        commit();

        // Obtain a table lock in order to block the waiter threads.
        s.executeUpdate("lock table derby4711 in share mode");

        // Create first waiter thread.
        Waiter t1 = new Waiter();
        t1.start();
        Thread.sleep(2000); // give t1 time to become the first waiter

        // Create second waiter thread.
        Waiter t2 = new Waiter();
        t2.start();
        Thread.sleep(2000); // give t2 time to enter the wait queue

        // Now that the queue of waiters has been set up, interrupt the
        // first thread and give the interrupt a little time to do its work.
        t1.interrupt();
        Thread.sleep(1000);

        // Release the table lock to allow the waiters to proceed.
        commit();

        // Wait for the threads to complete before checking their state.
        t1.join();
        t2.join();

        // The first thread should fail because it was interrupted.
        Throwable e1 = t1.throwable;
        assertNotNull("First thread should fail because of interrupt", e1);
        if (!(e1 instanceof SQLException)) {
            fail("Unexpected exception from first thread", e1);
        }
        assertSQLState(INTERRUPTED, (SQLException) e1);

        if (hasInterruptibleIO()) {
            println("Skipping assert for t1.InterruptFlagSetOnThrow due " +
                    " to interruptible IO.");
            println("This is default on Solaris/Sun Java <= 1.6, use " +
                    "-XX:-UseVMInterruptibleIO if available.");
            // The flag will may get swallowed
        } else {
            assertTrue(t1.InterruptFlagSetOnThrow);
        }

        // The second thread should be able to complete successfully.
        Throwable e2 = t2.throwable;
        if (e2 != null) {
            fail("Unexpected exception from second thread", e2);
        }

        // And the second thread should be able to complete in less time than
        // the deadlock timeout (before DERBY-4711, it would wait for a
        // timeout before obtaining the lock, even if the lock was available
        // long before).
        if (t2.elapsedTime >= DEADLOCK_TIMEOUT * 1000) {
            fail("Second thread needed " + t2.elapsedTime +
                 " ms to complete. Probably stuck waiting for a lock.");
        }

        // Expect that the second thread managed to insert a row.
        JDBC.assertSingleValueResultSet(
                s.executeQuery("select * from derby4711"), "1");
    }

    /**
     * Thread class that opens a new connection and attempts to insert a
     * row into a table.
     */
    private class Waiter extends Thread {
        private final Connection c;
        private final PreparedStatement ps;

        private Throwable throwable;
        private boolean InterruptFlagSetOnThrow;
        private long elapsedTime;

        private Waiter() throws SQLException {
            c = openDefaultConnection();
            ps = c.prepareStatement("insert into derby4711 values 1");
        }

        public void run() {
            try {
                runWaiter();
            } catch (Throwable t) {
                throwable = t;
                InterruptFlagSetOnThrow = interrupted(); // clears also
            }
        };

        private void runWaiter() throws SQLException {
            long start = System.currentTimeMillis();
            try {
                ps.executeUpdate();
            } finally {
                ps.close();
                c.close();
            }
            elapsedTime = System.currentTimeMillis() - start;
        }
    }
}
