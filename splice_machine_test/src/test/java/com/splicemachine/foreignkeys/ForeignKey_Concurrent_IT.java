package com.splicemachine.foreignkeys;

import com.google.common.collect.Lists;
import com.splicemachine.concurrent.Threads;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.test_dao.TableDAO;
import com.splicemachine.test_tools.TableCreator;
import org.apache.commons.lang.math.RandomUtils;
import org.junit.*;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static com.splicemachine.test_tools.Rows.row;
import static com.splicemachine.test_tools.Rows.rows;
import static org.junit.Assert.*;

/**
 * Foreign key tests for concurrent transactions deleting parent rows and inserting child rows.
 */
public class ForeignKey_Concurrent_IT {

    private static final String SCHEMA = ForeignKey_Concurrent_IT.class.getSimpleName();

    @ClassRule
    public static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(SCHEMA);

    @ClassRule
    public static SpliceWatcher classWatcher = new SpliceWatcher(SCHEMA);

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(SCHEMA);

    @BeforeClass
    public static void createSharedTables() throws Exception {

        new TableCreator(classWatcher.getOrCreateConnection())
                .withCreate("create table P (a bigint primary key, b bigint)")
                .withInsert("insert into P values(?,?)")
                .withRows(rows(row(1, 1), row(2, 2), row(3, 3))).create();

        new TableCreator(classWatcher.getOrCreateConnection())
                .withCreate("create table C (a bigint, b bigint, CONSTRAINT fk1 FOREIGN KEY(a) REFERENCES P(a))")
                .create();
    }

    @Test(timeout=10000)
    public void concurrentTransactions_insertFirst() throws Exception {
        Connection connection1 = newNoAutoCommitConnection();
        Connection connection2 = newNoAutoCommitConnection();

        // Transaction 1: insert child row referencing parent we will delete
        connection1.createStatement().executeUpdate("insert into C values(1,1)");

        // Transaction 2: verify cannot delete/update parent
        assertQueryFail(connection2, "DELETE FROM P where a=1", "Write Conflict detected between transactions");
        assertQueryFail(connection2, "UPDATE P set a=9 where a=1", "Write Conflict detected between transactions");
        connection2.commit();
        connection2.close();

        // Transaction 2: verify CAN update parent
        connection1.createStatement().executeUpdate("update P set b=-1 where a=1");

        connection1.commit();

        // After concurrent transaction commit verify row count seen by third transaction.
        assertEquals(1L, methodWatcher.query("select count(*) from P where a=1"));
        assertEquals(1L, methodWatcher.query("select count(*) from C where a=1"));
    }

    @Test(timeout=10000)
    public void concurrentTransactions_deleteFirst() throws Exception {
        Connection connection1 = newNoAutoCommitConnection();
        Connection connection2 = newNoAutoCommitConnection();

        // Transaction 1: delete the parent row we will try to reference in another transaction
        connection1.createStatement().executeUpdate("DELETE FROM P where a=2");
        connection1.createStatement().executeUpdate("UPDATE P set a=300 where a=3");

        // Transaction 2: Gets a FK violation when I try to reference deleted parent.
        assertQueryFail(connection2, "insert into C values(2,2)", "Operation on table 'C' caused a violation of foreign key constraint 'FK1' for key (A).  The statement has been rolled back.");
        assertQueryFail(connection2, "insert into C values(3,3)", "Operation on table 'C' caused a violation of foreign key constraint 'FK1' for key (A).  The statement has been rolled back.");

        connection1.commit();
        connection2.commit();

        // After concurrent transaction commit verify row count seen by third transaction.
        assertEquals(0L, methodWatcher.query("select count(*) from P where a=2"));
        assertEquals(0L, methodWatcher.query("select count(*) from P where a=3"));
        assertEquals(1L, methodWatcher.query("select count(*) from P where a=300"));
        assertEquals(0L, methodWatcher.query("select count(*) from C where a=2"));
    }

    @Ignore("for manual testing")
    @Test
    public void multipleLargeThreadCount() throws Exception {
        for (int i = 0; i < 200; i++) {
            System.out.println("i=" + i);
            largerNumberOfConcurrentThreads();
        }
    }

    /**
     * Among other things this verifies:
     *
     * <ul>
     * <li>Multiple child threads inserting references to the same parent do NOT conflict with each other</li>
     * </ul>
     */
    @Test(timeout =10000)
    public void largerNumberOfConcurrentThreads() throws Exception {
        final int THREADS = 8;
        final int CHILD_ROWS_PER_THREAD = 5;
        final CountDownLatch countDownLatch = new CountDownLatch(THREADS + 1);

        // get a new parent id
        long parentId = RandomUtils.nextLong();

        // create the initial table state and commit
        {
            Connection conn = newNoAutoCommitConnection();
            // insert parent id
            conn.createStatement().executeUpdate("insert into P values(" + parentId + "," + parentId + ")");
            // commit
            conn.commit();
        }

        // create and start threads
        List<T> threads = Lists.newArrayList();
        for (int i = 0; i < THREADS; i++) {
            T t = new T(newNoAutoCommitConnection(), countDownLatch, parentId, CHILD_ROWS_PER_THREAD);
            t.start();
            threads.add(t);
        }

        // release the hounds
        countDownLatch.countDown();

        // Somewhere in the middle of work of threads, delete reference to parent.  Sometimes this will happen
        // before any child row is inserted, in which case it will succeed.  Sometimes it will happen after
        // at least one child row has been inserted in which case it will fail.
        Connection deleteParentConn = newNoAutoCommitConnection();
        try {
            deleteParentConn.createStatement().execute("delete from p where a=" + parentId);
        } catch (SQLException e) {
            if (!e.getMessage().startsWith("Write Conflict") && !e.getMessage().startsWith("Operation on table 'P' caused a violation of foreign key constraint 'FK1'")) {
                fail("We expect Write Conflict or FK violation here sometimes, but not anything else: " + e.getMessage());
            }
        }

        // Wait for threads to finish
        for (T t : threads) {
            t.join(30_000);
            assertNull(t.throwable);
        }

        deleteParentConn.commit();

        // finally verify DB is in a consistent state.

        // to be sure we are using a connection/transaction that starts after commit above.
        methodWatcher.closeAll();

        long parentCount = methodWatcher.query("select count(*) from P where a=" + parentId);
        long childCount = methodWatcher.query("select count(*) from C where a=" + parentId);
        System.out.printf("parentCount=%,d, childCount=%,d \n", parentCount, childCount);
        assertTrue(
                String.format("parentCount=%,d, childCount=%,d", parentCount, childCount),
                (parentCount == 0 && childCount == 0) ||
                        (parentCount == 1 && childCount == THREADS * CHILD_ROWS_PER_THREAD)
        );
    }

    private static class T extends Thread {

        CountDownLatch countDownLatch;
        Connection connection;
        long parentId;
        int myId;
        int childRowsPerThread;
        Throwable throwable;

        T(Connection connection, CountDownLatch countDownLatch, long parentId, int childRowsPerThread) {
            this.connection = connection;
            this.countDownLatch = countDownLatch;
            this.parentId = parentId;
            this.myId = System.identityHashCode(this);
            this.childRowsPerThread = childRowsPerThread;
        }

        @Override
        public void run() {
            try {
                countDownLatch.countDown();
                countDownLatch.await();
                Threads.sleep(RandomUtils.nextInt(100), TimeUnit.MILLISECONDS);
                PreparedStatement preparedStatement = connection.prepareStatement("insert into C values(?,?)");
                for (int i = 1; i <= childRowsPerThread; i++) {
                    preparedStatement.setLong(1, parentId);
                    preparedStatement.setLong(2, myId);
                    preparedStatement.execute();
                }
            } catch (Throwable e) {
                if (!e.getMessage().startsWith("Operation on table 'C' caused a violation of foreign key constraint 'FK1'")) {
                    this.throwable = e;
                }
            } finally {
                try {
                    connection.commit();
                } catch (SQLException e) {
                    throwable = e;
                }
            }
        }

    }

    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    //
    // helper methods
    //
    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    private Connection newNoAutoCommitConnection() throws Exception {
        Connection connection = methodWatcher.createConnection();
        connection.setAutoCommit(false);
        return connection;
    }

    private void assertQueryFail(Connection connection, String sql, String expectedExceptionMessage) {
        try {
            connection.createStatement().executeUpdate(sql);
            fail("query did not fail: " + sql);
        } catch (Exception e) {
            assertTrue("expected message=" + expectedExceptionMessage + "actual message=" + e.getMessage(),
                    e.getMessage().startsWith(expectedExceptionMessage));
        }
    }

}