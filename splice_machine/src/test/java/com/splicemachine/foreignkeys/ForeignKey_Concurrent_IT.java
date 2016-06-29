package com.splicemachine.foreignkeys;

import org.sparkproject.guava.collect.Lists;
import com.splicemachine.concurrent.Threads;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.test.SerialTest;
import org.junit.*;
import org.junit.experimental.categories.Category;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Random;
import java.util.concurrent.*;

import static org.junit.Assert.*;

/**
 * Foreign key tests for concurrent transactions deleting parent rows and inserting child rows.
 */
//@Category(SerialTest.class)
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
        classWatcher.executeUpdate("create table P (a bigint primary key, b bigint)");
        classWatcher.executeUpdate("insert into P values(1,1),(2,2),(3,3)");
        classWatcher.executeUpdate("create table C (a bigint, b bigint, CONSTRAINT fk1 FOREIGN KEY(a) REFERENCES P(a))");
    }

    @Test(timeout = 10000)
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

    @Test(timeout = 10000)
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
     * Multiple threads attempt to insert references to a row in the parent table while that row is concurrently deleted.
     * Verifies that either (1) parent is deleted and all children fail; or (2) parent cannot be deleted and all children
     * succeed, but never anything in between.
     */
    @Test(timeout = 10000)
    public void largerNumberOfConcurrentThreads() throws Exception {
        final int THREADS = 4;
        final int CHILD_ROWS_PER_THREAD = 5;
        final CountDownLatch countDownLatch = new CountDownLatch(1);

        // get a new parent id
        long parentId = new Random().nextLong();

        // create the initial table state and commit
        {
            Connection conn = newNoAutoCommitConnection();
            // insert parent id
            conn.createStatement().executeUpdate("insert into P values(" + parentId + "," + parentId + ")");
            // commit
            conn.commit();
        }

        // create and start threads
        ExecutorService executor = Executors.newFixedThreadPool(THREADS);
        try {
            List<Future<Void>> futures = Lists.newArrayList();
            for (int i = 0; i < THREADS; i++) {
                Callable<Void> t = new T(newNoAutoCommitConnection(), countDownLatch, parentId, CHILD_ROWS_PER_THREAD);
                futures.add(executor.submit(t));
            }

            // release the hounds
            countDownLatch.countDown();

            // Somewhere in the middle of work of threads, which insert references to parent, delete parent.
            //
            // CASE 1: The parent row is deleted before any children are inserted.  The parent row delete succeeds
            //         without exception and all child inserts fail (with FK violation). The final state is zero rows
            //         in parent and zero rows in child.
            //
            // CASE 2: At least one child row is inserted before the parent is deleted.  The parent delete fails with
            //         FK violation if if the child row is committed, WriteWrite if it is not committed (it happened
            //         after the parent delete transaction started). All child row inserts succeed.  The final
            //         state of the database should be; parentRows=1, childRows=THREADS*CHILD_ROWS_PER_THREAD
            Connection deleteParentConn = newNoAutoCommitConnection();
            try {
                Statement statement = deleteParentConn.createStatement();
                statement.setQueryTimeout(5);
                statement.execute("delete from p where a=" + parentId);
            } catch (SQLException e) {
                if (!e.getMessage().startsWith("Write Conflict") && !e.getMessage().startsWith("Operation on table 'P' caused a violation of foreign key constraint 'FK1'")) {
                    fail("We expect Write Conflict or FK violation here sometimes, but not anything else: " + e.getMessage());
                }
            }

            // Wait for threads to finish
            for (Future<Void> future : futures) {
                try {
                    future.get(); //make sure we don't throw any errors other than FK violation
                } catch (ExecutionException e) {
                    if (!e.getMessage().contains("Operation on table 'C' caused a violation of foreign key constraint 'FK1' for key (A).")) {
                        fail("unexpected failure in child threads: " + e.getMessage());
                    }
                }
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
        } finally {
            executor.shutdownNow();
        }
    }

    private static class T implements Callable<Void> {

        CountDownLatch countDownLatch;
        Connection connection;
        long parentId;
        int myId;
        int childRowsPerThread;

        T(Connection connection, CountDownLatch countDownLatch, long parentId, int childRowsPerThread) {
            this.connection = connection;
            this.countDownLatch = countDownLatch;
            this.parentId = parentId;
            this.myId = System.identityHashCode(this);
            this.childRowsPerThread = childRowsPerThread;
        }

        @Override
        public Void call() throws Exception {
            countDownLatch.await();
            Threads.sleep(new Random().nextInt(100), TimeUnit.MILLISECONDS);
            PreparedStatement preparedStatement = connection.prepareStatement("insert into C values(?,?)");
            preparedStatement.setQueryTimeout(5);
            for (int i = 1; i <= childRowsPerThread; i++) {
                if (Thread.currentThread().isInterrupted())
                    throw new InterruptedException(); //blow up if we've been cancelled
                preparedStatement.setLong(1, parentId);
                preparedStatement.setLong(2, myId);
                preparedStatement.execute();
            }
            connection.commit();
            return null;
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
            Statement statement = connection.createStatement();
            statement.setQueryTimeout(5);
            statement.executeUpdate(sql);
            fail("query did not fail: " + sql);
        } catch (Exception e) {
            assertTrue("expected message=" + expectedExceptionMessage + "actual message=" + e.getMessage(),
                    e.getMessage().startsWith(expectedExceptionMessage));
        }
    }

}