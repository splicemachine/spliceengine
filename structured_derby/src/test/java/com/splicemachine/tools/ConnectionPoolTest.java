package com.splicemachine.tools;

import junit.framework.Assert;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.util.concurrent.*;

import static org.mockito.Mockito.*;

/**
 * @author Scott Fines
 *         Created on: 3/22/13
 */
public class ConnectionPoolTest {

    private ExecutorService executor;

    @Before
    public void setupTest() throws Exception{
        executor = Executors.newFixedThreadPool(4);
    }

    @After
    public void tearDownTest() throws Exception{
        executor.shutdownNow();
    }

    @Test(timeout=1000)
    public void testCanAcquireSingleThread() throws Exception{
        Connection correctConnection = mock(Connection.class);

        ConnectionPool.Supplier mockSupplier = mock(ConnectionPool.Supplier.class);
        when(mockSupplier.createNew()).thenReturn(correctConnection);

        ConnectionPool pool = ConnectionPool.create(mockSupplier,1);

        Connection conn = pool.acquire();

        conn.close();

        //verify that the connection was returned to the pool and wasn't closed
        verify(correctConnection,never()).close();
    }

    @Test(timeout=1000)
    public void testTryAcquireWorks() throws Exception{
        Connection correctConnection = mock(Connection.class);

        ConnectionPool.Supplier mockSupplier = mock(ConnectionPool.Supplier.class);
        when(mockSupplier.createNew()).thenReturn(correctConnection);

        ConnectionPool pool = ConnectionPool.create(mockSupplier,1);

        Connection conn = pool.tryAcquire();

        Assert.assertNotNull("Connection was not returned!",conn);
        conn.close();

        //verify that the connection was returned to the pool and wasn't closed
        verify(correctConnection,never()).close();
    }

    @Test(timeout =1000)
    public void testShutdownWorks() throws Exception{
        Connection correctConnection = mock(Connection.class);

        ConnectionPool.Supplier mockSupplier = mock(ConnectionPool.Supplier.class);
        when(mockSupplier.createNew()).thenReturn(correctConnection);

        ConnectionPool pool = ConnectionPool.create(mockSupplier,1);

        Connection conn = pool.tryAcquire();

        Assert.assertNotNull("Connection was not returned!",conn);
        conn.close();

        pool.shutdown();
        //verify that the connection was only closed once
        verify(correctConnection,atMost(1)).close();
    }

    @Test(timeout= 1000)
    public void testBlockUntilReleased() throws Exception{
        Connection correctConnection = mock(Connection.class);

        ConnectionPool.Supplier mockSupplier = mock(ConnectionPool.Supplier.class);
        when(mockSupplier.createNew()).thenReturn(correctConnection);

        final ConnectionPool pool = ConnectionPool.create(mockSupplier,1);
        final CountDownLatch blockingLatch = new CountDownLatch(1);
        Future<Void> future = executor.submit(new Callable<Void>(){

            @Override
            public Void call() throws Exception {
                //block until the other thread tells you to proceed
                blockingLatch.await();
                Connection conn = pool.acquire();
                //we've acquired the connection!
                conn.close();
                return null;
            }
        });

        //acquire the connection
        Connection conn = pool.acquire();

        //tell the other thread to proceed
        blockingLatch.countDown();

        //wait some time
        Thread.sleep(200);

        //release the connection back to the pool
        conn.close();

        //wait for other thread to complete
        future.get();

        //verify the underlying connection was never closed
        verify(correctConnection,never()).close();
    }

    @Test(timeout= 1000)
    public void testTimedAcquireTimesOut() throws Exception{
        Connection correctConnection = mock(Connection.class);

        ConnectionPool.Supplier mockSupplier = mock(ConnectionPool.Supplier.class);
        when(mockSupplier.createNew()).thenReturn(correctConnection);

        final ConnectionPool pool = ConnectionPool.create(mockSupplier,1);
        final CountDownLatch blockingLatch = new CountDownLatch(1);
        Future<Boolean> future = executor.submit(new Callable<Boolean>(){

            @Override
            public Boolean call() throws Exception {
                //block until the other thread tells you to proceed
                blockingLatch.await();
                Connection conn = pool.tryAcquire(100,TimeUnit.MILLISECONDS);
                try{
                    return conn!=null;
                }finally{
                    if(conn!=null)conn.close();
                }
            }
        });

        //acquire the connection
        Connection conn = pool.acquire();

        //tell the other thread to proceed
        blockingLatch.countDown();

        //wait some time
        Thread.sleep(200);

        //release the connection back to the pool
        conn.close();

        //wait for other thread to complete
        Boolean acquired = future.get();
        Assert.assertTrue("Second thread successfully acquired connection!",!acquired);

        //verify the underlying connection was never closed
        verify(correctConnection,never()).close();
    }

    @Test(timeout= 1000)
    public void testTimedAcquireSucceeds() throws Exception{
        Connection correctConnection = mock(Connection.class);

        ConnectionPool.Supplier mockSupplier = mock(ConnectionPool.Supplier.class);
        when(mockSupplier.createNew()).thenReturn(correctConnection);

        final ConnectionPool pool = ConnectionPool.create(mockSupplier,1);
        final CountDownLatch blockingLatch = new CountDownLatch(1);
        Future<Boolean> future = executor.submit(new Callable<Boolean>(){

            @Override
            public Boolean call() throws Exception {
                //block until the other thread tells you to proceed
                blockingLatch.await();
                Connection conn = pool.tryAcquire(200,TimeUnit.MILLISECONDS);
                try{
                    return conn!=null;
                }finally{
                    if(conn!=null)conn.close();
                }
            }
        });

        //acquire the connection
        Connection conn = pool.acquire();

        //tell the other thread to proceed
        blockingLatch.countDown();

        //wait some time
        Thread.sleep(100);

        //release the connection back to the pool
        conn.close();

        //wait for other thread to complete
        Boolean acquired = future.get();
        Assert.assertTrue("Second thread never acquired connection!",acquired);

        //verify the underlying connection was never closed
        verify(correctConnection,never()).close();
    }

    @Test(timeout=1000)
    public void testTryAcquireFailsIfBlocked() throws Exception{
        Connection correctConnection = mock(Connection.class);

        ConnectionPool.Supplier mockSupplier = mock(ConnectionPool.Supplier.class);
        when(mockSupplier.createNew()).thenReturn(correctConnection);

        final ConnectionPool pool = ConnectionPool.create(mockSupplier,1);
        final CountDownLatch blockingLatch = new CountDownLatch(1);
        Future<Boolean> failAcquire = executor.submit(new Callable<Boolean>(){

            @Override
            public Boolean call() throws Exception {
                //block until the other thread tells you to proceed
                blockingLatch.await();
                Connection conn = pool.tryAcquire();
                return conn!=null;
            }
        });

        final CountDownLatch successLatch = new CountDownLatch(1);
        Future<Boolean> successAcquire = executor.submit(new Callable<Boolean>(){

            @Override
            public Boolean call() throws Exception {
                //block until the other thread tells you to proceed
                successLatch.await();
                Connection conn = pool.tryAcquire();
                return conn!=null;
            }
        });

        //acquire the connection
        Connection conn = pool.acquire();

        //tell the other thread to proceed
        blockingLatch.countDown();

        //wait for other thread to complete
        Boolean acquired = failAcquire.get();
        Assert.assertTrue("Second thread acquired Connection!",!acquired);

        //close the connection
        conn.close();

        //now tell the other thread to try
        successLatch.countDown();

        acquired = successAcquire.get();
        Assert.assertTrue("Second thread unable to acquire connection!",acquired);

        //verify the underlying connection was never closed
        verify(correctConnection,never()).close();
    }


}
