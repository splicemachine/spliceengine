package com.splicemachine.db.iapi.pyprocedure;

import com.splicemachine.db.impl.sql.pyprocedure.PyInterpreterPool;
import org.junit.*;
import org.python.util.PythonInterpreter;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class PyInterpreterPoolTest {
    static final int MAX_SIZE = PyInterpreterPool.POOL_MAX_SIZE;
    static final int TEST_ROUNDS = 30;
    BlockingQueue<PyInterpreterPool> pyPools;

    /*
     * Test the Singleton initialization is threadsafe
     */
    @Test
    public void testSingleton(){
        pyPools = new ArrayBlockingQueue<>(TEST_ROUNDS);
        PyInterpreterPool origPool = PyInterpreterPool.getInstance();
        Thread[] threadArr = new Thread[TEST_ROUNDS];
        for(int i = 0; i < TEST_ROUNDS; ++i){
            Thread testThread = new TestSingletonHelperThread();
            testThread.run();
            threadArr[i] = testThread;
        }
    }

    /*
     * Test the number of PythonInterpreter allocated is less than or equal to
     * PyInterpreterPool's max limit.
     */
    @Test
    public void testPyInterpreterPoolSize() throws Exception{
        PyInterpreterPool pool = PyInterpreterPool.getInstance();
        Assert.assertEquals(0, pool.size());
        Thread[] threadArr = new Thread[MAX_SIZE * 2];
        for(int i = 0; i < MAX_SIZE * 2; ++i){
            threadArr[i] = new AcquireReleaseHelperThread(pool);
            threadArr[i].run();
        }
        Assert.assertTrue(pool.size() <= MAX_SIZE);
        for(Thread thread : threadArr){
            thread.join();
        }
        Assert.assertEquals("Incorrect number of allocated PythonInterpreters",MAX_SIZE, pool.size());
        Assert.assertEquals("Incorrect queue size",MAX_SIZE, pool.currentQueueSize());
    }

    /* =======================================================================
     * Helper class and functions
     * ========================================================================*/
    class AcquireReleaseHelperThread extends Thread{
        PyInterpreterPool pool;

        AcquireReleaseHelperThread(PyInterpreterPool pool){
            this.pool = pool;
        }

        public void run(){
            try{
                PythonInterpreter inter = pool.acquire();
                Assert.assertTrue(inter != null);
                Thread.sleep(2000);
                pool.release(inter);
            } catch (Exception e){
                e.printStackTrace();
            }
        }
    }

    class TestSingletonHelperThread extends Thread{
        public void run(){
            pyPools.add(PyInterpreterPool.getInstance());
        }
    }
}
