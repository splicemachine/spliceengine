package com.splicemachine.db.impl.sql.pyprocedure;

import org.python.core.PyObject;
import org.python.util.PythonInterpreter;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.locks.ReentrantLock;

public class PyInterpreterPool {
    // The maximum pool size is hard coded as 2.
    // In the future may make it configurable
    public static int POOL_MAX_SIZE = 2;
    private static volatile PyInterpreterPool INSTANCE;
    private ArrayBlockingQueue<PythonInterpreter> pool;
    private ReentrantLock lock = new ReentrantLock();
    private int maxSize = 0;
    private int size;
    private boolean full = false;
    private static String NAME_VAR_STR = "__name__";
    private static String DOC_VAR_STR = "__doc__";

    private PyInterpreterPool(int maxSize) {
        if(INSTANCE != null){
            throw new RuntimeException();
        }
        this.pool = new ArrayBlockingQueue<>(maxSize, true);
        this.maxSize = maxSize;
    }

    /*
     * retrieve PythonInterperter from the resource pool.
     * If the number of interpreter allocated by the pool is less than
     * the maxSize, then the interpreter will create and return a new
     * interpreter.
     * If the the number of interpreter allocated by the pool has
     * reached the maxSize, the thread acquiring for the interpreter
     * needs to retrive from the Blocking Queue (pool) and waits if
     * it is necessary.
     */
    public PythonInterpreter acquire() throws Exception {
        PythonInterpreter result = null;

        // One thread might read stale value of full (race condition), but it is OK,
        // As full is only used to save lock operations
        if(!full){
            if(lock.tryLock()){
                try {
                    if(size < maxSize){
                        size++;
                        result = new PythonInterpreter();
                    }
                }
                finally{
                    if(size == maxSize){
                        full = true;
                    }
                    lock.unlock();
                }
            }
        }
        if(result == null){
            result = pool.take();
        }
        return result;
    }

    /*
     * Put back the interpreter back into the pool.
     * Always put back the interpreter (i.e. use finally block). Otherwise, it can cause dead lock as
     * the threads that are waiting to acquire an interpreter might wait forever.
     */
    public void release(PythonInterpreter interpreter){
        // Clean the interpreter by setting the user defined instances to null
        PyObject locals = interpreter.getLocals();
        List<String> interpreterVars = new ArrayList<String>();
        for (PyObject item : locals.__iter__().asIterable()) {
            interpreterVars.add(item.toString());
        }
        for (String varName : interpreterVars) {
            if(varName.equals(NAME_VAR_STR) || varName.equals(DOC_VAR_STR)) continue;
            interpreter.set(varName, null);
        }
        pool.add(interpreter);
    }

    public static PyInterpreterPool getInstance(){
        if(INSTANCE == null){
            synchronized (PyInterpreterPool.class){
                if(INSTANCE == null){
                    INSTANCE = new PyInterpreterPool(POOL_MAX_SIZE);
                }
            }
        }
        return INSTANCE;
    }

    /* This function is for test use
     * @NOTE: calling size() and acquire() on the PyInterpreterPool which has not allocated
     * any Interpreter before might cause dead lock */
    public int size(){
        int result;
        lock.lock();
        result = size;
        lock.unlock();
        return result;
    }

    /*
     * get the current size of the BlockingQueue (pool)
     */
    public int currentQueueSize(){
        return pool.size();
    }
}