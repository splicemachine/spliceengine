package com.splicemachine.derby.hbase;

import org.junit.Assert;
import org.junit.Test;


public class WriteSemaphoreTest {

    @Test
    public void testCanAcquireDependent() throws Exception {
        WriteSemaphore semaphore = new WriteSemaphore(10,10,10,10);
        WriteSemaphore.Status status = semaphore.acquireDependentPermit(1);
        Assert.assertEquals("Incorrect return status", WriteSemaphore.Status.DEPENDENT, status);
    }

    @Test
    public void testCannotAcquireDependentWhenFull() throws Exception {
        WriteSemaphore semaphore = new WriteSemaphore(1,10,10,10);
        WriteSemaphore.Status status = semaphore.acquireDependentPermit(1);
        Assert.assertEquals("Incorrect return status", WriteSemaphore.Status.DEPENDENT, status);

        status = semaphore.acquireDependentPermit(1);
        Assert.assertEquals("Incorrect return status",WriteSemaphore.Status.REJECTED,status);
    }

    @Test
    public void testCannotAcquireDependentWhenFullThreads() throws Exception {
        WriteSemaphore semaphore = new WriteSemaphore(2,1,2,2);
        WriteSemaphore.Status status = semaphore.acquireDependentPermit(1);
        Assert.assertEquals("Incorrect return status", WriteSemaphore.Status.DEPENDENT, status);

        status = semaphore.acquireDependentPermit(2);
        Assert.assertEquals("Incorrect return status",WriteSemaphore.Status.REJECTED,status);
    }

    @Test
    public void testCanAcquireIndependent() throws Exception {
        WriteSemaphore semaphore = new WriteSemaphore(10,10,10,10);
        WriteSemaphore.Status status = semaphore.acquireIndependentPermit(1);
        Assert.assertEquals("Incorrect return status", WriteSemaphore.Status.INDEPENDENT, status);
    }

    @Test
    public void testCanAcquireIndependentWithFullDependent() throws Exception {
        WriteSemaphore semaphore = new WriteSemaphore(0,10,10,10);
        WriteSemaphore.Status status = semaphore.acquireIndependentPermit(1);
        Assert.assertEquals("Incorrect return status", WriteSemaphore.Status.INDEPENDENT, status);
    }

    @Test
    public void testCannotAcquireIndependentWhenFull() throws Exception {
        WriteSemaphore semaphore = new WriteSemaphore(0,1,10,10);
        WriteSemaphore.Status status = semaphore.acquireIndependentPermit(1);
        Assert.assertEquals("Incorrect return status", WriteSemaphore.Status.INDEPENDENT, status);

        status = semaphore.acquireIndependentPermit(1);
        Assert.assertEquals("Incorrect return status",WriteSemaphore.Status.REJECTED,status);
    }

    @Test
    public void testIndependentDowngradesWhenNeeded() throws Exception {
        WriteSemaphore semaphore = new WriteSemaphore(1,1,10,10);
        WriteSemaphore.Status status = semaphore.acquireIndependentPermit(1);
        Assert.assertEquals("Incorrect return status", WriteSemaphore.Status.INDEPENDENT, status);

        status = semaphore.acquireIndependentPermit(1);
        Assert.assertEquals("Incorrect return status",WriteSemaphore.Status.DEPENDENT,status);
    }

}