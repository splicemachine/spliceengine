package com.splicemachine.derby.impl.job.scheduler;

import com.splicemachine.job.TaskStatus;
import com.splicemachine.job.Status;
import com.splicemachine.job.Task;
import com.splicemachine.job.TaskFuture;
import com.splicemachine.utils.SpliceLogUtils;
import junit.framework.Assert;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Scott Fines
 * Created on: 4/3/13
 */
public class ThreadedTaskSchedulerTest {
    private static final Logger LOG = Logger.getLogger(ThreadedTaskSchedulerTest.class);
    private ThreadedTaskScheduler scheduler = ThreadedTaskScheduler.create(4);

    @Before
    public void setUpTest() throws Exception{
        scheduler = ThreadedTaskScheduler.create(1);
        scheduler.start();
    }

    @After
    public void tearDownTest() throws Exception{
        scheduler.shutdown();
    }

    @Test(timeout = 1000l)
    public void testCanSubmitAndExecuteTask() throws Exception{
        CountDownTask task = new CountDownTask("test");
        SpliceLogUtils.trace(LOG,"Submitting test task");
        TaskFuture future = scheduler.submit(task);

        //ensure that the future returns without blocking or exploding
        future.complete();

        SpliceLogUtils.trace(LOG,"Future returned correctly");

        //ensure that the state transitions were correct
        Assert.assertEquals("Task never entered the Executing State!",1,task.started.get());
        Assert.assertEquals("Task never entered the completed state!", 1, task.completed.get());
        Assert.assertEquals("Task entered the failed state!", 0, task.failed.get());
        Assert.assertEquals("Task entered the cancelled state!", 0, task.cancelled.get());
        Assert.assertNull("Task has an Error!",task.getTaskStatus().getError());

        SpliceLogUtils.trace(LOG,"State Transitions were correct!");

        //ensure that the final state is complete
        Assert.assertEquals("Incorrect final state on future!", Status.COMPLETED,future.getStatus());
        SpliceLogUtils.trace(LOG,"final future state is correct");
    }

    @Test(timeout = 1000l)
    public void testFailuresPropagateCorrectly() throws Exception{
        FailTask task = new FailTask("testFailure");
        SpliceLogUtils.trace(LOG,"Submitting failure task");

        TaskFuture future = scheduler.submit(task);

        try{
            future.complete();
        }catch(ExecutionException ee){
            Throwable root = ee.getCause();
            Assert.assertTrue("Incorrect exception type returned",root instanceof IOException);
            IOException ioe = (IOException)root;
            Assert.assertEquals("Incorrect error message returned!",task.getTaskId(),ioe.getMessage());
        }

        //ensure that the state transitions were correct
        Assert.assertEquals("Task never entered the Executing State!",1,task.started.get());
        Assert.assertEquals("Task never entered the completed state!", 0, task.completed.get());
        Assert.assertEquals("Task entered the failed state!", 1, task.failed.get());
        Assert.assertEquals("Task entered the cancelled state!", 0, task.cancelled.get());
        Assert.assertNotNull("Task does not have an Error!", task.getTaskStatus().getError());

        SpliceLogUtils.trace(LOG,"State Transitions were correct!");

        //ensure that the final state is complete
        Assert.assertEquals("Incorrect final state on future!", Status.FAILED,future.getStatus());
        SpliceLogUtils.trace(LOG,"final future state is correct");
    }

    @Test(timeout=1000l)
    public void testCanCancelBeforeSubmittingThrowsCancellationException() throws Exception{
        WaitTask task = new WaitTask("testFailure",new CountDownLatch(1));
        task.markCancelled();
        Assert.assertTrue("Test Error: markCancelled did not set cancelled state correctly!", task.isCancelled());
        SpliceLogUtils.trace(LOG,"Submitting cancelled task");

        TaskFuture future = scheduler.submit(task);

        try{
            future.complete();
            Assert.fail("Did not receive a cancellation notification!");
        }catch(CancellationException ce){

        }

        //ensure that the state transitions were correct
        Assert.assertEquals("Task never entered the Executing State!",0,task.started.get());
        Assert.assertEquals("Task never entered the completed state!", 0, task.completed.get());
        Assert.assertEquals("Task entered the failed state!", 0, task.failed.get());
        Assert.assertEquals("Task entered the cancelled state!", 1, task.cancelled.get());
        Assert.assertNull("Task has an Error!", task.getTaskStatus().getError());

        SpliceLogUtils.trace(LOG,"State Transitions were correct!");

        //ensure that the final state is complete
        Assert.assertEquals("Incorrect final state on future!", Status.CANCELLED,future.getStatus());
        SpliceLogUtils.trace(LOG,"final future state is correct");
    }

    @Test(timeout=1000l)
    public void testCanCancelAfterSubmitting() throws Exception{
        CountDownLatch latch = new CountDownLatch(1);
        WaitTask task = new WaitTask("testFailure",latch);
        SpliceLogUtils.trace(LOG,"Submitting cancelled task");

        TaskFuture future = scheduler.submit(task);
        latch.await(); //make sure that the task is executing before cancelling
        Thread.sleep(100);
        SpliceLogUtils.trace(LOG,"Cancelling task");
        future.cancel();
        Assert.assertTrue("Test Error: markCancelled did not set cancelled state correctly!",task.isCancelled());

        try{
            future.complete();
            Assert.fail("Did not receive a cancellation notification!");
        }catch(CancellationException ce){

        }

        //ensure that the state transitions were correct
        Assert.assertEquals("Task never entered the Executing State!",1,task.started.get());
        Assert.assertEquals("Task never entered the completed state!", 0, task.completed.get());
        Assert.assertEquals("Task entered the failed state!", 0, task.failed.get());
        Assert.assertEquals("Task entered the cancelled state!", 1, task.cancelled.get());
        Assert.assertNull("Task has an Error!",task.getTaskStatus().getError());

        SpliceLogUtils.trace(LOG,"State Transitions were correct!");

        //ensure that the final state is complete
        Assert.assertEquals("Incorrect final state on future!", Status.CANCELLED,future.getStatus());
        SpliceLogUtils.trace(LOG,"final future state is correct");
    }

    private static class CountDownTask implements Task {
        AtomicInteger started = new AtomicInteger(0);
        AtomicInteger completed = new AtomicInteger(0);
        AtomicInteger cancelled = new AtomicInteger(0);
        AtomicInteger failed = new AtomicInteger(0);
        AtomicInteger executed = new AtomicInteger(0);
        private final String taskString;
        private final TaskStatus taskStatus = new TaskStatus(Status.PENDING,null);
        private AtomicInteger invalid = new AtomicInteger(0);

        private CountDownTask(String taskString) {
            this.taskString = taskString;
        }

        @Override
        public void markStarted() throws ExecutionException, CancellationException {
            this.started.incrementAndGet();
            taskStatus.setStatus(Status.EXECUTING);
        }

        @Override
        public void markCompleted() throws ExecutionException {
            completed.incrementAndGet();
            taskStatus.setStatus(Status.COMPLETED);
        }

        @Override
        public void markFailed(Throwable error) throws ExecutionException {
            failed.incrementAndGet();
            taskStatus.setError(error);
            taskStatus.setStatus(Status.FAILED);
        }

        @Override
        public void markCancelled() throws ExecutionException {
            cancelled.incrementAndGet();
            taskStatus.setStatus(Status.CANCELLED);
        }

        @Override
        public void execute() throws ExecutionException, InterruptedException {
            executed.incrementAndGet();
        }

        @Override
        public boolean isCancelled() throws ExecutionException {
            return cancelled.get()>0;
        }

        @Override
        public String getTaskId() {
            return taskString;
        }

        @Override
        public TaskStatus getTaskStatus() {
            return taskStatus;
        }

        @Override
        public void markInvalid() {
            invalid.incrementAndGet();
        }
    }

    private class FailTask extends CountDownTask{

        private FailTask(String taskString) {
            super(taskString);
        }

        @Override
        public void execute() throws ExecutionException, InterruptedException {
            throw new ExecutionException(new IOException(getTaskId()));
        }
    }

    private static class WaitTask extends CountDownTask{
        private final CountDownLatch latch;
        private WaitTask(String taskString,CountDownLatch latch) {
            super(taskString);
            this.latch = latch;
        }

        @Override
        public void execute() throws ExecutionException, InterruptedException {
            latch.countDown();
            Thread.sleep(Long.MAX_VALUE);
        }
    }
}
