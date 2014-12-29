package com.splicemachine.derby.impl.job.scheduler;

import com.google.common.collect.Lists;
import com.splicemachine.job.Status;
import com.splicemachine.job.Task;
import com.splicemachine.job.TaskStatus;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;

public class WorkStealingTaskScheduler_WorkerTest {

    WorkStealingTaskScheduler<Task> scheduler = new WorkStealingTaskScheduler<>(1, 100L, null, Lists.<StealableTaskScheduler<Task>>newArrayList(), "name");

    @After
    public void after() {
        scheduler.shutdown();
    }

    @Test
    public void testTaskStatusSetToCancelledWhenExecutingThreadInterrupted() throws ExecutionException, InterruptedException {

        final CountDownLatch executeLatch = new CountDownLatch(1);
        final CountDownLatch cancelLatch = new CountDownLatch(1);
        final AtomicReference<Thread> executingThread = new AtomicReference<>();

        Task mockTask = mock(Task.class);
        when(mockTask.getTaskStatus()).thenReturn(new TaskStatus(Status.PENDING, null));
        when(mockTask.getTaskId()).thenReturn(new byte[]{1, 2, 3, 4, 5, 6, 7, 8});
        when(mockTask.getJobId()).thenReturn("jobId");
        doAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                executingThread.set(Thread.currentThread());
                executeLatch.countDown();
                Thread.sleep(20 * 1000);
                return null;
            }
        }).when(mockTask).execute();

        doAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                cancelLatch.countDown();
                return null;
            }
        }).when(mockTask).markCancelled(true);

        scheduler.submit(mockTask);

        assertTrue("Execute was not called", executeLatch.await(10, TimeUnit.SECONDS));

        /* Here we are simulating something external interrupting the thread, a case distinct from our own framework
        calling task.markCancelled() which also interrupts the executing thread. */
        executingThread.get().interrupt();

        assertTrue("Cancel was not called", cancelLatch.await(10, TimeUnit.SECONDS));
    }

}