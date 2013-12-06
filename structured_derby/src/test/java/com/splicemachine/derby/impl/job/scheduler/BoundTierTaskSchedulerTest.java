package com.splicemachine.derby.impl.job.scheduler;

import com.splicemachine.job.Status;
import com.splicemachine.job.Task;
import com.splicemachine.job.TaskStatus;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.Mockito.*;

/**
 * @author Scott Fines
 * Date: 12/4/13
 */
public class BoundTierTaskSchedulerTest {

		@Test(timeout= 1000)
		public void testLowerPriorityTierStealsFromHigher() throws Exception {
				ConstrainedTaskScheduler<Task> cScheduler = new ConstrainedTaskScheduler<Task>(new ExpandingTaskScheduler<Task>(),
								Collections.<ConstrainedTaskScheduler.Constraint<Task>>emptyList(),true);

				final AtomicBoolean overflowCheck = new AtomicBoolean(false);
				BoundTierTaskScheduler.OverflowHandler overflowHandler = new BoundTierTaskScheduler.OverflowHandler() {
						@Override
						public OverflowPolicy shouldOverflow(Task t) {
								Assert.assertTrue("Overflow was called twice!",overflowCheck.compareAndSet(false,true));
								return OverflowPolicy.ENQUEUE;
						}
				};
				final BoundTierTaskScheduler<Task> taskScheduler = new BoundTierTaskScheduler<Task>(
								new PresetTieredTaskSchedulerSetup(new int[]{0,1},new int[]{1,1}), overflowHandler,cScheduler);
				try{

						final CountDownLatch latch = new CountDownLatch(3);
						final CyclicBarrier startLatch = new CyclicBarrier(2);

						TaskStatus taskStatus1 = createTaskStatus();

						final byte[] testTaskId = Bytes.toBytes(1l);
						final Task testTask1 = mock(Task.class);
						when(testTask1.getPriority()).thenReturn(0);
						when(testTask1.getTaskStatus()).thenReturn(taskStatus1);
						doAnswer(new Answer<Void>() {

								@Override
								public Void answer(InvocationOnMock invocationOnMock) throws Throwable {
										startLatch.await();
										Logger.getRootLogger().trace("Executing task");

										latch.countDown();
										return null;
								}
						}).when(testTask1).execute();

						TaskStatus taskStatus2 = createTaskStatus();

						Task testTask2 = mock(Task.class);
						when(testTask2.getTaskId()).thenReturn(testTaskId);
						when(testTask2.getTaskStatus()).thenReturn(taskStatus2);
						when(testTask2.getPriority()).thenReturn(0);

						doAnswer(new Answer<Void>() {
								@Override
								public Void answer(InvocationOnMock invocationOnMock) throws Throwable {
										startLatch.await();

										latch.countDown();
										return null;
								}
						}).when(testTask2).execute();

						TaskStatus taskStatus3 = createTaskStatus();
						Task testTask3 = mock(Task.class);
						when(testTask3.getTaskId()).thenReturn(testTaskId);
						when(testTask3.getTaskStatus()).thenReturn(taskStatus3);
						when(testTask3.getPriority()).thenReturn(1);

						final CountDownLatch stealLatch = new CountDownLatch(1);
						doAnswer(new Answer<Void>() {
								@Override
								public Void answer(InvocationOnMock invocationOnMock) throws Throwable {
										stealLatch.await();

										latch.countDown();
										return null;
								}
						}).when(testTask3).execute();


						taskScheduler.submit(testTask2);
						taskScheduler.submit(testTask1);

						taskScheduler.submit(testTask3);

						//tell the lower-priority task that he can complete, which will allow
						//the higher priority tasks to complete also
						stealLatch.countDown();

						latch.await();

						Assert.assertTrue("Overflow was never called!",overflowCheck.get());

				}finally{
						taskScheduler.shutdown();
				}
		}

		@Test(timeout= 1000)
		public void testHigherPriorityTasksShrugToLowerPriorityTiers() throws Exception {
				ConstrainedTaskScheduler<Task> cScheduler = new ConstrainedTaskScheduler<Task>(new ExpandingTaskScheduler<Task>(),
								Collections.<ConstrainedTaskScheduler.Constraint<Task>>emptyList(),true);

				BoundTierTaskScheduler.OverflowHandler overflowHandler = new BoundTierTaskScheduler.OverflowHandler() {
						@Override
						public OverflowPolicy shouldOverflow(Task t) {
								Assert.fail("Overflow should not be called");
								return OverflowPolicy.REJECT;
						}
				};
				final BoundTierTaskScheduler<Task> taskScheduler = new BoundTierTaskScheduler<Task>(
								new PresetTieredTaskSchedulerSetup(new int[]{0,1},new int[]{1,1}), overflowHandler,cScheduler);
				try{

						final CountDownLatch latch = new CountDownLatch(2);
						final CyclicBarrier startLatch = new CyclicBarrier(3);

						TaskStatus taskStatus1 = createTaskStatus();

						final byte[] testTaskId = Bytes.toBytes(1l);
						final Task testTask1 = mock(Task.class);
						when(testTask1.getPriority()).thenReturn(0);
						when(testTask1.getTaskStatus()).thenReturn(taskStatus1);
						doAnswer(new Answer<Void>() {

								@Override
								public Void answer(InvocationOnMock invocationOnMock) throws Throwable {
										startLatch.await();
										Logger.getRootLogger().trace("Executing task");

										latch.countDown();
										return null;
								}
						}).when(testTask1).execute();

						TaskStatus taskStatus = createTaskStatus();

						Task testTask = mock(Task.class);
						when(testTask.getTaskId()).thenReturn(testTaskId);
						when(testTask.getTaskStatus()).thenReturn(taskStatus);
						when(testTask.getPriority()).thenReturn(0);

						doAnswer(new Answer<Void>() {
								@Override
								public Void answer(InvocationOnMock invocationOnMock) throws Throwable {
										startLatch.await();

										latch.countDown();
										return null;
								}
						}).when(testTask).execute();

						taskScheduler.submit(testTask);
						taskScheduler.submit(testTask1);

						//tell the first task that he can operate, now that both are implemented
						startLatch.await();

						latch.await();

				}finally{
						taskScheduler.shutdown();
				}
		}

		@Test(timeout= 1000)
		public void testMultipleTasksEnqueue() throws Exception {
				ConstrainedTaskScheduler<Task> cScheduler = new ConstrainedTaskScheduler<Task>(new ExpandingTaskScheduler<Task>(),
								Collections.<ConstrainedTaskScheduler.Constraint<Task>>emptyList(),true);
				final AtomicBoolean queued = new AtomicBoolean(false);
				BoundTierTaskScheduler.OverflowHandler overflowHandler = new BoundTierTaskScheduler.OverflowHandler() {
						@Override
						public OverflowPolicy shouldOverflow(Task t) {
								boolean set = queued.compareAndSet(false, true);
								Assert.assertTrue("overflow called multiple times!", set);
								return OverflowPolicy.ENQUEUE;
						}
				};
				final BoundTierTaskScheduler<Task> taskScheduler = new BoundTierTaskScheduler<Task>(
								new PresetTieredTaskSchedulerSetup(new int[]{1},new int[]{1}), overflowHandler,cScheduler);
				try{

						final CountDownLatch latch = new CountDownLatch(1);

						TaskStatus mockSubStatus = createTaskStatus();

						final byte[] testTaskId = Bytes.toBytes(1l);
						final Task testTask1 = mock(Task.class);
						when(testTask1.getPriority()).thenReturn(0);
						when(testTask1.getTaskStatus()).thenReturn(mockSubStatus);
						doAnswer(new Answer<Void>() {

								@Override
								public Void answer(InvocationOnMock invocationOnMock) throws Throwable {
										Logger.getRootLogger().trace("Executing task");

										latch.countDown();
										return null;
								}
						}).when(testTask1).execute();

						TaskStatus taskStatus = createTaskStatus();

						Task testTask = mock(Task.class);
						when(testTask.getTaskId()).thenReturn(testTaskId);
						when(testTask.getTaskStatus()).thenReturn(taskStatus);
						when(testTask.getPriority()).thenReturn(0);

						final CountDownLatch startLatch = new CountDownLatch(1);
						doAnswer(new Answer<Void>() {
								@Override
								public Void answer(InvocationOnMock invocationOnMock) throws Throwable {
										startLatch.await();

										latch.countDown();
										return null;
								}
						}).when(testTask).execute();

						taskScheduler.submit(testTask);
						taskScheduler.submit(testTask1);

						//tell the first task that he can operate, now that both are implemented
						startLatch.countDown();

						latch.await();

						Assert.assertTrue("Task was not queued!",queued.get());
				}finally{
						taskScheduler.shutdown();
				}
		}

		@Test(timeout= 1000)
		public void testSubtaskSubmissionOverflows() throws Exception {
				ConstrainedTaskScheduler<Task> cScheduler = new ConstrainedTaskScheduler<Task>(new ExpandingTaskScheduler<Task>(),
								Collections.<ConstrainedTaskScheduler.Constraint<Task>>emptyList(),true);
				BoundTierTaskScheduler.OverflowHandler overflowHandler = new BoundTierTaskScheduler.OverflowHandler() {
						@Override
						public OverflowPolicy shouldOverflow(Task t) {
								if (t.getParentTaskId() != null)
										return OverflowPolicy.OVERFLOW;
								else
										return OverflowPolicy.REJECT;
						}
				};
				final BoundTierTaskScheduler<Task> taskScheduler = new BoundTierTaskScheduler<Task>(
								new PresetTieredTaskSchedulerSetup(new int[]{1},new int[]{1}), overflowHandler,cScheduler);
				try{
						final CountDownLatch subLatch = new CountDownLatch(1);

						TaskStatus mockSubStatus = createTaskStatus();

						final byte[] testTaskId = Bytes.toBytes(1l);
						final Task testSubtask = mock(Task.class);
						when(testSubtask.getParentTaskId()).thenReturn(testTaskId);
						when(testSubtask.getPriority()).thenReturn(0);
						when(testSubtask.getTaskStatus()).thenReturn(mockSubStatus);
						doAnswer(new Answer<Void>() {

								@Override
								public Void answer(InvocationOnMock invocationOnMock) throws Throwable {
										Logger.getRootLogger().trace("Executing task");

										subLatch.countDown();
										return null;
								}
						}).when(testSubtask).execute();

						TaskStatus taskStatus = createTaskStatus();

						Task testTask = mock(Task.class);
						when(testTask.getTaskId()).thenReturn(testTaskId);
						when(testTask.getTaskStatus()).thenReturn(taskStatus);
						when(testTask.getPriority()).thenReturn(0);

						final CountDownLatch latch = new CountDownLatch(1);
						doAnswer(new Answer<Void>() {
								@Override
								public Void answer(InvocationOnMock invocationOnMock) throws Throwable {
										taskScheduler.submit(testSubtask);
										subLatch.await();

										latch.countDown();
										return null;
								}
						}).when(testTask).execute();


						taskScheduler.submit(testTask);

						latch.await();
				}finally{
						taskScheduler.shutdown();
				}
		}

		@Test(timeout= 1000)
//		@Test
		public void testSubmissionWorks() throws Exception {
				ConstrainedTaskScheduler<Task> cScheduler = new ConstrainedTaskScheduler<Task>(new ExpandingTaskScheduler<Task>(),
								Collections.<ConstrainedTaskScheduler.Constraint<Task>>emptyList(),true);
				BoundTierTaskScheduler.OverflowHandler overflowHandler = new BoundTierTaskScheduler.OverflowHandler() {
						@Override
						public OverflowPolicy shouldOverflow(Task t) {
								return OverflowPolicy.REJECT;
						}
				};
				final BoundTierTaskScheduler<Task> taskScheduler = new BoundTierTaskScheduler<Task>(
								new PresetTieredTaskSchedulerSetup(new int[]{1},new int[]{1}), overflowHandler,cScheduler);
				try{

						final CountDownLatch latch = new CountDownLatch(1);

						TaskStatus mockStatus = createTaskStatus();

						Task testTask = mock(Task.class);
						when(testTask.getTaskStatus()).thenReturn(mockStatus);
						doAnswer(new Answer<Void>() {

								@Override
								public Void answer(InvocationOnMock invocationOnMock) throws Throwable {
										Logger.getRootLogger().trace("Executing task");
										latch.countDown();
										return null;
								}
						}).when(testTask).execute();

						when(testTask.getPriority()).thenReturn(0);

						taskScheduler.submit(testTask);

						latch.await();
				}finally{
						taskScheduler.shutdown();
				}
		}

		private TaskStatus createTaskStatus() {
				TaskStatus mockStatus = mock(TaskStatus.class);
				doNothing().when(mockStatus).attachListener(any(TaskStatus.StatusListener.class));
				final AtomicReference<Status> status = new AtomicReference<Status>(Status.PENDING);
				when(mockStatus.getStatus()).thenReturn(status.get());
				doAnswer(new Answer<Void>() {
						@Override
						public Void answer(InvocationOnMock invocationOnMock) throws Throwable {
								status.set((Status)invocationOnMock.getArguments()[0]);
								return null;
						}
				}).when(mockStatus).setStatus(any(Status.class));
				return mockStatus;
		}


}

