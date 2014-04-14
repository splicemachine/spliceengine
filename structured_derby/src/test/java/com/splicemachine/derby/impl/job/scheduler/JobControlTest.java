package com.splicemachine.derby.impl.job.scheduler;

import com.google.common.collect.Maps;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.impl.job.coprocessor.CoprocessorJob;
import com.splicemachine.derby.impl.job.coprocessor.RegionTask;
import com.splicemachine.derby.impl.job.coprocessor.SpliceSchedulerProtocol;
import com.splicemachine.derby.impl.job.coprocessor.TaskFutureContext;
import com.splicemachine.encoding.Encoding;
import com.splicemachine.hbase.table.BoundCall;
import com.splicemachine.job.Status;
import com.splicemachine.job.TaskStatus;
import com.splicemachine.utils.SpliceZooKeeperManager;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.zookeeper.RecoverableZooKeeper;
import org.apache.log4j.Logger;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.*;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

/**
 * @author Scott Fines
 * Created on: 11/11/13
 */
public class JobControlTest {

    private static final Logger LOG = Logger.getLogger(JobControlTest.class);

//    @Test(timeout = 10000)
    @Test
    public void testCompleteAllWaitsForAllToFinishSameStartAndEndKey() throws Throwable {
        HTableInterface mockTable = mock(HTableInterface.class);

        CoprocessorJob mockJob = mock(CoprocessorJob.class);
        when(mockJob.getTable()).thenReturn(mockTable);

        SpliceZooKeeperManager mockZkManager = mock(SpliceZooKeeperManager.class);
        final RecoverableZooKeeper fakeZk = mock(RecoverableZooKeeper.class);
        when(mockZkManager.getRecoverableZooKeeper()).thenReturn(fakeZk);
        doCallRealMethod().when(mockZkManager).executeUnlessExpired(any(SpliceZooKeeperManager.Command.class));
        ZooKeeper zk = mock(ZooKeeper.class);
        when(zk.getSessionTimeout()).thenReturn(10000);
        when(fakeZk.getZooKeeper()).thenReturn(zk);

        JobMetrics throwaway = mock(JobMetrics.class);

        SpliceDriver.getKryoPool(); //initialize stuff before it's needed so as not to interfere with the test
        final JobControl control = new JobControl(mockJob,"/test",mockZkManager,1,throwaway);

        //submit two tasks
        final Map<String,Watcher> watcherMap = Maps.newHashMap();

        final String taskNode1 = "/test-1";
        byte[] startKey = HConstants.EMPTY_START_ROW;
        byte[] endKey = HConstants.EMPTY_END_ROW;
        RegionTask task1= getNewTask(mockTable, fakeZk, watcherMap, taskNode1, startKey, endKey);
        //"submit" the process
        control.submit(task1, Pair.newPair(startKey, endKey),mockTable,0);


        final String taskNode2 = "/test-2";
//        startKey = Encoding.encode(1);
//        endKey= HConstants.EMPTY_END_ROW;
        RegionTask task2 = getNewTask(mockTable,fakeZk,watcherMap,taskNode2,startKey,endKey);
        control.submit(task2, Pair.newPair(startKey,endKey),mockTable,0);

        ExecutorService controlExecutor = Executors.newSingleThreadExecutor();
        Future<Void> future = controlExecutor.submit(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                control.completeAll(null);
                return null;
            }
        });

//        future.get();
        //wait a little bit to make sure that the jobControl hasn't terminated already
        try{
            future.get(100,TimeUnit.MILLISECONDS);
            Assert.fail("JobControl returned early!");
        }catch(TimeoutException te){
            //correct behavior
        }
        if(future.isDone()){
            future.get();
            Assert.fail("Future returned early!");
        }

        //complete a task
        task1.markCompleted();

        try{
            future.get(100,TimeUnit.MILLISECONDS);
            Assert.fail("JobControl returned early!");
        }catch(TimeoutException te){
            //correct behavior
        }
        if(future.isDone()){
            future.get();
            Assert.fail("Future returned early!");
        }

        //complete the next task
        task2.markCompleted();

        //this should return pretty quickly--rely on the @Test timeout to determine if things are stuck
        future.get();
    }

    @Test(timeout = 10000)
    public void testCompleteAllWaitsForAllToFinish() throws Throwable {
        HTableInterface mockTable = mock(HTableInterface.class);

        CoprocessorJob mockJob = mock(CoprocessorJob.class);
        when(mockJob.getTable()).thenReturn(mockTable);

        SpliceZooKeeperManager mockZkManager = mock(SpliceZooKeeperManager.class);
        final RecoverableZooKeeper fakeZk = mock(RecoverableZooKeeper.class);
        when(mockZkManager.getRecoverableZooKeeper()).thenReturn(fakeZk);
        doCallRealMethod().when(mockZkManager).executeUnlessExpired(any(SpliceZooKeeperManager.Command.class));
        ZooKeeper zk = mock(ZooKeeper.class);
        when(zk.getSessionTimeout()).thenReturn(10000);
        when(fakeZk.getZooKeeper()).thenReturn(zk);

        JobMetrics throwaway = mock(JobMetrics.class);

        SpliceDriver.getKryoPool(); //initialize stuff before it's needed so as not to interfere with the test
        final JobControl control = new JobControl(mockJob,"/test",mockZkManager,1,throwaway);

        //submit two tasks
        final Map<String,Watcher> watcherMap = Maps.newHashMap();

        final String taskNode1 = "/test-1";
        byte[] startKey = HConstants.EMPTY_START_ROW;
        byte[] endKey = Encoding.encode(1);
        RegionTask task1= getNewTask(mockTable, fakeZk, watcherMap, taskNode1, startKey, endKey);
        //"submit" the process
        control.submit(task1, Pair.newPair(startKey, endKey),mockTable,0);


        final String taskNode2 = "/test-2";
        startKey = Encoding.encode(1);
        endKey= HConstants.EMPTY_END_ROW;
        RegionTask task2 = getNewTask(mockTable,fakeZk,watcherMap,taskNode2,startKey,endKey);
        control.submit(task2, Pair.newPair(startKey,endKey),mockTable,0);

        ExecutorService controlExecutor = Executors.newSingleThreadExecutor();
        Future<Void> future = controlExecutor.submit(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                control.completeAll(null);
                return null;
            }
        });

//        future.get();
        //wait a little bit to make sure that the jobControl hasn't terminated already
        try{
            future.get(100,TimeUnit.MILLISECONDS);
            Assert.fail("JobControl returned early!");
        }catch(TimeoutException te){
            //correct behavior
        }
        if(future.isDone()){
            future.get();
            Assert.fail("Future returned early!");
        }

        //complete a task
        task1.markCompleted();

        try{
            future.get(100,TimeUnit.MILLISECONDS);
            Assert.fail("JobControl returned early!");
        }catch(TimeoutException te){
            //correct behavior
        }
        if(future.isDone()){
            future.get();
            Assert.fail("Future returned early!");
        }

        //complete the next task
        task2.markCompleted();

        //this should return pretty quickly--rely on the @Test timeout to determine if things are stuck
        future.get();
    }

    @Test
    public void testRegionTaskControlSet() throws Exception {
        NavigableSet<RegionTaskControl> tasks = new ConcurrentSkipListSet<RegionTaskControl>();

        byte[] startRow = HConstants.EMPTY_START_ROW;
        int i=0;
        JobControl control =mock(JobControl.class);
        SpliceZooKeeperManager zkManager = mock(SpliceZooKeeperManager.class);
        Map<Integer,RegionTaskControl> actualMap = Maps.newHashMap();
        do{
            TaskFutureContext tfc = new TaskFutureContext("/test-"+i,Bytes.toBytes(i),0.0d);
            RegionTaskControl taskControl = new RegionTaskControl(startRow,mock(RegionTask.class),control,tfc,0,zkManager);
            actualMap.put(i,taskControl);
            tasks.add(taskControl);
            i++;
        }while(i<10);

        Assert.assertEquals("incorrect task size!",10,tasks.size());

        for(int pos=0;pos<10;pos++){
            RegionTaskControl taskControl = actualMap.get(pos);

            RegionTaskControl next = taskControl;
            do{
                next = tasks.higher(next);
            }while(next!=null && Bytes.compareTo(next.getStartRow(),taskControl.getStartRow())==0);

            tasks.remove(taskControl);

            byte[] start = Encoding.encode(pos);
            RegionTaskControl newTask = new RegionTaskControl(start,taskControl.getTask(),control,new TaskFutureContext("/test-"+pos+"-2",Bytes.toBytes(pos),0.0d),0,zkManager);
            tasks.add(newTask);
            Assert.assertEquals("Incorrect size!",10,tasks.size());
        }

    }

    private RegionTask getNewTask(HTableInterface mockTable, RecoverableZooKeeper fakeZk, final Map<String, Watcher> watcherMap, final String taskNode1, byte[] startKey, byte[] endKey) throws Throwable {
        final CountDownRegionTask task = new CountDownRegionTask(new CountDownLatch(1),1);
        when(fakeZk.getData(eq(taskNode1),any(Watcher.class),any(Stat.class))).thenAnswer(new Answer<byte[]>() {
            @Override
            public byte[] answer(InvocationOnMock invocation) throws Throwable {
                final Watcher w = (Watcher)invocation.getArguments()[1];
                final String node = (String)invocation.getArguments()[0];
                watcherMap.put(node, new Watcher() {

                    @Override
                    public void process(WatchedEvent event) {
                        watcherMap.remove(node);
                        w.process(event);
                    }
                });
                return task.getTaskStatus().toBytes();
            }
        });
        //noinspection unchecked
        doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                TaskFutureContext futureContext = new TaskFutureContext(taskNode1, Bytes.toBytes(1),0.0d);
                @SuppressWarnings("unchecked") Batch.Callback<TaskFutureContext> o = (Batch.Callback<TaskFutureContext>) invocation.getArguments()[4];
                byte[] startKey = (byte[])invocation.getArguments()[1];

                o.update(startKey,startKey,futureContext);
                return null;  //To change body of implemented methods use File | Settings | File Templates.
            }
        }).when(mockTable).coprocessorExec(
                eq(SpliceSchedulerProtocol.class),
                eq(startKey),
                eq(endKey),
                any(BoundCall.class), any(Batch.Callback.class));

        task.taskStatus.attachListener(new TaskStatus.StatusListener() {
            @Override
            public void statusChanged(Status oldStatus, Status newStatus, TaskStatus taskStatus) {
                Watcher watcher = watcherMap.get(taskNode1);
                if(watcher!=null){
                    WatchedEvent event = new WatchedEvent(Watcher.Event.EventType.NodeDataChanged,
                            Watcher.Event.KeeperState.SyncConnected, taskNode1);
                    watcher.process(event);
                }
            }
        });
        return task;
    }

    private static final Logger TASK_LOG = Logger.getLogger(CountDownRegionTask.class);
    private class CountDownRegionTask implements RegionTask{
        private final CountDownLatch latch;
        private volatile TaskStatus taskStatus;
        private final int pos;

        public CountDownRegionTask(CountDownLatch latch,int pos) {
            this.latch = latch;
            this.taskStatus = new TaskStatus(Status.PENDING,null);
            this.pos = pos;
        }

        @Override
        public void prepareTask(byte[] start, byte[] stop,RegionCoprocessorEnvironment rce, SpliceZooKeeperManager zooKeeper) throws ExecutionException {
            TASK_LOG.trace("Task preparing");
        }

        @Override
        public boolean invalidateOnClose() {
            return false;
        }

        @Override
        public String getTaskNode() {
            return "/test/taskNode";
        }

				@Override
				public RegionTask getClone() {
						return this;
				}

				@Override
				public boolean isSplittable() {
						return false;
				}

				@Override
        public void markStarted() throws ExecutionException, CancellationException {
            TASK_LOG.debug("Task started");
            taskStatus.setStatus(Status.EXECUTING);
        }

        @Override
        public void markCompleted() throws ExecutionException {
            TASK_LOG.debug("Task completed successfully");
            taskStatus.setStatus(Status.COMPLETED);
        }

        @Override
        public void markFailed(Throwable error) throws ExecutionException {
            TASK_LOG.error("Task failed with error ", error);
            taskStatus.setError(error);
            taskStatus.setStatus(Status.FAILED);
        }

        @Override
        public void markCancelled() throws ExecutionException {
            TASK_LOG.info("Task was cancelled");
            taskStatus.setStatus(Status.CANCELLED);
        }

        @Override
        public void execute() throws ExecutionException, InterruptedException {
            markStarted();
            try{
                latch.await();
                markCompleted();
            }catch(InterruptedException ie){
                markFailed(ie);
                throw ie;
            }
        }

        @Override
        public boolean isCancelled() throws ExecutionException {
            return taskStatus.getStatus()==Status.CANCELLED;
        }

        @Override
        public byte[] getTaskId() {
            return new byte[]{};
        }

        @Override
        public TaskStatus getTaskStatus() {
            return taskStatus;
        }

        @Override
        public void markInvalid() throws ExecutionException {
            taskStatus.setStatus(Status.INVALID);
        }

        @Override
        public boolean isInvalidated() {
            return taskStatus.getStatus()==Status.INVALID;
        }

        @Override
        public void cleanup() throws ExecutionException {
            //no-op
        }

        @Override
        public int getPriority() {
            return 1;
        }

        @Override
        public boolean isTransactional() {
            return false;
        }

				@Override
				public byte[] getParentTaskId() {
						return null;
				}

				@Override
				public String getJobId() {
						return null;
				}
		}
}
