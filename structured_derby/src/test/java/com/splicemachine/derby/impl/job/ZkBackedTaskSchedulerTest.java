package com.splicemachine.derby.impl.job;

import com.google.common.collect.Maps;
import com.splicemachine.derby.impl.job.coprocessor.TaskStatus;
import com.splicemachine.derby.impl.job.scheduler.ZkBackedTaskScheduler;
import com.splicemachine.job.Status;
import com.splicemachine.job.Task;
import com.splicemachine.job.TaskFuture;
import com.splicemachine.job.TaskScheduler;
import junit.framework.Assert;
import org.apache.hadoop.hbase.zookeeper.RecoverableZooKeeper;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.*;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;

/**
 * Mocked network-free test of ZkBackedTaskScheduler.
 *
 * Uses mocks to control dependency behavior; additional tests that run
 * directly against ZooKeeper are also needed.
 *
 * @author Scott Fines
 * Created on: 4/4/13
 */
public class ZkBackedTaskSchedulerTest {

    @Test
    public void testSubmissionWritesToZooKeeperProperly() throws Exception{
        RecoverableZooKeeper keeper = getMockZooKeeper();
        TaskScheduler delegateScheduler = getMockScheduler();
        ZkBackedTaskScheduler scheduler = new ZkBackedTaskScheduler(keeper,delegateScheduler);

        DurableCountingTask task = new DurableCountingTask("/test/queue/task-",keeper);
        TaskFuture future = scheduler.submit(task);

        //ensure that no errors happened during execution e.g by some ZooKeeper problem
        future.complete();

        //make sure that executed was called
        Assert.assertEquals("Task was not executed!",1,task.executedCount);

        //make sure that the task was written to the node /test/queue/task-<number>
        //and that status was written to the node /test/queue/task-<number>/status
        //and that task.getTaskId() returns the correct taskId
        //and that future.getTaskId() returns the correct taskId
        Assert.assertEquals("Incorrect task id returned!","/test/queue/task-1",task.getTaskId());
        Assert.assertEquals("Future has incorrect task id!",task.getTaskId(),future.getTaskId());

        TaskStatus status = getTaskStatus(keeper, task);

        Assert.assertEquals("Status information did not propagate!", Status.COMPLETED, status.getStatus());

        //make sure that the data was serialized properly for durability
        DurableCountingTask deSerializedTask = getSerializedTask(keeper, task);
        Assert.assertEquals("incorrect executed count!",0,deSerializedTask.executedCount);
    }

    private DurableCountingTask getSerializedTask(RecoverableZooKeeper keeper, DurableCountingTask task) throws Exception{
        ByteArrayInputStream bais = new ByteArrayInputStream(keeper.getData(task.getTaskId(),false,new Stat()));
        ObjectInput oi = new ObjectInputStream(bais);
        return (DurableCountingTask)oi.readObject();
    }

    private TaskStatus getTaskStatus(RecoverableZooKeeper keeper, DurableCountingTask task) throws Exception{
        //get the status for the node
        byte[] data = keeper.getData(task.getTaskId()+"/status",false,new Stat());
        ByteArrayInputStream bais = new ByteArrayInputStream(data);
        ObjectInput oi = new ObjectInputStream(bais);
        return (TaskStatus)oi.readObject();
    }

    @Test
    public void testFailedTaskPropagatesErrorMessage() throws Exception{
        RecoverableZooKeeper keeper = getMockZooKeeper();
        TaskScheduler delegate = getMockScheduler();
        ZkBackedTaskScheduler scheduler = new ZkBackedTaskScheduler(keeper,delegate);

        FailCountingTask task = new FailCountingTask("/test/queue/task-","testFailure",keeper);
        TaskFuture future = scheduler.submit(task);

        //check that future throws an exception
        try{
            future.complete();
            Assert.fail("Failed to throw an exception!");
        }catch (ExecutionException ee){
            //check that the error message is correct
            Throwable cause = ee.getCause();
            Assert.assertTrue("Incorrect error cause",cause instanceof IOException);
            Assert.assertEquals("Incorrect error message",task.failureMessage,cause.getMessage());
        }

        //check that the backed status is FAILED, and that the error propagated over correctly
        TaskStatus status = getTaskStatus(keeper,task);
        Assert.assertEquals("Status information did not propagate correctly!",Status.FAILED, status.getStatus());
        Throwable error = status.getError();
        Assert.assertNotNull("No Error was propagated!",error);

        Assert.assertTrue("Incorrect error type propagated",error instanceof IOException);
        Assert.assertEquals("Incorrect error message propagated",task.failureMessage,error.getMessage());
    }

    private TaskScheduler getMockScheduler() throws ExecutionException {
        TaskScheduler delegateScheduler = mock(TaskScheduler.class);
        when(delegateScheduler.submit((Task)any())).thenAnswer(new Answer<TaskFuture>() {
            @Override
            public TaskFuture answer(InvocationOnMock invocation) throws Throwable {
                Task task = (Task)invocation.getArguments()[0];
                //execute the task synchronously, and return a mock future that
                //returns correctly
                Status status = Status.PENDING;
                task.markStarted();
                status = Status.EXECUTING;
                Throwable error = null;
                try{
                    task.execute();
                    task.markCompleted();
                    status = Status.COMPLETED;
                }catch(ExecutionException ee){
                    error = ee.getCause();
                    task.markFailed(ee.getCause());
                    status = Status.FAILED;
                }

                TaskFuture future = mock(TaskFuture.class);
                when(future.getStatus()).thenReturn(status);
                when(future.getTaskId()).thenReturn(task.getTaskId());
                if(error!=null){
                    doThrow(new ExecutionException(error)).when(future).complete();
                }else{
                    doNothing().when(future).complete();
                }

                return future;
            }
        });
        return delegateScheduler;
    }

    private static class FailCountingTask extends DurableCountingTask {
        private String failureMessage;

        public FailCountingTask(){
            super();
        }

        private FailCountingTask(String failureMessage,String taskId, RecoverableZooKeeper zooKeeper) {
            super(taskId, zooKeeper);
            this.failureMessage = failureMessage;
        }

        @Override
        public void execute() throws ExecutionException, InterruptedException {
            throw new ExecutionException(new IOException(failureMessage));
        }
    }

    private static class DurableCountingTask extends ZooKeeperTask {
        private int executedCount = 0;

        public DurableCountingTask(){
            super();
        }

        private DurableCountingTask(String taskId, RecoverableZooKeeper zooKeeper) {
            super(taskId, zooKeeper);
        }

        @Override
        public void writeExternal(ObjectOutput out) throws IOException {
            out.writeInt(executedCount);
        }

        @Override
        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            executedCount = in.readInt();
        }

        @Override
        public void execute() throws ExecutionException, InterruptedException {
            executedCount++;
        }
    }
    private RecoverableZooKeeper getMockZooKeeper() throws KeeperException, InterruptedException {
        final ConcurrentMap<String,byte[]> dataMap = Maps.newConcurrentMap();
        final AtomicInteger currentNodePosition = new AtomicInteger(0);
        RecoverableZooKeeper keeper = mock(RecoverableZooKeeper.class);
        when(keeper.create(anyString(),
                (byte[]) anyObject(),
                (List) anyObject(), eq(CreateMode.PERSISTENT_SEQUENTIAL)))
                .thenAnswer(new Answer<String>() {
                    @Override
                    public String answer(InvocationOnMock invocation) throws Throwable {
                        String nodeToCreate = (String) invocation.getArguments()[0];
                        String createdNode = nodeToCreate + (currentNodePosition.incrementAndGet());

                        byte[] data = (byte[]) invocation.getArguments()[1];
                        if (dataMap.putIfAbsent(createdNode, data) != null) {
                            throw KeeperException.create(KeeperException.Code.NODEEXISTS);
                        }
                        return createdNode;
                    }
                });

        when(keeper.create(anyString(),
                (byte[])any(),
                (List)any(),eq(CreateMode.PERSISTENT))).thenAnswer(new Answer<String>() {
            @Override
            public String answer(InvocationOnMock invocation) throws Throwable {
                String nodeToCreate = (String) invocation.getArguments()[0];
                if (dataMap.putIfAbsent(nodeToCreate, (byte[]) invocation.getArguments()[1]) != null) {
                    throw KeeperException.create(KeeperException.Code.NODEEXISTS);
                }
                return nodeToCreate;
            }
        });

        when(keeper.setData(anyString(),(byte[])any(),anyInt())).thenAnswer(new Answer<Stat>() {
            @Override
            public Stat answer(InvocationOnMock invocation) throws Throwable {
                String node = (String) invocation.getArguments()[0];
                byte[] old = dataMap.get(node);
                if(old==null)
                    throw KeeperException.create(KeeperException.Code.NONODE);
                if(!dataMap.replace(node,old,(byte[])invocation.getArguments()[1])){
                    throw KeeperException.create(KeeperException.Code.BADVERSION);
                }
                Stat stat = new Stat();
                return stat;
            }
        });

        when(keeper.getData(anyString(),anyBoolean(),(Stat)any())).thenAnswer(new Answer<byte[]>() {
            @Override
            public byte[] answer(InvocationOnMock invocation) throws Throwable {
                String node = (String) invocation.getArguments()[0];
                byte[] data = dataMap.get(node);
                if(data==null)
                    throw KeeperException.create(KeeperException.Code.NONODE);
                return data;
            }
        });
        return keeper;
    }
}
