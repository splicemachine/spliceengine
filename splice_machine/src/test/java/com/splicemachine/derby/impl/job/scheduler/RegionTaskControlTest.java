package com.splicemachine.derby.impl.job.scheduler;

import com.splicemachine.derby.impl.job.coprocessor.RegionTask;
import com.splicemachine.derby.impl.job.coprocessor.TaskFutureContext;
import com.splicemachine.job.Status;
import com.splicemachine.job.TaskStatus;
import com.splicemachine.utils.SpliceZooKeeperManager;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.zookeeper.RecoverableZooKeeper;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.concurrent.ExecutionException;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Scott Fines
 * Created on: 10/9/13
 */
public class RegionTaskControlTest {

    @Test(expected=KeeperException.class)
    public void testZkSessionExpirationCausesExplosion() throws Exception {
       /*
        * If the task execution node has a zookeeper session expiration, we should detect
        * that the ephemeral task node was deleted, and react as if it was invalidated.
        */
        JobControl mockJobControl = mock(JobControl.class);

        ZooKeeper mockZk = mock(ZooKeeper.class);
        when(mockZk.getSessionTimeout()).thenReturn(10000000);

        final TaskStatus[] taskStatusRef = new TaskStatus[1];
        taskStatusRef[0] = new TaskStatus(Status.PENDING,null);
        final byte[][] statusBytes = new byte[][]{taskStatusRef[0].toBytes()};
        final Watcher[] watcherRef = new Watcher[1];

        String taskNode = "/testNode";
        KeeperException[] errorRef = new KeeperException[1];
        SpliceZooKeeperManager mockZkManager = setUpZooKeeper(mockZk, statusBytes, watcherRef,errorRef);

        TaskFutureContext tfCtx = mock(TaskFutureContext.class);
        when(tfCtx.getTaskNode()).thenReturn(taskNode);

        RegionTask task = mock(RegionTask.class);
        RegionTaskControl regionTaskControl = new RegionTaskControl(HConstants.EMPTY_START_ROW,
                task,
                mockJobControl,
                tfCtx,
                0,
                mockZkManager);

        //make sure initial state is correct
        Assert.assertEquals("Incorrect starting status!",taskStatusRef[0].getStatus(),regionTaskControl.getStatus());

        //make sure that we can notify changes by firing the watcher we got in the getData() call

        doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                RegionTaskControl rtc = (RegionTaskControl)invocation.getArguments()[0];
                try{
                    rtc.getStatus();
                }catch(ExecutionException ee){
                    Throwable t =  ee.getCause();
                    Assert.assertTrue("Incorrect error type!",t instanceof KeeperException);

                    KeeperException ke = (KeeperException)t;
                    Assert.assertEquals("Incorrect Keeper error code!",KeeperException.Code.SESSIONEXPIRED,ke.code());

                    throw ke;
                }
                Assert.fail("Error was not thrown!");

                return null;
            }
        }).when(mockJobControl).taskChanged(regionTaskControl);

        //set up a NoNodeException
        errorRef[0] = new KeeperException.SessionExpiredException();

        /*
         * Most of the time, a SessionExpiration is preceeded by a Disconnected event.
         */
        WatchedEvent event = new WatchedEvent(Watcher.Event.EventType.None, Watcher.Event.KeeperState.Disconnected,taskNode);
        watcherRef[0].process(event);
    }

    @Test
    public void testTaskExecutionSessionExpiresGoesToInvalidState() throws Exception {
       /*
        * If the task execution node has a zookeeper session expiration, we should detect
        * that the ephemeral task node was deleted, and react as if it was invalidated.
        */
        JobControl mockJobControl = mock(JobControl.class);

        ZooKeeper mockZk = mock(ZooKeeper.class);
        when(mockZk.getSessionTimeout()).thenReturn(10000000);

        final TaskStatus[] taskStatusRef = new TaskStatus[1];
        taskStatusRef[0] = new TaskStatus(Status.PENDING,null);
        final byte[][] statusBytes = new byte[][]{taskStatusRef[0].toBytes()};
        final Watcher[] watcherRef = new Watcher[1];

        String taskNode = "/testNode";
        KeeperException[] errorRef = new KeeperException[1];
        SpliceZooKeeperManager mockZkManager = setUpZooKeeper(mockZk, statusBytes, watcherRef,errorRef);

        TaskFutureContext tfCtx = mock(TaskFutureContext.class);
        when(tfCtx.getTaskNode()).thenReturn(taskNode);

        RegionTask task = mock(RegionTask.class);
        RegionTaskControl regionTaskControl = new RegionTaskControl(HConstants.EMPTY_START_ROW,
                task,
                mockJobControl,
                tfCtx,
                0,
                mockZkManager);

        //make sure initial state is correct
        Assert.assertEquals("Incorrect starting status!",taskStatusRef[0].getStatus(),regionTaskControl.getStatus());

        //make sure that we can notify changes by firing the watcher we got in the getData() call

        doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                RegionTaskControl rtc = (RegionTaskControl)invocation.getArguments()[0];
                Status status = rtc.getStatus();

                Assert.assertEquals("Incorrect status!",Status.INVALID,status);
                return null;
            }
        }).when(mockJobControl).taskChanged(regionTaskControl);

        //set up a NoNodeException
        errorRef[0] = new KeeperException.NoNodeException(taskNode);

        /*
         * ZooKeeper will notify watchers of getData() calls that a Node was deleted
         */
        WatchedEvent event = new WatchedEvent(Watcher.Event.EventType.NodeDeleted, Watcher.Event.KeeperState.SyncConnected,taskNode);
        watcherRef[0].process(event);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testCanSubmitAndReceiveProperStateTransitions() throws Exception {
        /*
         * The idea here is to test that the RegionTaskControl can
         * observe the different state transitions in a normal, non-failure process.
         * That is, that it sees PENDING, then EXECUTING, then COMPLETED.
         */
        JobControl mockJobControl = mock(JobControl.class);

        ZooKeeper mockZk = mock(ZooKeeper.class);
        when(mockZk.getSessionTimeout()).thenReturn(10000000);

        final TaskStatus[] taskStatusRef = new TaskStatus[1];
        taskStatusRef[0] = new TaskStatus(Status.PENDING,null);
        final byte[][] statusBytes = new byte[][]{taskStatusRef[0].toBytes()};
        final Watcher[] watcherRef = new Watcher[1];

        SpliceZooKeeperManager mockZkManager = setUpZooKeeper(mockZk, statusBytes, watcherRef,null);

        String taskNode = "/testNode";
        TaskFutureContext tfCtx = mock(TaskFutureContext.class);
        when(tfCtx.getTaskNode()).thenReturn(taskNode);

        RegionTask task = mock(RegionTask.class);
        RegionTaskControl regionTaskControl = new RegionTaskControl(HConstants.EMPTY_START_ROW,
                task,
                mockJobControl,
                tfCtx,
                0,
                mockZkManager);

        //make sure initial state is CORRECT
        Assert.assertEquals("Incorrect starting status!",taskStatusRef[0].getStatus(),regionTaskControl.getStatus());

        //make sure that we can notify changes by firing the watcher we got in the getData() call

        doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                RegionTaskControl rtc = (RegionTaskControl)invocation.getArguments()[0];
                Status status = rtc.getStatus();

                Assert.assertEquals("Incorrect status!",taskStatusRef[0].getStatus(),status);
                return null;
            }
        }).when(mockJobControl).taskChanged(regionTaskControl);

        //change the statusBytes to EXECUTING
        taskStatusRef[0] = new TaskStatus(Status.EXECUTING,null);
        statusBytes[0] = taskStatusRef[0].toBytes();

        //mock receiving a ZooKeeper event
        watcherRef[0].process(mock(WatchedEvent.class));


        //change status to COMPLETED
        taskStatusRef[0] = new TaskStatus(Status.COMPLETED,null);
        statusBytes[0] = taskStatusRef[0].toBytes();

        //mock receiving a ZooKeeper event
        watcherRef[0].process(mock(WatchedEvent.class));

    }

    @SuppressWarnings("unchecked")
    private SpliceZooKeeperManager setUpZooKeeper(ZooKeeper mockZk,
                                                  final byte[][] statusBytes,
                                                  final Watcher[] watcherRef,
                                                  final KeeperException[] errorRef) throws KeeperException, InterruptedException, ZooKeeperConnectionException {
        RecoverableZooKeeper rzk = mock(RecoverableZooKeeper.class);
        when(rzk.getZooKeeper()).thenReturn(mockZk);

        when(rzk.getData(any(String.class),any(Watcher.class),any(Stat.class))).thenAnswer(new Answer<byte[]>() {
            @Override
            public byte[] answer(InvocationOnMock invocation) throws Throwable {
                if(errorRef!=null&& errorRef[0]!=null)
                    throw errorRef[0];

                Watcher watcher = (Watcher) invocation.getArguments()[1];

                watcherRef[0] = watcher;

                return statusBytes[0];
            }
        });

        SpliceZooKeeperManager mockZkManager = mock(SpliceZooKeeperManager.class);
        when(mockZkManager.getRecoverableZooKeeper()).thenReturn(rzk);
        when(mockZkManager.executeUnlessExpired(any(SpliceZooKeeperManager.Command.class))).thenCallRealMethod();
        return mockZkManager;
    }
}
