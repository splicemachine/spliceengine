package com.splicemachine.derby.impl.job;

import com.splicemachine.derby.impl.job.coprocessor.CoprocessorTaskScheduler;
import com.splicemachine.derby.impl.job.coprocessor.RegionTask;
import com.splicemachine.derby.utils.ByteDataOutput;
import com.splicemachine.job.Status;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.MD5Hash;
import org.apache.hadoop.hbase.zookeeper.RecoverableZooKeeper;
import org.apache.log4j.Logger;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;

/**
 * @author Scott Fines
 * Created on: 4/4/13
 */
public abstract class ZooKeeperTask extends DurableTask implements RegionTask {
    private static final long serialVersionUID=1l;
    protected final Logger LOG;
    protected RecoverableZooKeeper zooKeeper;
    private String statusNode;
    protected String jobId;

    protected ZooKeeperTask(){
        super(null);
        this.LOG = Logger.getLogger(this.getClass());
    }

    protected ZooKeeperTask(String jobId){
        super(null);
        this.jobId = jobId.replaceAll("/","_");
        this.LOG = Logger.getLogger(this.getClass());
    }

    protected ZooKeeperTask(String taskId,String jobId,RecoverableZooKeeper zooKeeper) {
        super(taskId);
        this.zooKeeper = zooKeeper;
        this.jobId = jobId;
        this.LOG = Logger.getLogger(this.getClass());
    }

    @Override
    public void prepareTask(HRegion region,
                            RecoverableZooKeeper zooKeeper) throws ExecutionException {
        this.taskId = buildTaskId(region,jobId ,getTaskType());
        this.zooKeeper = zooKeeper;
        //write out the payload to a durable node
        ByteDataOutput byteOut = new ByteDataOutput();
        try {
            byteOut.writeObject(this);
            byte[] payload = byteOut.toByteArray();

            String taskId = getTaskId();
            taskId = zooKeeper.create(taskId,payload,
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
            setTaskId(taskId);

            byte[] statusData = statusToBytes();
            statusNode = zooKeeper.create(taskId+"/status",statusData,ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT);
            checkNotCancelled();

        } catch (IOException e) {
            throw new ExecutionException(e);
        } catch (InterruptedException e) {
            throw new ExecutionException(e);
        } catch (KeeperException e) {
            throw new ExecutionException(e);
        }
    }

    protected abstract String getTaskType();

    @Override
    public void markCancelled() throws ExecutionException {
        SpliceLogUtils.trace(LOG,"Marking task %s cancelled",taskId);
        markCancelled(true);
    }

    @Override
    public void markStarted() throws ExecutionException, CancellationException {
        SpliceLogUtils.trace(LOG, "Marking task %s started", taskId);
        status.setStatus(Status.EXECUTING);
        updateStatus(true);
        //reset the cancellation watch to notify us if the node is deleted
        checkNotCancelled();

    }

    @Override
    public void markCompleted() throws ExecutionException {
        SpliceLogUtils.trace(LOG, "Marking task %s completed", taskId);
        status.setStatus(Status.COMPLETED);
        updateStatus(false);

    }

    @Override
    public void markFailed(Throwable error) throws ExecutionException {
        switch (status.getStatus()) {
            case INVALID:
            case FAILED:
            case COMPLETED:
                SpliceLogUtils.warn(LOG,"Received task error after entering "+status.getStatus()+" state, ignoring",error);
                return;
        }

        SpliceLogUtils.trace(LOG,"Marking task %s failed",taskId);
        status.setError(error);
        status.setStatus(Status.FAILED);
        updateStatus(false);
    }

    @Override
    public void markInvalid() throws ExecutionException {
        SpliceLogUtils.trace(LOG,"Marking task %s invalid",taskId);
        status.setStatus(Status.INVALID);
        updateStatus(false);
    }

    @Override
    public void updateStatus(boolean cancelOnError) throws CancellationException, ExecutionException {
        assert zooKeeper!=null;
        try{
            zooKeeper.setData(taskId+"/status",statusToBytes(),-1);
        } catch (InterruptedException e) {
            throw new CancellationException();
        } catch (KeeperException e) {
            if(e.code()== KeeperException.Code.NONODE&&cancelOnError){
                status.setStatus(Status.CANCELLED);
                throw new CancellationException();
            }else
                throw new ExecutionException(e);
        } catch (IOException e) {
            throw new ExecutionException(e);
        }
    }

    @Override
    public void cleanup() throws ExecutionException {
        //delete the task node and status node
        try {
            zooKeeper.delete(taskId+"/status",-1);
        } catch (InterruptedException e) {
            throw new ExecutionException(e);
        } catch (KeeperException e) {
            if(e.code()!= KeeperException.Code.NONODE)
                throw new ExecutionException(e);
        }
        try {
            zooKeeper.delete(taskId,-1);
        } catch (InterruptedException e) {
            throw new ExecutionException(e);
        } catch (KeeperException e) {
            if(e.code()!= KeeperException.Code.NONODE)
                throw new ExecutionException(e);
        }
    }

    private void markCancelled(boolean propagate) throws ExecutionException{
        SpliceLogUtils.trace(LOG,"cancelling task %s "+(propagate ? ", propagating cancellation state":"not propagating state"),taskId);
        switch (status.getStatus()) {
            case FAILED:
            case COMPLETED:
            case CANCELLED:
                return; //nothing to do
        }

        status.setStatus(Status.CANCELLED);
        if(propagate)
            updateStatus(false);
    }

    private static String buildTaskId(HRegion region,String jobId,String taskType) {
        HRegionInfo regionInfo = region.getRegionInfo();
        return CoprocessorTaskScheduler.getRegionQueue(regionInfo)+"_"+jobId+"_"+taskType+"-";
    }

    private void checkNotCancelled()throws ExecutionException {
        SpliceLogUtils.trace(LOG,"Attaching existence watcher to task %s",taskId);
        //call exists() on status to make sure that we notice cancellations
        Stat stat;
        try {
            stat = zooKeeper.exists(CoprocessorTaskScheduler.getJobPath()+"/"+jobId,new Watcher() {
                @Override
                public void process(WatchedEvent event) {
                    SpliceLogUtils.trace(LOG,"Received WatchedEvent "+ event.getType());
                    if(event.getType()!=Event.EventType.NodeDeleted)
                        return; //nothing to do
                    try{
                        markCancelled(false);
                    }catch(ExecutionException ee){
                        SpliceLogUtils.error(LOG, "Unable to cancel task with id " + getTaskId(), ee.getCause());
                    }
                }
            });
            if(stat==null){
                //we've already been cancelled!
                markCancelled(false);
            }
        } catch (KeeperException e) {
            throw new ExecutionException(e);
        } catch (InterruptedException e) {
            throw new ExecutionException(e);
        }
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        jobId = in.readUTF();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeUTF(jobId);
    }
}
