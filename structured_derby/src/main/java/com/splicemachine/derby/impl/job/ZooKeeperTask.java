package com.splicemachine.derby.impl.job;

import com.splicemachine.job.Status;
import org.apache.hadoop.hbase.zookeeper.RecoverableZooKeeper;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;

/**
 * @author Scott Fines
 * Created on: 4/4/13
 */
public abstract class ZooKeeperTask extends DurableTask{
    private RecoverableZooKeeper zooKeeper;

    protected ZooKeeperTask(){
        super(null);
    }

    protected ZooKeeperTask(String taskId,RecoverableZooKeeper zooKeeper) {
        super(taskId);
        this.zooKeeper = zooKeeper;
    }

    @Override
    public void updateStatus(boolean cancelOnError) throws CancellationException, ExecutionException {
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

}
