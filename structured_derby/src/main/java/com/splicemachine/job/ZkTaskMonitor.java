package com.splicemachine.job;

import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.hbase.zookeeper.RecoverableZooKeeper;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.util.Collections;
import java.util.List;

/**
 * @author Scott Fines
 * Created on: 4/8/13
 */
public class ZkTaskMonitor implements TaskMonitor{
    private static final Logger LOG = Logger.getLogger(ZkTaskMonitor.class);
    private final RecoverableZooKeeper zooKeeper;
    private final String baseQueueNode;

    public ZkTaskMonitor(String baseQueueNode,RecoverableZooKeeper zooKeeper) {
        this.zooKeeper = zooKeeper;
        this.baseQueueNode = baseQueueNode;
    }

    @Override
    public int getNumRegions(String tableId) {
        return getRegions(tableId).size();
    }

    @Override
    public List<String> getRegions(String tableId) {
        try {
            return zooKeeper.getChildren(baseQueueNode+"/"+tableId,false);
        } catch (KeeperException e) {
            if(e.code()== KeeperException.Code.NONODE)
                return Collections.emptyList();
            LOG.debug("Error getting task regions:"+e.getMessage());
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<String> getTables() {
        try{
            return zooKeeper.getChildren(baseQueueNode,false);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (KeeperException e) {
            if(e.code()== KeeperException.Code.NONODE)
                return Collections.emptyList(); //should never happen, but just in case
            LOG.debug("Error getting task regions:"+e.getMessage());
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<String> getTasks(String tableId, String regionId) {
        try{
            return zooKeeper.getChildren(baseQueueNode+"/"+tableId+"/"+regionId,false);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (KeeperException e) {
            if(e.code()== KeeperException.Code.NONODE)
                return Collections.emptyList();
            LOG.debug("Error getting tasks:"+e.getMessage());
            throw new RuntimeException(e);
        }
    }

    @Override
    public void cancelTask(String tableId, String regionId, String taskId) {
        String path = baseQueueNode+"/"+tableId+"/"+regionId+"/"+taskId+"/status";
        try{
            zooKeeper.delete(path,-1);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (KeeperException e) {
            if(e.code() == KeeperException.Code.NONODE){
                SpliceLogUtils.trace(LOG,"task %s does not exist, assuming already cancelled",path);
                return; //already cancelled! whoo!
            }
            LOG.debug("Error cancelling task:"+e.getMessage());
            throw new RuntimeException(e);
        }
    }

    @Override
    public String getStatus(String tableId, String regionId, String taskId) {
        return getTaskStatus(tableId, regionId, taskId).getStatus().name();
    }

    private TaskStatus getTaskStatus(String tableId, String regionId, String taskId) {
        try{
            byte[] data = zooKeeper.getData(baseQueueNode+"/"+tableId+"/"+regionId+"/"+taskId+"/status",false, new Stat());
            ByteArrayInputStream bais = new ByteArrayInputStream(data);
            ObjectInput in = new ObjectInputStream(bais);
            return (TaskStatus)in.readObject();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (KeeperException e) {
            if(e.code()== KeeperException.Code.NONODE){
                //task has been Cancelled
                return TaskStatus.cancelled();
            }
            LOG.debug("Error getting task status:",e);
            throw new RuntimeException(e);
        } catch (ClassNotFoundException e) {
            LOG.debug("Error getting task status:",e);
            throw new RuntimeException(e);
        } catch (IOException e) {
            LOG.debug("Error getting task status:",e);
            throw new RuntimeException(e);
        }
    }
}
