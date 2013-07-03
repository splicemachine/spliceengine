package com.splicemachine.job;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;
import com.splicemachine.derby.impl.job.coprocessor.CoprocessorTaskScheduler;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.commons.collections.PredicateUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.RecoverableZooKeeper;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;

import javax.annotation.Nullable;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Scott Fines
 * Created on: 4/8/13
 */
public class ZkTaskMonitor implements TaskMonitor{
    private static final Logger LOG = Logger.getLogger(ZkTaskMonitor.class);
    private final RecoverableZooKeeper zooKeeper;
    private final String baseQueueNode;
    private final Map<String,Set<? extends Task>> runningTaskMap = new ConcurrentHashMap<String, Set<? extends Task>>();

    public ZkTaskMonitor(String baseQueueNode,RecoverableZooKeeper zooKeeper) {
        this.zooKeeper = zooKeeper;
        this.baseQueueNode = baseQueueNode;
    }

    public <T extends Task> Set<T> registerRegion(String region){
        Set<T> taskSet = Collections.newSetFromMap(new ConcurrentHashMap<T, Boolean>());
        runningTaskMap.put(region,taskSet);
        return taskSet;
    }

    public void deregisterRegion(String region){
        runningTaskMap.remove(region);
    }

    private static final Predicate<Task> executingFilter = new Predicate<Task>() {
        @Override
        public boolean apply(@Nullable Task input) {
            return input.getTaskStatus().getStatus() == Status.EXECUTING;
        }
    };

    private static final Function<Task,String> taskIdMapper = new Function<Task, String>() {
        @Override
        public String apply(@Nullable Task input) {
            return Bytes.toStringBinary(input.getTaskId());
        }
    };
    @Override
    public List<String> getRunningTasks() {
        List<String> runningTasks = Lists.newArrayList();
        for(String region:runningTaskMap.keySet()){
            runningTasks.addAll(Collections2.transform(Collections2.filter(runningTaskMap.get(region),executingFilter),taskIdMapper));
        }
        return runningTasks;
    }

    @Override
    public void cancelJob(String jobId) {
        String path = CoprocessorTaskScheduler.getJobPath()+"/"+jobId;
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
    public List<String> getRunningJobs() {
        try {
            return zooKeeper.getChildren(CoprocessorTaskScheduler.getJobPath(),false);
        } catch (KeeperException e) {
            if(e.code() ==KeeperException.Code.NONODE){
                SpliceLogUtils.info(LOG,"No tasks have been submitted to this cluster. Ever");
                return Collections.emptyList();
            }
            SpliceLogUtils.error(LOG,"Unable to get running jobs!"+e.getMessage(),e);
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String getStatus(String taskId) {
        return getTaskStatus(taskId).getStatus().name();
    }

    private TaskStatus getTaskStatus(String taskId) {
        try{
            byte[] data = zooKeeper.getData(taskId+"/status",false, new Stat());
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
