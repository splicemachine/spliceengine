package com.splicemachine.derby.impl.hbase.job.scheduler;

import com.splicemachine.derby.hbase.job.Status;
import com.splicemachine.derby.hbase.job.TaskFuture;
import com.splicemachine.derby.hbase.job.TaskScheduler;
import com.splicemachine.derby.impl.hbase.job.ZooKeeperTask;
import org.apache.hadoop.hbase.zookeeper.RecoverableZooKeeper;
import org.apache.log4j.Logger;
import org.apache.zookeeper.*;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.util.concurrent.*;

/**
 * Wrapping Scheduler that writes tasks out to ZooKeeper for durability
 *
 * @author Scott Fines
 * Created on: 4/3/13
 */
public class ZkBackedTaskScheduler<T extends ZooKeeperTask> implements TaskScheduler<T> {
    private static final Logger LOG = Logger.getLogger(ZkBackedTaskScheduler.class);
    private final RecoverableZooKeeper zooKeeper;
    private final String baseQueueNode;
    private final TaskScheduler<T> delegate;
    private final ExecutorService cancellationThreads;

    public ZkBackedTaskScheduler(String baseQueueNode,
                                 RecoverableZooKeeper zooKeeper,
                                 TaskScheduler<T> delegate) {
        this.zooKeeper = zooKeeper;
        this.baseQueueNode = baseQueueNode;
        this.delegate = delegate;
        this.cancellationThreads = new ThreadPoolExecutor(1,4,60, TimeUnit.SECONDS,new LinkedBlockingQueue<Runnable>());
    }

    @Override
    public TaskFuture submit(T task) throws ExecutionException {
        try{
            //write out the payload to one node
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutput output = new ObjectOutputStream(baos);
            output.writeObject(task);
            output.flush();
            byte[] data = baos.toByteArray();

            String taskId = task.getTaskId();
            taskId = zooKeeper.create(baseQueueNode+"/"+taskId,data,
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
            task.setTaskId(taskId);

            //write out a status node to ensure that it's properly serialized
            byte[] statusData = task.statusToBytes();
            zooKeeper.create(taskId + "/status", statusData, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

            final TaskFuture future = delegate.submit(task);

            //call exists() on status to make sure that we notice cancellations
            try{
                zooKeeper.exists(taskId+"/status",new Watcher() {
                    @Override
                    public void process(WatchedEvent event) {
                        cancellationThreads.submit(new Callable<Void>(){

                            @Override
                            public Void call() throws Exception {
                                Status status = future.getStatus();
                                //only actually cancel the future if the task is still running
                                switch (status) {
                                    case FAILED:
                                    case COMPLETED:
                                    case CANCELLED:
                                        return null;
                                }
                                try{
                                    future.cancel();
                                }catch(ExecutionException ee){
                                    LOG.error("Unable to cancel task with id "+future.getTaskId(),ee.getCause());
                                }
                                return null;
                            }
                        });
                    }
                });
            }catch(KeeperException ke){
                if(ke.code()== KeeperException.Code.NONODE){
                    //somebody canceled it before we even had a chance to listen for it!
                    future.cancel();
                }
            }

            return future;
        } catch (IOException e) {
            throw new ExecutionException(e);
        } catch (InterruptedException e) {
            throw new ExecutionException(e);
        } catch (KeeperException e) {
            throw new ExecutionException(e);
        }
    }
}
