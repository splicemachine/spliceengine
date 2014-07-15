package com.splicemachine.derby.impl.job;

import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.impl.job.coprocessor.CoprocessorTaskScheduler;
import com.splicemachine.derby.impl.job.coprocessor.RegionTask;
import com.splicemachine.derby.utils.ErrorReporter;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.job.Status;
import com.splicemachine.job.TaskStatus;
import com.splicemachine.si.api.TransactionLifecycle;
import com.splicemachine.si.api.Txn;
import com.splicemachine.si.api.TxnLifecycleManager;
import com.splicemachine.si.impl.ActiveWriteTxn;
import com.splicemachine.si.impl.InheritingTxnView;
import com.splicemachine.si.impl.ReadOnlyTxn;
import com.splicemachine.utils.SpliceLogUtils;
import com.splicemachine.utils.SpliceZooKeeperManager;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.RecoverableZooKeeper;
import org.apache.log4j.Logger;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;

/**
 *
 * @author Scott Fines
 * Created on: 5/9/13
 */
public abstract class ZkTask implements RegionTask,Externalizable {
    private static final long serialVersionUID = 6l;
    protected final Logger LOG;
    protected TaskStatus status;
    //job scheduler creates the task id
    protected byte[] taskId;
    protected String jobId;
    protected SpliceZooKeeperManager zkManager;
    private int priority;

		private String taskPath;

		protected byte[] parentTaskId = null;
		private TaskWatcher taskWatcher;

		private volatile Thread executionThread;
		private long prepareTimestamp;
		protected long waitTimeNs;
		protected HRegion region;

		private Txn txn;

		protected ZkTask() {
        this.LOG = Logger.getLogger(this.getClass());
        this.status = new TaskStatus(Status.PENDING,null);
    }

    /**
     * @param jobId
     * @param priority
     */
    protected ZkTask(String jobId, int priority) {
				this(jobId,priority,null);
    }

		protected ZkTask(String jobId, int priority,
										 byte[] parentTaskId) {
				this.LOG = Logger.getLogger(this.getClass());
				this.jobId = jobId;
				this.priority = priority;
				this.status = new TaskStatus(Status.PENDING,null);
				this.parentTaskId = parentTaskId;
		}

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        jobId = in.readUTF();
        priority = in.readInt();
        if(in.readBoolean())
						txn = decodeTxn(in);
        int taskIdSize = in.readInt();
        if(taskIdSize>0){
            taskId = new byte[taskIdSize];
            in.readFully(taskId);
        }
				if(in.readBoolean()){
						parentTaskId = new byte[in.readInt()];
						in.readFully(parentTaskId);
				}
    }


		@Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeUTF(jobId);
        out.writeInt(priority);
        out.writeBoolean(txn!=null);
        if(txn!=null)
						encodeTxn(out);
        if(taskId!=null){
            out.writeInt(taskId.length);
            out.write(taskId);
        }else
            out.writeInt(0);
				out.writeBoolean(parentTaskId!=null);
				if(parentTaskId!=null){
						out.writeInt(parentTaskId.length);
						out.write(parentTaskId);
				}
    }


		@Override
    public void execute() throws ExecutionException, InterruptedException {
//        /*
//         * Create the Child transaction.
//         *
//         * We do this here rather than elsewhere (like inside the JobScheduler) so
//         * that we avoid timing out the transaction when we have to sit around and
//         * wait for a long time.
//         */
//        if(parentTxn!=null){
//						TxnLifecycleManager tc = TransactionLifecycle.getLifecycleManager();
////            TransactionManager transactor = HTransactorFactory.getTransactionManager();
////            TransactionId parent = transactor.transactionIdFromString(parentTxnId);
//            try {
//                Txn childTxnId  = beginChildTransaction(tc,parentTxn);
//                status.setTxnId(childTxnId.getTransactionIdString());
//            } catch (IOException e) {
//                throw new ExecutionException("Unable to acquire child transaction",e);
//            }
//        }

				waitTimeNs = System.nanoTime()-prepareTimestamp;
				try{
						doExecute();
				}finally{
						taskWatcher.task=null;
				}
    }

		protected Txn beginChildTransaction(Txn parentTxn,TxnLifecycleManager tc) throws IOException {
				return tc.beginChildTransaction(parentTxn,null);
//				return transactor.beginChildTransaction(parent, !readOnly, !readOnly);
		}

		protected abstract void doExecute() throws ExecutionException, InterruptedException;

    @Override
    public void prepareTask(byte[] start, byte[] stop,RegionCoprocessorEnvironment rce, SpliceZooKeeperManager zooKeeper)
                                                                throws ExecutionException {
        taskId = SpliceUtils.getUniqueKey();
        taskPath = jobId+"_"+ getTaskType()+"-"+ Bytes.toLong(taskId);
        this.zkManager = zooKeeper;
				this.region = rce.getRegion();
        try {
            //create a status node
            final byte[] statusData = statusToBytes();

            taskPath= zooKeeper.execute(new SpliceZooKeeperManager.Command<String>() {
                @Override
                public String execute(RecoverableZooKeeper zooKeeper) throws InterruptedException, KeeperException {
                    String path = SpliceConstants.zkSpliceTaskPath + "/" + taskPath;
                    zooKeeper.getZooKeeper().create(path,statusData,ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.EPHEMERAL,new AsyncCallback.StringCallback() {
                        @Override
                        public void processResult(int rc, String path, Object ctx, String name) {
                            if(LOG.isTraceEnabled())
                                LOG.trace("Task path created");
                        }
                    },this);
//                    return zooKeeper.create(path,
//                            statusData, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
                    return path;
                }
            });
        } catch (InterruptedException e) {
            throw new ExecutionException(e);
        } catch (KeeperException e) {
            throw new ExecutionException(e);
        } catch (IOException e) {
            throw new ExecutionException(e);
        }

        //attach a listener to the job node
        checkNotCancelled();

				prepareTimestamp = System.nanoTime();
    }

		@Override
		public void setTxn(Txn txn) {
				this.txn = txn;
		}

		protected abstract String getTaskType();

    private void checkNotCancelled() throws ExecutionException{
        Stat stat;
        try {
						taskWatcher = new TaskWatcher(this);
            stat = zkManager.execute(new SpliceZooKeeperManager.Command<Stat>() {
                @Override
                public Stat execute(RecoverableZooKeeper zooKeeper) throws InterruptedException, KeeperException {
                    return zooKeeper.exists(CoprocessorTaskScheduler.getJobPath()+"/"+jobId,taskWatcher);
                }
            });
            if(stat==null)
                markCancelled(false);
        } catch (InterruptedException e) {
            throw new ExecutionException(e);
        } catch (KeeperException e) {
            throw new ExecutionException(e);
        }
    }

    @Override
    public void markCancelled(boolean propagate) throws ExecutionException{
        switch (status.getStatus()) {
            case FAILED:
            case COMPLETED:
            case CANCELLED:
                return;
        }
        status.setStatus(Status.CANCELLED);
        if (propagate)
            updateStatus(false);

        if (executionThread != null) {
            LOG.info("Task " + Bytes.toLong(taskId) + " has been cancelled, interrupting worker thread");
            executionThread.interrupt();
        }
    }

    private void updateStatus(final boolean cancelOnError) throws ExecutionException{
				boolean interrupted = Thread.currentThread().isInterrupted();
				Thread.interrupted(); //clear the interrupt status so that this will complete
        try{
            final byte[] status = statusToBytes();
            zkManager.executeUnlessExpired(new SpliceZooKeeperManager.Command<Void>() {
                @Override
                public Void execute(RecoverableZooKeeper zooKeeper) throws InterruptedException, KeeperException {
                    zooKeeper.setData(taskPath,status,-1);
                    return null;
                }
            });
        } catch (IOException e) {
            throw new ExecutionException(e);
        } catch (InterruptedException e) {
            throw new ExecutionException(e);
        } catch (KeeperException e) {
            if(e.code()== KeeperException.Code.NONODE&&cancelOnError){
                status.setStatus(Status.CANCELLED);
                throw new CancellationException();
            }
            throw new ExecutionException(e);
        }finally{
						if(interrupted)
								Thread.currentThread().interrupt();
				}
    }

    private byte[] statusToBytes() throws IOException {
        return status.toBytes();
    }

    @Override
    public void markStarted() throws ExecutionException, CancellationException {
				executionThread = Thread.currentThread();
				TaskStatus taskStatus = getTaskStatus();
				//if task has already been cancelled, then throw it away
				if(taskStatus.getStatus()==Status.CANCELLED) throw new CancellationException();
				setStatus(Status.EXECUTING,true);
    }

    private void setStatus(Status newStatus, boolean cancelOnError) throws ExecutionException{
				if(LOG.isTraceEnabled())
						SpliceLogUtils.trace(LOG,"Marking task %s %s",Bytes.toLong(getTaskId()),newStatus);
        status.setStatus(newStatus);
        updateStatus(cancelOnError);
    }

    @Override
    public void markCompleted() throws ExecutionException {
        setStatus(Status.COMPLETED,false);
    }

    @Override
    public void markFailed(Throwable error) throws ExecutionException {
        ErrorReporter.get().reportError(this.getClass(),error);
        switch (status.getStatus()) {
            case INVALID:
            case FAILED:
            case COMPLETED:
                SpliceLogUtils.warn(LOG,"["+Bytes.toLong(getTaskId())+"]:Received task error after entering " + status.getStatus()+" state, ignoring",error);
                return;
        }
        status.setError(error);
        setStatus(Status.FAILED,false);
    }

    @Override
    public void markCancelled() throws ExecutionException {
				markCancelled(false);
    }

    @Override
    public boolean isCancelled() throws ExecutionException {
        return status.getStatus()==Status.CANCELLED;
    }

    @Override
    public byte[] getTaskId() {
        return taskId;
    }

    @Override
    public TaskStatus getTaskStatus() {
        return status;
    }

    @Override
    public void markInvalid() throws ExecutionException {
				if(taskWatcher!=null)
						taskWatcher.task = null;
        //only invalidate if we are in the PENDING state, otherwise let it go through or fail
        if(getTaskStatus().getStatus()!=Status.PENDING)
            return;
        setStatus(Status.INVALID,false);
    }

    @Override
    public boolean isInvalidated() {
        return status.getStatus()==Status.INVALID;
    }

    @Override
    public void cleanup() throws ExecutionException {
				taskWatcher.task= null;
        throw new UnsupportedOperationException(
                "Tasks can not be cleaned up on the Task side--use JobScheduler.cleanupJob() instead");
    }

    @Override
    public String getTaskNode() {
        return taskPath;
    }

		@Override public boolean isSplittable() { return false; }

		@Override
		public Txn getTxn() {
				return txn;
		}

		@Override
    public byte[] getParentTaskId() {
        return parentTaskId;
    }

		@Override
		public String getJobId() {
				return jobId;
		}

		private static class TaskWatcher implements Watcher {
				private volatile ZkTask task;

				private TaskWatcher(ZkTask task) {
						this.task = task;
				}

				@Override
				public void process(WatchedEvent event) {
						if (event.getType() != Event.EventType.NodeDeleted)
								return;


						/*
                         * If the watch was triggered after
						 * dereferencing us, then we don't care about it
						 */
						if (task == null) return;

						if (task.LOG.isTraceEnabled())
								task.LOG.trace("Received node deleted notice from ZooKeeper, attempting cancellation");

						switch (task.status.getStatus()) {
								case FAILED:
								case COMPLETED:
								case CANCELLED:
										if (task.LOG.isTraceEnabled())
												task.LOG.trace("Node is already in a finalize state, ignoring cancellation attempt");
										task = null;
										return;
						}

						try {
								task.markCancelled(false);
						} catch (ExecutionException ee) {
								SpliceLogUtils.error(task.LOG, "Unable to cancel task with id " + Bytes.toLong(task.getTaskId()), ee.getCause());
						} finally {
								task = null;
						}
				}
		}

		private void encodeTxn(ObjectOutput out) throws IOException {
				out.writeLong(txn.getTxnId());
				out.writeLong(txn.getParentTransaction().getTxnId());
				out.writeByte(txn.getIsolationLevel().encode());
				out.writeBoolean(txn.allowsWrites());
				out.writeBoolean(txn.isDependent());
				out.writeBoolean(txn.isAdditive());
		}

		private Txn decodeTxn(ObjectInput in) throws IOException {
				long txnId = in.readLong();
				long parentTxnId = in.readLong();
				Txn.IsolationLevel level = Txn.IsolationLevel.fromByte(in.readByte());
				boolean allowWrites = in.readBoolean();
				boolean dependent = in.readBoolean();
				boolean additive = in.readBoolean();

				//should be lazy, so won't cost anything
				if(!allowWrites){
						/*
						 * The parent transaction didn't allow writes, so it probably doesn't have an
						 * entry in the Txn table anyway, so we don't need to go through the storage,
						 * and deal with any of that damage. Instead, we just create an arbitrary
						 * read-only txn object to represent the parent and the child.
						 *
						 * This is helped by the fact that we cannot create a writable child of a read-only
						 * transaction. Thus, anything that we do with this transaction must be read-only,
						 * so it really doesn't matter if the parent of the parent's transaction is read-only
						 * in effect, or reality (recall that we are going to use the transaction here decoded
						 *  to create a child transaction).
						 */
						Txn parentTxn;
						TxnLifecycleManager lifecycleManager = TransactionLifecycle.noOpLifecycleManager();
						if(parentTxnId<0)
								parentTxn = Txn.ROOT_TRANSACTION;
						else{
								parentTxn = ReadOnlyTxn.createReadOnlyTransaction(parentTxnId,
												Txn.ROOT_TRANSACTION,parentTxnId,level,dependent,dependent, lifecycleManager);
						}
						return ReadOnlyTxn.createReadOnlyTransaction(txnId, parentTxn, txnId, level, dependent, additive, lifecycleManager);
				}else{
						/*
						 * The parent transaction allows writes. Typically, this means that we would need to get
						 * a transaction from storage. However, we want to avoid any excessive network calls, so
						 * we will use an "ActiveWriteTxn" to represent the parent and the transaction itself
						 */
						Txn parentTxn;
						if(parentTxnId<0)
								parentTxn = Txn.ROOT_TRANSACTION;
						else{
                //TODO -sf- is this correct?
								parentTxn = new ActiveWriteTxn(parentTxnId,parentTxnId,Txn.ROOT_TRANSACTION);
						}
						return new InheritingTxnView(parentTxn,txnId,txnId,level,
										true,dependent,
										true,additive,
										true,true,
										-1l,-1l,
										Txn.State.ACTIVE);
				}
		}
}
