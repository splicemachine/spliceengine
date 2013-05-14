package com.splicemachine.derby.impl.job;

import com.splicemachine.utils.SpliceZooKeeperManager;
import com.splicemachine.derby.utils.ByteDataOutput;
import com.splicemachine.job.Status;
import com.splicemachine.job.TaskStatus;
import com.splicemachine.si.api.TransactionId;
import com.splicemachine.si.api.Transactor;
import com.splicemachine.si.impl.TransactorFactoryImpl;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.zookeeper.RecoverableZooKeeper;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.concurrent.ExecutionException;

/**
 * @author Scott Fines
 * Created on: 5/3/13
 */
public abstract class TransactionalTask extends ZooKeeperTask{
    private static final long serialVersionUID=2l;
    private String transactionId;
    private String parentTransaction;
    private boolean readOnly;

    protected TransactionalTask() {
        super();
    }

    protected TransactionalTask(String jobId, int priority, String parentTransaction,boolean readOnly) {
        super(jobId, priority);
        this.parentTransaction = parentTransaction;
        this.transactionId = null;
        this.readOnly = readOnly;
    }

    @Override
    public void prepareTask(HRegion region, SpliceZooKeeperManager zkManager) throws ExecutionException {
        //check the current state
        try {
            TaskStatus currentStatus = getTaskStatus();
            switch (currentStatus.getStatus()) {
                case INVALID:
                case PENDING:
                    break;
                case EXECUTING:
                case FAILED:
                    rollbackIfNecessary();
                    break;
                case COMPLETED:
                case CANCELLED:
                    return; //nothing to do here, since we've already taken care of things
            }

            //create a new child transaction of the parent transaction
            final Transactor<Put, Get, Scan, Mutation> transactor = TransactorFactoryImpl.getTransactor();
            TransactionId id = transactor.beginChildTransaction(
                    transactor.transactionIdFromString(parentTransaction), !readOnly, !readOnly, null, null);
            transactionId = id.getTransactionIdString();

            /*
             * If our taskId is non-null, then we are a re-run of an earlier attempt, which
             * means that we needed to roll back the transaction and create a new one. This
             * in turn means that our state representation has changed, and we need to update
             * it to reflect the new transaction.
             */
            if(taskId!=null){
                ByteDataOutput byteOut = new ByteDataOutput();
                byteOut.writeObject(this);
                byte[] payload = byteOut.toByteArray();

                RecoverableZooKeeper zooKeeper =zkManager.getRecoverableZooKeeper();
                zooKeeper.setData(taskId,payload,-1);

                getTaskStatus().setStatus(Status.PENDING);
                byte[] statusData = statusToBytes();
                zooKeeper.create(taskId+"/status",statusData,ZooDefs.Ids.OPEN_ACL_UNSAFE,
                        CreateMode.EPHEMERAL);
            }
        } catch (IOException e) {
            throw new ExecutionException(e);
        } catch (InterruptedException e) {
            throw new ExecutionException(e);
        } catch (KeeperException e) {
            throw new ExecutionException(e);
        }
        super.prepareTask(region, zkManager);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeUTF(parentTransaction);
        out.writeBoolean(readOnly);
        out.writeBoolean(transactionId!=null);
        if(transactionId!=null)
            out.writeUTF(transactionId);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        parentTransaction = in.readUTF();
        readOnly = in.readBoolean();
        if(in.readBoolean())
            transactionId = in.readUTF();

    }

    private void rollbackIfNecessary() throws IOException {
        if(transactionId==null) return; //nothing to roll back just yet

        final Transactor<Put, Get, Scan, Mutation> transactor = TransactorFactoryImpl.getTransactor();
        transactor.rollback(transactor.transactionIdFromString(transactionId));

    }
}
