package com.splicemachine.job;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.splicemachine.SpliceKryoRegistry;
import com.splicemachine.derby.stats.TaskStats;
import com.splicemachine.si.api.Txn;
import com.splicemachine.si.api.TxnView;
import com.splicemachine.si.impl.InheritingTxnView;
import com.splicemachine.si.impl.LazyTxnView;
import com.splicemachine.si.impl.TransactionStorage;
import com.splicemachine.utils.kryo.KryoPool;
import com.splicemachine.pipeline.exception.Exceptions;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author Scott Fines
 *         Created on: 4/3/13
 */
public class TaskStatus implements Externalizable{
    private static final long serialVersionUID = 6l;

    private AtomicReference<Status> status;
    private final Set<StatusListener> listeners;
    private volatile TaskStats stats;

//    private volatile boolean shouldRetry;
//    private volatile String errorCode;
//    private volatile String errorMessage;
    private volatile ErrorTransport errorTransport;

    private TxnView txn;

    public static TaskStatus failed(String s) {
        return new TaskStatus(Status.FAILED,new IOException(s));
    }

		public Throwable getError() {
        return errorTransport.getError();
    }

    public TxnView getTxnInformation() {
        return txn;
    }


    public static interface StatusListener{
        void statusChanged(Status oldStatus,Status newStatus,TaskStatus taskStatus);
    }

    public TaskStatus(){
       this.listeners = new CopyOnWriteArraySet<StatusListener>();
    }

    public TaskStatus(Status status, Throwable error) {
        this();
        this.status = new AtomicReference<Status>(status);
        if(error!=null){
            error = Exceptions.getRootCause(error);
            errorTransport = ErrorTransport.newTransport(error);
        }
    }

    public void setTxn(TxnView txn) {
        this.txn = txn;
    }

    public boolean shouldRetry(){
        return errorTransport.shouldRetry();
    }

    public Status getStatus(){
        return status.get();
    }

    /**
     * @return stats if the task has then, or {@code null}. Usually, stats are only
     * present when the state is COMPLETED.
     */
    public TaskStats getStats(){
        return this.stats;
    }

    public byte[] toBytes() throws IOException {
    	Kryo kryo = null;
    	try {
    		kryo = SpliceKryoRegistry.getInstance().get();
    		Output output = new Output(100,-1);
    		kryo.writeObject(output,this);
    		return output.toBytes();
    	} finally {
    		if (kryo != null)	
    			SpliceKryoRegistry.getInstance().returnInstance(kryo);
    	}
    }

    public static TaskStatus fromBytes(byte[] bytes) throws IOException, ClassNotFoundException {
        Input input = new Input(bytes);
        Kryo kryo = null;
        KryoPool kryoPool = SpliceKryoRegistry.getInstance();
        try {
            kryo = kryoPool.get();
            return kryo.readObject(input,TaskStatus.class);
        } finally {
            if (kryo != null)
                kryoPool.returnInstance(kryo);
        }
    }

    public static TaskStatus cancelled(){
        return new TaskStatus(Status.CANCELLED,null);
    }

    /**
     * @param status the new status to set
     */
    public void setStatus(Status status) {
        Status oldStatus = this.status.getAndSet(status);
        for(StatusListener listener:listeners){
            listener.statusChanged(oldStatus,status,this);
        }
    }

    public void setError(Throwable error) {
        error = Exceptions.getRootCause(error);
        this.errorTransport = ErrorTransport.newTransport(error);
    }

    public void setStats(TaskStats stats){
        this.stats = stats;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        Status statusInfo = status.get();
        out.writeUTF(statusInfo.name());
        if(statusInfo==Status.FAILED){
            out.writeObject(errorTransport);
        }
        out.writeBoolean(stats!=null);
        if(stats!=null)
            out.writeObject(stats);
        out.writeBoolean(txn!=null);
        if(txn!=null){
            encodeTxn(out);
        }
    }

    private void encodeTxn(ObjectOutput out) throws IOException {
        out.writeLong(txn.getTxnId());
        out.writeLong(txn.getParentTxnId());
        out.writeBoolean(txn.allowsWrites());
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        status = new AtomicReference<Status>(Status.valueOf(in.readUTF()));
        if(status.get()==Status.FAILED){
            errorTransport = (ErrorTransport)in.readObject();
        }
        if(in.readBoolean()){
            stats = (TaskStats)in.readObject();
        }
        if(in.readBoolean()){
            long txnId = in.readLong();
            long parentTxn = in.readLong();
            boolean allowsWrites = in.readBoolean();
            /*
             * If the parent transaction id is the same as ours, and we don't allow writes,then
             * we treat this as a read-only transaction. Since commits and rollbacks don't do anything
             * for read only operations, it's fine to just inherit from the root transaction.
             */
            TxnView pView = !allowsWrites? Txn.ROOT_TRANSACTION: new LazyTxnView(parentTxn,TransactionStorage.getTxnSupplier());
            txn = new InheritingTxnView(pView,txnId,txnId,allowsWrites, null, null);
        }
    }

    public void attachListener(StatusListener listener) {
        this.listeners.add(listener);
        listener.statusChanged(null,status.get(),this);
    }

    public void detachListener(StatusListener listener){
        this.listeners.remove(listener);
    }

}
