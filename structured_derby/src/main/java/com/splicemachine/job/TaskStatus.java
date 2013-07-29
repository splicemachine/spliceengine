package com.splicemachine.job;

import com.google.common.base.Throwables;
import com.splicemachine.derby.stats.TaskStats;
import com.splicemachine.derby.utils.Exceptions;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.client.Row;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
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
    private volatile String txnId;

    private volatile boolean shouldRetry;
    private volatile String errorCode;
    private volatile String errorMessage;

    public static TaskStatus failed(String s) {
        return new TaskStatus(Status.FAILED,new IOException(s));
    }

    public String getTransactionId() {
        return txnId;
    }

    public String getErrorCode() {
        return errorCode;
    }


    public static interface StatusListener{
        void statusChanged(Status oldStatus,Status newStatus,TaskStatus taskStatus);
    }
    public TaskStatus(){
       this.listeners = Collections.newSetFromMap(new ConcurrentHashMap<StatusListener, Boolean>());
    }

    public TaskStatus(Status status, Throwable error) {
        this();
        this.status = new AtomicReference<Status>(status);
        if(error!=null){
            error = Exceptions.getRootCause(error);
            this.shouldRetry = Exceptions.shouldRetry(error);
            this.errorMessage = error.getMessage();
        }
    }

    public void setTxnId(String txnId){
        this.txnId = txnId;
    }

    public String getErrorMessage(){
       return errorMessage;
    }

    public boolean shouldRetry(){
        return shouldRetry;
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
        ByteArrayOutputStream baos= new ByteArrayOutputStream();
        ObjectOutput oo = new ObjectOutputStream(baos);
        oo.writeObject(this);
        oo.flush();
        return baos.toByteArray();
    }

    public void fromBytes(byte[] bytes) throws IOException, ClassNotFoundException {
        ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
        ObjectInput ii = new ObjectInputStream(bais);
        readExternal(ii);
    }

    public static TaskStatus cancelled(){
        return new TaskStatus(Status.CANCELLED,null);
    }

    /**
     * @param status the new status to set
     * @return the old status
     */
    public void setStatus(Status status) {
        Status oldStatus = this.status.getAndSet(status);
        for(StatusListener listener:listeners){
            listener.statusChanged(oldStatus,status,this);
        }
    }

    public void setError(Throwable error) {
        error = Exceptions.getRootCause(error);
        this.errorMessage = error.getMessage();
        this.errorCode = Exceptions.getErrorCode(error);
        this.shouldRetry = Exceptions.shouldRetry(error);
    }

    public void setStats(TaskStats stats){
        this.stats = stats;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        Status statusInfo = status.get();
        out.writeUTF(statusInfo.name());
        if(statusInfo==Status.FAILED){
            out.writeBoolean(shouldRetry);
            out.writeUTF(errorCode);
            out.writeUTF((errorMessage != null ? errorMessage : "NULL"));
        }
        out.writeBoolean(stats!=null);
        if(stats!=null)
            out.writeObject(stats);
        out.writeBoolean(txnId !=null);
        if(txnId !=null)
            out.writeUTF(txnId);
    }

    private void writeError(ObjectOutput out, Throwable error) throws IOException {
        Throwable e = Throwables.getRootCause(error);

        if(e instanceof RetriesExhaustedWithDetailsException){
            RetriesExhaustedWithDetailsException rewde = (RetriesExhaustedWithDetailsException)e;
            List<String>hostnameAndPorts = Collections.emptyList();
            RetriesExhaustedWithDetailsException copy = new RetriesExhaustedWithDetailsException(rewde.getCauses(),
                    Collections.<Row>emptyList(),hostnameAndPorts);
            e = copy;
            out.writeObject(e);
        }else{
            out.writeObject(error);
        }
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        status = new AtomicReference<Status>(Status.valueOf(in.readUTF()));
        if(status.get()==Status.FAILED){
            shouldRetry = in.readBoolean();
            errorCode = in.readUTF();
            errorMessage = in.readUTF();
        }
        if(in.readBoolean()){
            stats = (TaskStats)in.readObject();
        }

        if(in.readBoolean())
            txnId = in.readUTF();
    }

    public void attachListener(StatusListener listener) {
        this.listeners.add(listener);
    }

    public void detachListener(StatusListener listener){
        this.listeners.remove(listener);
    }

}
