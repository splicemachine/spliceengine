package com.splicemachine.job;

import com.google.common.base.Throwables;
import com.splicemachine.derby.stats.TaskStats;
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
    private static final long serialVersionUID = 4l;

    public static TaskStatus failed(String s) {
        return new TaskStatus(Status.FAILED,new IOException(s));
    }

    public String getTransactionId() {
        return txnId;
    }


    public static interface StatusListener{
       void statusChanged(Status oldStatus,Status newStatus,TaskStatus taskStatus);
    }
    private AtomicReference<Status> status;
    private volatile Throwable error;
    private final Set<StatusListener> listeners;
    private volatile TaskStats stats;
    private volatile String txnId;

    public TaskStatus(){
       this.listeners = Collections.newSetFromMap(new ConcurrentHashMap<StatusListener, Boolean>());
    }

    public TaskStatus(Status status, Throwable error) {
        this();
        this.status = new AtomicReference<Status>(status);
        this.error = error;
    }

    public void setTxnId(String txnId){
        this.txnId = txnId;
    }

    public Throwable getError(){
        return error;
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
        this.error = error;
    }

    public void setStats(TaskStats stats){
        this.stats = stats;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeUTF(status.get().name());
        out.writeBoolean(error!=null);
        if(error!=null){
            writeError(out, error);
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
        }
        out.writeObject(e);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        status = new AtomicReference<Status>(Status.valueOf(in.readUTF()));
        if(in.readBoolean()){
            error = (Throwable)in.readObject();
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
