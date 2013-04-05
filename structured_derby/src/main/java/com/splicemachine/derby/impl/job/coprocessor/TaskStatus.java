package com.splicemachine.derby.impl.job.coprocessor;

import com.splicemachine.derby.impl.job.scheduler.ThreadedTaskScheduler;
import com.splicemachine.job.Status;

import java.io.*;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author Scott Fines
 *         Created on: 4/3/13
 */
public class TaskStatus implements Externalizable{
    public static interface StatusListener{
       void statusChanged(Status oldStatus,Status newStatus,TaskStatus taskStatus);
    }
    private AtomicReference<Status> status;
    private volatile Throwable error;
    private final Set<StatusListener> listeners;

    public TaskStatus(){
       this.listeners = Collections.newSetFromMap(new ConcurrentHashMap<StatusListener, Boolean>());
    }

    public TaskStatus(Status status, Throwable error) {
        this();
        this.status = new AtomicReference<Status>(status);
        this.error = error;
    }

    public Throwable getError(){
        return error;
    }

    public Status getStatus(){
        return status.get();
    }

    public byte[] toBytes() throws IOException {
        ByteArrayOutputStream baos= new ByteArrayOutputStream();
        ObjectOutput oo = new ObjectOutputStream(baos);
        writeExternal(oo);
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

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeUTF(status.get().name());
        out.writeBoolean(error!=null);
        if(error!=null)
            out.writeObject(error);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        status = new AtomicReference<Status>(Status.valueOf(in.readUTF()));
        if(in.readBoolean()){
            error = (Throwable)in.readObject();
        }
    }

    public void attachListener(StatusListener listener) {
        this.listeners.add(listener);
    }

    public void detachListener(StatusListener listener){
        this.listeners.remove(listener);
    }

}
