package com.splicemachine.job;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.stats.TaskStats;
import com.splicemachine.derby.utils.Exceptions;
import org.apache.log4j.Logger;

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
    private volatile String txnId;

//    private volatile boolean shouldRetry;
//    private volatile String errorCode;
//    private volatile String errorMessage;
    private volatile ErrorTransport errorTransport;

    public static TaskStatus failed(String s) {
        return new TaskStatus(Status.FAILED,new IOException(s));
    }

    public String getTransactionId() {
        return txnId;
    }

    public Throwable getError() {
        return errorTransport.getError();
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
//            this.shouldRetry = Exceptions.shouldRetry(error);
//            this.errorMessage = error.getMessage();
        }
    }

    public void setTxnId(String txnId){
        this.txnId = txnId;
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
    		kryo = SpliceDriver.getKryoPool().get();
    		Output output = new Output(100,-1);
    		kryo.writeObject(output,this);
    		return output.toBytes();
    	} finally {
    		if (kryo != null)	
    			SpliceDriver.getKryoPool().returnInstance(kryo);
    	}
    }

    public static TaskStatus fromBytes(byte[] bytes) throws IOException, ClassNotFoundException {
        Input input = new Input(bytes);
        Kryo kryo = null;
        try {
        	kryo = SpliceDriver.getKryoPool().get();
        return kryo.readObject(input,TaskStatus.class);
        } finally {
        	if (kryo != null)
        		SpliceDriver.getKryoPool().returnInstance(kryo);
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
//        this.errorMessage = error.getMessage();
//        this.errorCode = Exceptions.getErrorCode(error);
//        if (error instanceof DoNotRetryIOException) {
//            final String message = error.getMessage();
//            if (message != null && message.contains("transaction") && message.contains("is not ACTIVE. State is ERROR")) {
//                this.shouldRetry = true;
//            } else {
//                this.shouldRetry = Exceptions.shouldRetry(error);
//            }
//        } else {
//            this.shouldRetry = Exceptions.shouldRetry(error);
//        }
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
        out.writeBoolean(txnId !=null);
        if(txnId !=null)
            out.writeUTF(txnId);
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

        if(in.readBoolean())
            txnId = in.readUTF();
    }

    public void attachListener(StatusListener listener) {
        this.listeners.add(listener);
        listener.statusChanged(null,status.get(),this);
    }

    public void detachListener(StatusListener listener){
        this.listeners.remove(listener);
    }

}
