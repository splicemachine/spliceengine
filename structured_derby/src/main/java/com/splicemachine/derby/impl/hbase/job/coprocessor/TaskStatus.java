package com.splicemachine.derby.impl.hbase.job.coprocessor;

import com.splicemachine.derby.hbase.job.Status;

import java.io.*;

/**
 * @author Scott Fines
 *         Created on: 4/3/13
 */
public class TaskStatus implements Externalizable{

    private Status status;
    private Throwable error;

    public TaskStatus(){}

    public TaskStatus(Status status, Throwable error) {
        this.status = status;
        this.error = error;
    }

    public Throwable getError(){
        return error;
    }

    public Status getStatus(){
        return status;
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

    public void setStatus(Status status) {
        this.status = status;
    }

    public void setError(Throwable error) {
        this.error = error;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeUTF(status.name());
        out.writeBoolean(error!=null);
        if(error!=null)
            out.writeObject(error);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        status = Status.valueOf(in.readUTF());
        if(in.readBoolean()){
            error = (Throwable)in.readObject();
        }
    }
}
