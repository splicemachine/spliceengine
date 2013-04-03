package com.splicemachine.derby.impl.hbase.job.coprocessor;

import com.splicemachine.derby.hbase.job.Status;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;

/**
 * @author Scott Fines
 *         Created on: 4/3/13
 */
public class TaskStatus {

    private Status status;
    private Throwable error;

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

    public static TaskStatus fromBytes(byte[] bytes) {
        ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
        try{
            ObjectInput in = new ObjectInputStream(bais);
            Status status = Status.valueOf(in.readUTF());
            Throwable error = null;
            if(in.readBoolean()){
                error = (Throwable)in.readObject();
            }
            return new TaskStatus(status,error);
        } catch (ClassNotFoundException e) {
            //should never happen
            throw new RuntimeException(e);
        } catch (IOException e) {
            //should never happen
            throw new RuntimeException(e);
        }
    }

    public static TaskStatus cancelled(){
        return new TaskStatus(Status.CANCELLED,null);
    }

}
