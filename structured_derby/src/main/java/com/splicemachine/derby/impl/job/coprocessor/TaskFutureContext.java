package com.splicemachine.derby.impl.job.coprocessor;

import com.splicemachine.job.Status;

import java.io.*;

/**
 * @author Scott Fines
 * Created on: 4/3/13
 */
public class TaskFutureContext implements Externalizable{
    private static final long serialVersionUID = 2l;
    private String taskNode;
    private byte[] taskId;
    private double estimatedCost;
    private Status status;

    public TaskFutureContext(String taskNode,byte[] taskId,double estimatedCost){
        this.taskNode = taskNode;
        this.estimatedCost = estimatedCost;
        this.taskId = taskId;
    }

    public TaskFutureContext(){}

    public String getTaskNode() {
        return taskNode;
    }

    public byte[] getTaskId(){
        return taskId;
    }

    public double getEstimatedCost() {
        return estimatedCost;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeUTF(taskNode);
        out.writeDouble(estimatedCost);
        out.writeInt(taskId.length);
        out.write(taskId);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        taskNode = in.readUTF();
        estimatedCost = in.readDouble();
        taskId = new byte[in.readInt()];
        in.readFully(taskId);
    }

}
