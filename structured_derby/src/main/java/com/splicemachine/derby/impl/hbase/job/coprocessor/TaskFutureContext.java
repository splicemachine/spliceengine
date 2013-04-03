package com.splicemachine.derby.impl.hbase.job.coprocessor;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * @author Scott Fines
 * Created on: 4/3/13
 */
public class TaskFutureContext implements Externalizable{
    private static final long serialVersionUID = 1l;
    private String taskNode;
    private double estimatedCost;

    public String getTaskNode() {
        return taskNode;
    }

    public double getEstimatedCost() {
        return estimatedCost;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeUTF(taskNode);
        out.writeDouble(estimatedCost);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        taskNode = in.readUTF();
        estimatedCost = in.readDouble();
    }

}
