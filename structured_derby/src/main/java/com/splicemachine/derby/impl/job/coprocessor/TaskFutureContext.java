package com.splicemachine.derby.impl.job.coprocessor;

import com.splicemachine.job.Status;
import org.apache.hadoop.hbase.HConstants;

import java.io.*;

/**
 * @author Scott Fines
 * Created on: 4/3/13
 */
public class TaskFutureContext implements Externalizable{
    private static final long serialVersionUID =3l;
    private String taskNode;
    private byte[] taskId;
    private double estimatedCost;
    private Status status;
		private byte[] startRow;

    public TaskFutureContext(String taskNode,byte[] startRow,byte[] taskId,double estimatedCost){
        this.taskNode = taskNode;
        this.estimatedCost = estimatedCost;
        this.taskId = taskId;
				this.startRow = startRow;
    }

    public TaskFutureContext(){}

    public String getTaskNode() {
        return taskNode;
    }

		public byte[] getStartRow(){ return startRow;}
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
				out.writeBoolean(startRow!=null);
				if(startRow!=null){
						out.writeInt(startRow.length);
						out.write(startRow);
				}
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        taskNode = in.readUTF();
        estimatedCost = in.readDouble();
        taskId = new byte[in.readInt()];
        in.readFully(taskId);
				if(in.readBoolean()){
						startRow = new byte[in.readInt()];
						in.readFully(startRow);
				}else
						startRow = HConstants.EMPTY_START_ROW;
		}

}
