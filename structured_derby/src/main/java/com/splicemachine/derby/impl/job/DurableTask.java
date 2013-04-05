package com.splicemachine.derby.impl.job;

import com.splicemachine.derby.impl.job.coprocessor.TaskStatus;
import com.splicemachine.job.Status;
import com.splicemachine.job.Task;

import java.io.*;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;

/**
 * @author Scott Fines
 * Created on: 4/4/13
 */
public abstract class DurableTask implements Task,Externalizable {
    protected TaskStatus status;
    protected String taskId;

    protected DurableTask(String taskId) {
        this.status = new TaskStatus(Status.PENDING,null);
        this.taskId = taskId;
    }

    @Override
    public void markStarted() throws ExecutionException, CancellationException {
        status.setStatus(Status.EXECUTING);
        updateStatus(true);
    }

    public abstract void updateStatus(boolean cancelOnError) throws ExecutionException,CancellationException;

    @Override
    public void markCompleted() throws ExecutionException {
        status.setStatus(Status.COMPLETED);
        updateStatus(false);
    }

    @Override
    public void markFailed(Throwable error) throws ExecutionException {
        status.setError(error);
        status.setStatus(Status.FAILED);
        updateStatus(false);
    }

    @Override
    public void markCancelled() throws ExecutionException {
        //only update the status if the task is actually running
        switch (status.getStatus()) {
            case FAILED:
            case COMPLETED:
            case CANCELLED:
                return;
        }
        status.setStatus(Status.CANCELLED);
        updateStatus(false);
    }

    @Override
    public boolean isCancelled() throws ExecutionException {
        return status.getStatus()==Status.CANCELLED;
    }

    @Override
    public String getTaskId() {
        return taskId;
    }

    @Override
    public TaskStatus getTaskStatus() {
        return status;
    }

    public void setTaskId(String taskId){
        this.taskId = taskId;
    }

    public byte[] statusToBytes() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutput output = new ObjectOutputStream(baos);
        output.writeObject(status);
        output.flush();
        return baos.toByteArray();
    }

    protected void statusFromBytes(byte[] bytes) throws IOException, ClassNotFoundException {
        ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
        ObjectInput input = new ObjectInputStream(bais);
        status = (TaskStatus)input.readObject();
    }
}
