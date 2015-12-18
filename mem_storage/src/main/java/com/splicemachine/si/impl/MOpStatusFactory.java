package com.splicemachine.si.impl;

import com.splicemachine.si.api.data.OperationStatusFactory;
import com.splicemachine.si.api.server.ConstraintChecker;
import com.splicemachine.si.impl.data.NoOpConstraintChecker;
import com.splicemachine.storage.MOperationStatus;
import com.splicemachine.storage.MutationStatus;

import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 12/16/15
 */
public class MOpStatusFactory implements OperationStatusFactory{
    private final ConstraintChecker noOpChecker;

    public MOpStatusFactory(){
        this.noOpChecker = new NoOpConstraintChecker(this);
    }

    @Override
    public boolean processPutStatus(MutationStatus operationStatus) throws IOException{
        if(operationStatus.isFailed())
            throw new IOException(operationStatus.errorMessage());
        else return true;
    }

    @Override
    public MutationStatus getCorrectStatus(MutationStatus status,MutationStatus oldStatus){
        if(oldStatus.isSuccess()) return status;
        else return oldStatus;
    }

    @Override
    public MutationStatus success(){
        return MOperationStatus.success();
    }

    @Override
    public MutationStatus notRun(){
        return MOperationStatus.notRun();
    }

    @Override
    public MutationStatus failure(String message){
        return MOperationStatus.failure(message);
    }

    @Override
    public MutationStatus failure(Throwable t){
        return MOperationStatus.failure(t.getMessage());
    }

    @Override
    public ConstraintChecker getNoOpConstraintChecker(){
        return noOpChecker;
    }
}
