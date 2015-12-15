package com.splicemachine.storage;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.regionserver.OperationStatus;

/**
 * @author Scott Fines
 *         Date: 12/16/15
 */
public class HMutationStatus implements MutationStatus{
    private OperationStatus delegate;

    public HMutationStatus(){
    }

    public HMutationStatus(OperationStatus delegate){
        this.delegate=delegate;
    }

    public void set(OperationStatus delegate){
        this.delegate = delegate;
    }

    @Override
    public boolean isSuccess(){
        return delegate.getOperationStatusCode()==HConstants.OperationStatusCode.SUCCESS;
    }

    @Override
    public boolean isFailed(){
        return !isSuccess() &&!isNotRun();
    }

    @Override
    public boolean isNotRun(){
        return delegate.getOperationStatusCode()==HConstants.OperationStatusCode.NOT_RUN;
    }

    @Override
    public String errorMessage(){
        return delegate.getExceptionMsg();
    }

    @Override
    public MutationStatus getClone(){
        return new HMutationStatus(delegate);
    }
}
