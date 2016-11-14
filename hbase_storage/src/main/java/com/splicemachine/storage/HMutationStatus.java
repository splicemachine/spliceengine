/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.splicemachine.storage;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.regionserver.OperationStatus;

import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 12/16/15
 */
public class HMutationStatus implements MutationStatus{
    private static final MutationStatus SUCCESS= new HMutationStatus(new OperationStatus(HConstants.OperationStatusCode.SUCCESS));
    private static final MutationStatus NOT_RUN= new HMutationStatus(new OperationStatus(HConstants.OperationStatusCode.NOT_RUN));
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

    @Override
    public boolean hasException() {
        return false;
    }

    @Override
    public IOException getException() {
        return null;
    }

    public static MutationStatus success(){
        //singleton instance to use when convenient
        return SUCCESS;
    }

    public static MutationStatus notRun(){
        return NOT_RUN;
    }

    public OperationStatus unwrapDelegate(){
        return delegate;
    }
}
