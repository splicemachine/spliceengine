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
    public static final MOpStatusFactory INSTANCE = new MOpStatusFactory();
    private final ConstraintChecker noOpChecker;

    private MOpStatusFactory(){
        this.noOpChecker = new NoOpConstraintChecker(this);
    }

    @Override
    public boolean processPutStatus(MutationStatus operationStatus) throws IOException{
        if(operationStatus.isFailed()) {
            throw operationStatus.hasException() ? operationStatus.getException() : new IOException(operationStatus.errorMessage());
        }
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
        if (t instanceof IOException) {
            return MOperationStatus.failure((IOException) t);
        }
        return MOperationStatus.failure(t.getMessage());
    }

    @Override
    public ConstraintChecker getNoOpConstraintChecker(){
        return noOpChecker;
    }
}
