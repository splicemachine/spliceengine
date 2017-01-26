/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
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
