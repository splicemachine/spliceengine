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
