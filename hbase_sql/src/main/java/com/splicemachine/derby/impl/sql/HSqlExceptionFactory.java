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

package com.splicemachine.derby.impl.sql;

import com.splicemachine.SpliceDoNotRetryIOExceptionWrapping;
import com.splicemachine.SqlExceptionFactory;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.si.api.data.ExceptionFactory;
import com.splicemachine.si.api.txn.Txn;

import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 1/28/16
 */
public class HSqlExceptionFactory implements SqlExceptionFactory{
    private final ExceptionFactory delegate;

    public HSqlExceptionFactory(ExceptionFactory delegate){
        this.delegate=delegate;
    }

    @Override
    public IOException asIOException(StandardException se){
        return SpliceDoNotRetryIOExceptionWrapping.wrap(se);
    }

    @Override
    public IOException writeWriteConflict(long txn1,long txn2){
        return delegate.writeWriteConflict(txn1,txn2);
    }

    @Override
    public IOException readOnlyModification(String message){
        return delegate.readOnlyModification(message);
    }

    @Override
    public IOException noSuchFamily(String message){
        return delegate.noSuchFamily(message);
    }

    @Override
    public IOException transactionTimeout(long tnxId){
        return delegate.transactionTimeout(tnxId);
    }

    @Override
    public IOException cannotCommit(long txnId,Txn.State actualState){
        return delegate.cannotCommit(txnId,actualState);
    }

    @Override
    public IOException cannotCommit(String message){
        return delegate.cannotCommit(message);
    }

    @Override
    public IOException additiveWriteConflict(){
        return delegate.additiveWriteConflict();
    }

    @Override
    public IOException doNotRetry(String message){
        return delegate.doNotRetry(message);
    }

    @Override
    public IOException doNotRetry(Throwable t){
        return delegate.doNotRetry(t);
    }

    @Override
    public IOException processRemoteException(Throwable e){
        return delegate.processRemoteException(e);
    }

    @Override
    public IOException callerDisconnected(String message){
        return delegate.callerDisconnected(message);
    }

    @Override
    public IOException failedServer(String message){
        return delegate.failedServer(message);
    }

    @Override
    public IOException notServingPartition(String s){
        return delegate.notServingPartition(s);
    }

    @Override
    public IOException connectionClosingException(){
        return delegate.connectionClosingException();
    }

    @Override
    public boolean allowsRetry(Throwable error){
        return delegate.allowsRetry(error);
    }
}
