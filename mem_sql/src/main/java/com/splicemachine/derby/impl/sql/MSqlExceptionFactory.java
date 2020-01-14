/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
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

package com.splicemachine.derby.impl.sql;

import com.splicemachine.SqlExceptionFactory;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.si.api.data.ExceptionFactory;
import com.splicemachine.si.api.txn.Txn;

import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 1/28/16
 */
public class MSqlExceptionFactory implements SqlExceptionFactory{
    private final ExceptionFactory delegate;

    public MSqlExceptionFactory(ExceptionFactory delegate){
        this.delegate=delegate;
    }

    @Override
    public IOException asIOException(StandardException se){
        return new IOException(se);
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
    public IOException connectionClosingException() { return delegate.connectionClosingException(); }

    @Override
    public boolean allowsRetry(Throwable error){
        return delegate.allowsRetry(error);
    }
}
