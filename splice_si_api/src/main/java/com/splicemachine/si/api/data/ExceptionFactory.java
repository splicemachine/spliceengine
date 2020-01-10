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

package com.splicemachine.si.api.data;

import com.splicemachine.si.api.txn.Txn;

import java.io.IOException;

/**
 *
 * Created by jleach on 12/11/15.
 */
public interface ExceptionFactory{
    IOException writeWriteConflict(long txn1,long txn2);
    IOException readOnlyModification(String message);
    IOException noSuchFamily(String message);
    IOException transactionTimeout(long tnxId);
    IOException cannotCommit(long txnId,Txn.State actualState);
    IOException cannotCommit(String message);
    IOException additiveWriteConflict();
    IOException doNotRetry(String message);

    IOException doNotRetry(Throwable t);

    IOException processRemoteException(Throwable e);

    IOException callerDisconnected(String message);

    IOException failedServer(String message);

    IOException notServingPartition(String s);

    IOException connectionClosingException();

    boolean allowsRetry(Throwable error);
}
