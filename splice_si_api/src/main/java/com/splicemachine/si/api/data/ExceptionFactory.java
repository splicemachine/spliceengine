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

package com.splicemachine.si.api.data;

import com.splicemachine.si.api.txn.Txn;

import java.io.IOException;

/**
 *
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

    boolean allowsRetry(Throwable error);
}
