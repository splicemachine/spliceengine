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

package com.splicemachine.si.api.server;

import com.splicemachine.kvpair.KVPair;
import com.splicemachine.si.api.readresolve.RollForward;
import com.splicemachine.si.api.txn.Transaction;
import com.splicemachine.storage.DataPut;
import com.splicemachine.storage.MutationStatus;
import com.splicemachine.storage.Partition;

import java.io.IOException;
import java.util.Collection;

/**
 * The primary interface to the transaction module. This interface has the most burdensome generic signature so it is
 * only exposed in places where it is needed.
 */
public interface Transactor{

    boolean processPut(Partition table,RollForward rollForwardQueue,DataPut put) throws IOException;

    MutationStatus[] processPutBatch(Partition table,RollForward rollForwardQueue,DataPut[] mutations) throws IOException;

    MutationStatus[] processKvBatch(Partition table,
                                    RollForward rollForward,
                                    byte[] defaultFamilyBytes,
                                    byte[] packedColumnBytes,
                                    Collection<KVPair> toProcess,
                                    long transactionId,
                                    ConstraintChecker constraintChecker) throws IOException;

    MutationStatus[] processKvBatch(Partition table,
                                    RollForward rollForward,
                                    byte[] defaultFamilyBytes,
                                    byte[] packedColumnBytes,
                                    Collection<KVPair> toProcess,
                                    Transaction txn,
                                    ConstraintChecker constraintChecker) throws IOException;

}
