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

package com.splicemachine.si.api.server;

import com.splicemachine.kvpair.KVPair;
import com.splicemachine.si.api.rollforward.RollForward;
import com.splicemachine.si.api.txn.TxnView;
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
                                    TxnView txn,
                                    ConstraintChecker constraintChecker) throws IOException;

    MutationStatus[] processKvBatch(Partition table,
                                    RollForward rollForward,
                                    byte[] defaultFamilyBytes,
                                    byte[] packedColumnBytes,
                                    Collection<KVPair> toProcess,
                                    TxnView txn,
                                    ConstraintChecker constraintChecker,
                                    boolean skipConflictDetection,
                                    boolean skipWAL, boolean rollforward) throws IOException;

    void setDefaultConstraintChecker(ConstraintChecker constraintChecker);
}
