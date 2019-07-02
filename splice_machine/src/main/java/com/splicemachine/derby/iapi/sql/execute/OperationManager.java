/*
 * Copyright (c) 2012 - 2019 Splice Machine, Inc.
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
 *
 */

package com.splicemachine.derby.iapi.sql.execute;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.derby.stream.iapi.DataSetProcessor;
import com.splicemachine.utils.Pair;

import java.util.Date;
import java.util.List;
import java.util.UUID;
/**
 * Created by dgomezferro on 12/07/2017.
 */
public interface OperationManager {
    /**
     * Register an operation into the manager so that it can be killed
     * @param operation Operation that started execution
     * @param executingThread Thread that's responsible for the operation execution (oen DRDAConnThread)
     * @param submittedTime Operation submitted Time
     * @param engine Running engine CONTROL or SPARK
     * @param rdbIntTkn DRDA interruption token
     * @return UUID given to the operation that can be used for unregistering or killing it at a later time
     */
    UUID registerOperation(SpliceOperation operation, Thread executingThread, Date submittedTime, DataSetProcessor.Type engine, String rdbIntTkn);


    /**
     * Unregister an operation that has been closed
     * @param uuid Unique identifier provided by the registerOperation() method
     */
    void unregisterOperation(UUID uuid);

    /**
     * Get running operations for the given user, or all running operations if userId is null
     * @param userId user for which we want the operations, or null for all operations
     * @return list of running operations for the given user
     */
    List<Pair<UUID, RunningOperation>> runningOperations(String userId);

    /**
     * Kill a running operation. Only the user that started the operation might kill it. The database owner can kill any
     * operation. We close the operation and interrupt the executingThread associated with the operation. This might
     * affect some other operation that could be running on the same thread.
     *
     * @param uuid Unique identifier provided by the registerOperation() method
     * @param userId user that's trying to kill the operation, or null if the user is the database owner
     * @return true if the operation has been killed, false otherwise (if we don't find it)
     */
    boolean killOperation(UUID uuid, String userId) throws StandardException;


    /**
     * Kill a running operation. Only the user that started the operation might kill it. The database owner can kill any
     * operation. We close the operation and interrupt the executingThread associated with the operation. This might
     * affect some other operation that could be running on the same thread.
     *
     * @param rdbIntTkn Unique identifier for the DRDA connection running the operation
     * @param userId user that's trying to kill the operation, or null if the user is the database owner
     * @return true if the operation has been killed, false otherwise (if we don't find it)
     */
    boolean killDRDAOperation(String rdbIntTkn, String userId) throws StandardException;
}
