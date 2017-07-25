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
 *
 */

package com.splicemachine.derby.iapi.sql.execute;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.utils.Pair;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Created by dgomezferro on 12/07/2017.
 */
public class OperationManagerImpl implements OperationManager {
    private ConcurrentMap<UUID, Pair<SpliceOperation, Thread>> operations = new ConcurrentHashMap();

    public UUID registerOperation(SpliceOperation operation, Thread executingThread) {
        UUID uuid = UUID.randomUUID();
        operations.put(uuid, new Pair<>(operation, executingThread));
        return uuid;
    }

    public void unregisterOperation(UUID uuid) {
        operations.remove(uuid);
    }

    public List<Pair<UUID, SpliceOperation>> runningOperations(String userId) {
        List<Pair<UUID, SpliceOperation>> result = new ArrayList<>(operations.size());
        for (Map.Entry<UUID, Pair<SpliceOperation, Thread>> entry : operations.entrySet()) {
            Activation activation = entry.getValue().getFirst().getActivation();
            String runningUserId = activation.getLanguageConnectionContext().getCurrentUserId(activation);
            if (userId == null || userId.equals(runningUserId))
                result.add(new Pair<>(entry.getKey(), entry.getValue().getFirst()));
        }
        return result;
    }

    public boolean killOperation(UUID uuid, String userId) throws StandardException {
        Pair<SpliceOperation, Thread> pair = operations.get(uuid);
        if (pair == null)
            return false;
        Activation activation = pair.getFirst().getActivation();
        String databaseOwner = activation.getLanguageConnectionContext().getDataDictionary().getAuthorizationDatabaseOwner();
        String runningUserId = activation.getLanguageConnectionContext().getCurrentUserId(activation);

        if (!userId.equals(databaseOwner) && !userId.equals(runningUserId))
            throw StandardException.newException(SQLState.AUTH_NO_PERMISSION_FOR_KILLING_OPERATION, userId, uuid.toString());

        pair.getFirst().kill();
        pair.getSecond().interrupt();
        return true;
    }
}
