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
 *
 */

package com.splicemachine.derby.iapi.sql.execute;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.ResultSet;
import com.splicemachine.derby.stream.iapi.DataSetProcessor;
import com.splicemachine.utils.Pair;
import org.apache.log4j.Logger;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Created by dgomezferro on 12/07/2017.
 */
public class OperationManagerImpl implements OperationManager {
    private static final Logger LOG = Logger.getLogger(OperationManagerImpl.class);
    private ConcurrentMap<UUID, RunningOperation> operations = new ConcurrentHashMap();
    private ConcurrentMap<String, RunningOperation> drdaOperations = new ConcurrentHashMap();

    @Override
    public RunningOperation getRunningOperation(UUID uuid)
    {
        return operations.get(uuid);
    }

    public UUID registerOperation(SpliceOperation operation, Thread executingThread, Date submittedTime, DataSetProcessor.Type engine, String rdbIntTkn) {
        Random rnd = ThreadLocalRandom.current();
        // Question is if we really need two different UUIDs here.
        // seems like we don't always have a rdbIntTkn, but shouldn't we maybe only then create a custom UUID?
        // like if(rdbIntTkn == null) rdbIntTkn = new UUID(rnd.nextLong(), rnd.nextLong()).toString(); ?
        // HOWEVER, it might be that rdbIntTkn is not really unique, but only per connection?
        UUID uuid = new UUID(rnd.nextLong(), rnd.nextLong());
        RunningOperation ro = new RunningOperation(operation, executingThread, submittedTime, engine, uuid, rdbIntTkn);
        operations.put(uuid, ro);
        if (rdbIntTkn != null)
            drdaOperations.put(rdbIntTkn, ro);
        return uuid;
    }

    public void unregisterOperation(UUID uuid) {
        RunningOperation ro = operations.remove(uuid);
        if (ro != null && ro.getRdbIntTkn() != null)
            drdaOperations.remove(ro.getRdbIntTkn());
    }

    public List<Pair<String, RunningOperation>> runningOperations(String userId) {
        List<Pair<String, RunningOperation>> result = new ArrayList<>(operations.size());
        for (Map.Entry<UUID, RunningOperation> entry : operations.entrySet()) {
            Activation activation = entry.getValue().getOperation().getActivation();
            String runningUserId = activation.getLanguageConnectionContext().getCurrentUserId(activation);
            if (userId == null || userId.equals(runningUserId))
                result.add(new Pair<>(entry.getKey().toString(), entry.getValue()));
        }
        return result;
    }

    public List<Pair<String, RunningOperation>> runningOperationsDRDA(String userId) {
        List<Pair<String, RunningOperation>> result = new ArrayList<>(drdaOperations.size());
        for (Map.Entry<String, RunningOperation> entry : drdaOperations.entrySet()) {
            Activation activation = entry.getValue().getOperation().getActivation();
            String runningUserId = activation.getLanguageConnectionContext().getCurrentUserId(activation);
            if (userId == null || userId.equals(runningUserId))
                result.add(new Pair<>(entry.getKey(), entry.getValue()));
        }
        return result;
    }

    public boolean killDRDAOperation(String uuid, String userId) throws StandardException {
        return kill(drdaOperations.get(uuid), uuid, userId);
    }

    public boolean killOperation(UUID uuid, String userId) throws StandardException {
        return kill(operations.get(uuid), uuid.toString(), userId);
    }

    public boolean kill(RunningOperation op, String strUUID, String userId) throws StandardException {
        if (op == null)
            return false;
        Activation activation = op.getOperation().getActivation();
        String databaseOwner = activation.getLanguageConnectionContext().getDataDictionary().getAuthorizationDatabaseOwner();
        String runningUserId = activation.getLanguageConnectionContext().getCurrentUserId(activation);
        List<String> groupuserlist = activation.getLanguageConnectionContext().getCurrentGroupUser(activation);

        if (!userId.equals(databaseOwner) && !userId.equals(runningUserId)) {
            if (groupuserlist != null) {
                if (!groupuserlist.contains(databaseOwner) && !groupuserlist.contains(runningUserId))
                    throw StandardException.newException(SQLState.AUTH_NO_PERMISSION_FOR_KILLING_OPERATION, userId, strUUID);
            } else
                throw StandardException.newException(SQLState.AUTH_NO_PERMISSION_FOR_KILLING_OPERATION, userId, strUUID);
        }

        operations.remove(op.getUuid());
        if (op.getRdbIntTkn() != null) {
            drdaOperations.remove(op.getRdbIntTkn());
        }

        op.getOperation().kill();
        op.getThread().interrupt();
        ResultSet rs=activation.getResultSet();
        if (rs!=null && !rs.isClosed()) {
            try {
                rs.close();
            } catch (Exception e) {
                LOG.warn("Exception while closing ResultSet, probably due to forcefully killing the operation", e);
            }
        }

        return true;
    }
}
