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
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.ResultSet;
import com.splicemachine.derby.stream.iapi.DataSetProcessor;
import com.splicemachine.utils.Pair;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.Date;
/**
 * Created by dgomezferro on 12/07/2017.
 */
public class OperationManagerImpl implements OperationManager {
    private static final Logger LOG = Logger.getLogger(OperationManagerImpl.class);
    private ConcurrentMap<UUID, RunningOperation> operations = new ConcurrentHashMap();
    private ConcurrentMap<String, RunningOperation> drdaOperations = new ConcurrentHashMap();

    public UUID registerOperation(SpliceOperation operation, Thread executingThread, Date submittedTime, DataSetProcessor.Type engine, String rdbIntTkn) {
        UUID uuid = UUID.randomUUID();
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

    public List<Pair<UUID, RunningOperation>> runningOperations(String userId) {
        List<Pair<UUID, RunningOperation>> result = new ArrayList<>(operations.size());
        for (Map.Entry<UUID, RunningOperation> entry : operations.entrySet()) {
            Activation activation = entry.getValue().getOperation().getActivation();
            String runningUserId = activation.getLanguageConnectionContext().getCurrentUserId(activation);
            if (userId == null || userId.equals(runningUserId))
                result.add(new Pair<>(entry.getKey(), entry.getValue()));
        }
        return result;
    }

    public boolean killDRDAOperation(String uuid, String userId) throws StandardException {
        RunningOperation op = drdaOperations.get(uuid);
        if (op == null)
            return false;
        Activation activation = op.getOperation().getActivation();
        String databaseOwner = activation.getLanguageConnectionContext().getDataDictionary().getAuthorizationDatabaseOwner();
        String runningUserId = activation.getLanguageConnectionContext().getCurrentUserId(activation);
        List<String> groupuserlist = activation.getLanguageConnectionContext().getCurrentGroupUser(activation);

        if (!userId.equals(databaseOwner) && !userId.equals(runningUserId)) {
            if (groupuserlist != null) {
                if (!groupuserlist.contains(databaseOwner) && !groupuserlist.contains(runningUserId))
                    throw StandardException.newException(SQLState.AUTH_NO_PERMISSION_FOR_KILLING_OPERATION, userId, uuid);
            } else
                throw StandardException.newException(SQLState.AUTH_NO_PERMISSION_FOR_KILLING_OPERATION, userId, uuid);
        }

        drdaOperations.remove(uuid);
        operations.remove(op.getUuid());
        
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

    public boolean killOperation(UUID uuid, String userId) throws StandardException {
        RunningOperation op = operations.get(uuid);
        if (op == null)
            return false;
        Activation activation = op.getOperation().getActivation();
        String databaseOwner = activation.getLanguageConnectionContext().getDataDictionary().getAuthorizationDatabaseOwner();
        String runningUserId = activation.getLanguageConnectionContext().getCurrentUserId(activation);
        List<String> groupuserlist = activation.getLanguageConnectionContext().getCurrentGroupUser(activation);

        if (!userId.equals(databaseOwner) && !userId.equals(runningUserId)) {
            if (groupuserlist != null) {
                if (!groupuserlist.contains(databaseOwner) && !groupuserlist.contains(runningUserId))
                    throw StandardException.newException(SQLState.AUTH_NO_PERMISSION_FOR_KILLING_OPERATION, userId, uuid.toString());
            } else
                throw StandardException.newException(SQLState.AUTH_NO_PERMISSION_FOR_KILLING_OPERATION, userId, uuid.toString());
        }

        unregisterOperation(uuid);
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
