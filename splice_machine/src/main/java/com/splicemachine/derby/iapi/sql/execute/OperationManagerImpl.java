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
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.derby.stream.iapi.DataSetProcessor;
import com.splicemachine.utils.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Created by dgomezferro on 12/07/2017.
 */
public class OperationManagerImpl implements OperationManager {
    private static final Logger LOG = LogManager.getLogger(OperationManagerImpl.class);
    private ConcurrentMap<UUID, RunningOperation> operations = new ConcurrentHashMap();
    private ConcurrentMap<String, RunningOperation> drdaOperations = new ConcurrentHashMap();

    @Override
    public RunningOperation getRunningOperation(UUID uuid)
    {
        return operations.get(uuid);
    }

    public UUID registerOperation(SpliceOperation operation, Thread executingThread, Date submittedTime, DataSetProcessor.Type engine, String rdbIntTkn) {
        Random rnd = ThreadLocalRandom.current();
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

    private <T> List<Pair<String, RunningOperation>> runningOperationsT(
            String userId, String filterUuid, ConcurrentMap<T, RunningOperation> operations)
    {
        Stream<Map.Entry<T, RunningOperation>> stream = operations.entrySet().stream();
        if(userId != null)
            stream = stream.filter( entry -> entry.getValue().isFromUser(userId) );
        if(filterUuid != null && !filterUuid.isEmpty())
            stream = stream.filter( entry -> entry.getKey().toString().equals(filterUuid));
        return stream.map( entry -> new Pair<>(entry.getKey().toString(), entry.getValue()))
                .collect(Collectors.toList());
    }

    public List<Pair<String, RunningOperation>> runningOperations(String userId) {
        return runningOperationsT(userId, null, operations);
    }

    public List<Pair<String, RunningOperation>> runningOperationsDRDA(String userId, String drdaToken) {
        return runningOperationsT(userId, drdaToken, drdaOperations);
    }

    public boolean killDRDAOperation(String uuid, String userId) throws StandardException {
        return kill(drdaOperations.get(uuid), "DRDA " + uuid, userId);
    }

    public boolean killOperation(UUID uuid, String userId) throws StandardException {
        return kill(operations.get(uuid), uuid.toString(), userId);
    }

    private boolean kill(RunningOperation op, String strUUID, String userId) throws StandardException {
        if (op == null)
            return false;
        Activation activation = op.getOperation().getActivation();
        LanguageConnectionContext lcc = activation.getLanguageConnectionContext();
        String databaseOwner = lcc.getCurrentDatabase().getAuthorizationId();
        String runningUserId = lcc.getCurrentUserId(activation);
        List<String> groupuserlist = lcc.getCurrentGroupUser(activation);

        if (!userId.equals(databaseOwner) && !userId.equals(runningUserId)) {
            if (groupuserlist != null) {
                if (!groupuserlist.contains(databaseOwner) && !groupuserlist.contains(runningUserId))
                    throw StandardException.newException(SQLState.AUTH_NO_PERMISSION_FOR_KILLING_OPERATION, userId, strUUID);
            } else
                throw StandardException.newException(SQLState.AUTH_NO_PERMISSION_FOR_KILLING_OPERATION, userId, strUUID);
        }

        unregisterOperation(op.getUuid());

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
