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

package com.splicemachine.derby.impl.storage;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.dictionary.*;
import com.splicemachine.ddl.DDLMessage;
import com.splicemachine.derby.iapi.sql.olap.OlapStatus;
import com.splicemachine.derby.impl.store.access.SpliceTransactionManager;
import com.splicemachine.derby.jdbc.SpliceTransactionResourceImpl;

import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.Callable;

/**
 * Created by jyuan on 2/5/18.
 */
public class CheckTableJob implements Callable<Void> {

    private final OlapStatus jobStatus;
    private final DistributedCheckTableJob request;
    private long heapConglomId;
    private TableDescriptor td;

    public CheckTableJob(DistributedCheckTableJob request,OlapStatus jobStatus) {
        this.jobStatus = jobStatus;
        this.request = request;
    }

    @Override
    public Void call() throws Exception {
        init();

        if(!jobStatus.markRunning()){
            //the client has already cancelled us or has died before we could get started, so stop now
            return null;
        }

        CheckTableResult checkTableResult = new CheckTableResult();
        Map<String, List<String>> errors = CheckTableUtils.checkTable(request.schemaName, request.tableName, td, request.tentativeIndexList,
                heapConglomId, true, request.fix, request.isSystemTable, request.txn,
                request.ah.getActivation(), request.jobGroup);

        if (errors.size() > 0) {
            checkTableResult.setResults(errors);
        }

        jobStatus.markCompleted(checkTableResult);
        return null;
    }

    private void init() throws StandardException, SQLException{
        DDLMessage.Table table = request.tentativeIndexList.get(0).getTable();
        heapConglomId = table.getConglomerate();
        LanguageConnectionContext lcc = request.ah.getActivation().getLanguageConnectionContext();
        SpliceTransactionManager tc = (SpliceTransactionManager)lcc.getTransactionExecute();
        DataDictionary dd =lcc.getDataDictionary();

        SpliceTransactionResourceImpl transactionResource = new SpliceTransactionResourceImpl();
        boolean prepared = false;
        try {
            prepared=transactionResource.marshallTransaction(request.txn);
            SchemaDescriptor sd = dd.getSchemaDescriptor(request.schemaName, tc, true);
            td = dd.getTableDescriptor(request.tableName, sd, tc);
        }
        finally {
            if (prepared)
                transactionResource.close();
        }
    }
}
