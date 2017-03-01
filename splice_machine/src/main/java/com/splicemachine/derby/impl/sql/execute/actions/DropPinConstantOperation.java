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
 */

package com.splicemachine.derby.impl.sql.execute.actions;

import com.splicemachine.EngineDriver;
import com.splicemachine.db.catalog.UUID;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.StatementType;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.depend.DependencyManager;
import com.splicemachine.db.iapi.sql.dictionary.*;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.db.impl.services.uuid.BasicUUID;
import com.splicemachine.ddl.DDLMessage;
import com.splicemachine.derby.ddl.DDLUtils;
import com.splicemachine.derby.impl.sql.execute.pin.DistributedPopulatePinJob;
import com.splicemachine.derby.impl.sql.execute.pin.RemoteDropPinJob;
import com.splicemachine.derby.impl.store.access.SpliceTransactionManager;
import com.splicemachine.derby.stream.iapi.DistributedDataSetProcessor;
import com.splicemachine.pipeline.ErrorState;
import com.splicemachine.protobuf.ProtoUtil;
import com.splicemachine.si.api.txn.TxnView;


/**
 * This class describes actions that are ALWAYS performed for a DROP PIN Statement at Execution time.
 */
public class DropPinConstantOperation extends DDLSingleTableConstantOperation {
    private final long conglomerateNumber;
    private final String fullTableName;
    private final SchemaDescriptor sd;
    private final boolean cascade;

    /**
     * Make the ConstantAction for a DROP TABLE statement.
     *
     * @param fullTableName      Fully qualified table name
     * @param tableName          Table name.
     * @param sd                 Schema that table lives in.
     * @param conglomerateNumber Conglomerate number for heap
     * @param tableId            UUID for table
     * @param behavior           drop behavior: RESTRICT, CASCADE or default
     */
    public DropPinConstantOperation(String fullTableName, String tableName, SchemaDescriptor sd,
                                    long conglomerateNumber, UUID tableId, int behavior) {
        super(tableId);
        this.fullTableName = fullTableName;
        this.sd = sd;
        this.conglomerateNumber = conglomerateNumber;
        this.cascade = (behavior == StatementType.DROP_CASCADE);
        if (SanityManager.DEBUG)
            SanityManager.ASSERT(sd != null, "SchemaDescriptor is null");
    }

    @Override
    public String toString() {
        return "DROP PIN " + fullTableName;
    }

    /**
     * This is the guts of the Execution-time logic for DROP TABLE.
     */
    @Override
    public void executeConstantAction(Activation activation) throws StandardException {
        LanguageConnectionContext lcc = activation.getLanguageConnectionContext();
        DataDictionary dd = lcc.getDataDictionary();
        DependencyManager dm = dd.getDependencyManager();
        TransactionController userTransaction = lcc.getTransactionExecute();

        /* Get the table descriptor. */
        TableDescriptor td = dd.getTableDescriptor(tableId);
        activation.setDDLTableDescriptor(td);
        if (td == null) {
            throw StandardException.newException(SQLState.LANG_TABLE_NOT_FOUND_DURING_EXECUTION, fullTableName);
        }
        if(!td.isPinned()){
            throw StandardException.newException(SQLState.TABLE_NOT_PINNED, fullTableName);
        }

        /*
        ** Inform the data dictionary that we are about to write to it.
        ** There are several calls to data dictionary "get" methods here
        ** that might be done in "read" mode in the data dictionary, but
        ** it seemed safer to do this whole operation in "write" mode.
        **
        ** We tell the data dictionary we're done writing at the end of
        ** the transaction.
        */
        dd.startWriting(lcc);
        // Drop the table and then recreate it with the pin marked.
        try {
            dd.dropTableDescriptor(td,sd,userTransaction);
        } catch (StandardException e) {
            if (ErrorState.WRITE_WRITE_CONFLICT.getSqlState().equals(e.getSQLState())) {
                throw ErrorState.DDL_ACTIVE_TRANSACTIONS.newException("Drop Pin ()",
                        e.getMessage());
            }
            throw e;
        }

        // Change the table name of the table descriptor
        td.setColumnSequence(td.getColumnSequence()+1);

        //Mark the table not pinned
        td.setPinned(false);
        		    /* Prepare all dependents to invalidate.  (This is their chance
		     * to say that they can't be invalidated.  For example, an open
		     * cursor referencing a table/view that the user is attempting to
		     * alter.) If no one objects, then invalidate any dependent objects.
		     */
        dm.invalidateFor(td, DependencyManager.ALTER_TABLE, lcc);

        TransactionController tc = lcc.getTransactionExecute();

        DDLMessage.DDLChange ddlChange = ProtoUtil.createAlterTable(((SpliceTransactionManager) tc).getActiveStateTxn().getTxnId(),
                (BasicUUID) td.getUUID());
        // Run Remotely
        tc.prepareDataDictionaryChange(DDLUtils.notifyMetadataChange(ddlChange));


        // Save the TableDescriptor off in the Activation
        activation.setDDLTableDescriptor(td);


        dd.addDescriptor(td,sd,DataDictionary.SYSTABLES_CATALOG_NUM,false,userTransaction);

        long heapId = td.getHeapConglomerateId();
        try {
            EngineDriver.driver().getOlapClient().execute(new RemoteDropPinJob(heapId));
        } catch (Exception e) {
            throw StandardException.plainWrapException(e);
        }

    }

    public String getScopeName() {
        return String.format("Drop Table %s", fullTableName);
    }

}
