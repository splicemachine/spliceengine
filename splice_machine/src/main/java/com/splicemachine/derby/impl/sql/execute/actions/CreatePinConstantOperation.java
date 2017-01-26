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
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.services.io.FormatableBitSet;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.depend.DependencyManager;
import com.splicemachine.db.iapi.sql.dictionary.*;
import com.splicemachine.db.iapi.sql.execute.ConstantAction;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.db.impl.services.uuid.BasicUUID;
import com.splicemachine.ddl.DDLMessage;
import com.splicemachine.derby.ddl.DDLUtils;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.impl.sql.execute.operations.ScanOperation;
import com.splicemachine.derby.impl.sql.execute.pin.DistributedPopulatePinJob;
import com.splicemachine.derby.impl.store.access.SpliceTransactionManager;
import com.splicemachine.derby.impl.store.access.base.SpliceConglomerate;
import com.splicemachine.derby.stream.iapi.DistributedDataSetProcessor;
import com.splicemachine.derby.stream.iapi.ScanSetBuilder;
import com.splicemachine.derby.stream.iapi.ScopeNamed;
import com.splicemachine.derby.stream.utils.StreamUtils;
import com.splicemachine.pipeline.ErrorState;
import com.splicemachine.protobuf.ProtoUtil;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.utils.IntArrays;
import com.splicemachine.utils.SpliceLogUtils;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.log4j.Logger;

/**
 * Pin the table into columnar storage
 *
 */
public class CreatePinConstantOperation implements ConstantAction, ScopeNamed {
    private static final Logger LOG = Logger.getLogger(DDLConstantOperation.class);
    private final String schemaName;
    private final String tableName;

    // CONSTRUCTORS
    /**
     * 	Make the ConstantAction to create an index.
     *
     * @param schemaName	                schema that table (and index)
     *                                      lives in.
     * @param tableName	                    Name of table the index will be on
     *                                      associated with the index.
     */

    @SuppressFBWarnings(value = "EI_EXPOSE_REP2",justification = "Intentional")
    public CreatePinConstantOperation(
            String			schemaName,
            String			tableName) {
        SpliceLogUtils.trace(LOG, "CreatePinConstantOperation for schema table pattern %s.%s",schemaName,tableName);
        this.schemaName = schemaName;
        this.tableName = tableName;
    }

    /**
     *
     * Print out string representation
     *
     * @return
     */
    public	String	toString() {
        if (tableName == null)
            return "PIN SCHEMA " + schemaName;
        return String.format("PIN TABLE %s.%s",schemaName,tableName);
    }

    /**
     *	This is the guts of the Execution-time logic for
     *  creating an index.
     *
     *  <P>
     *  A index is represented as:
     *  <UL>
     *  <LI> ConglomerateDescriptor.
     *  </UL>
     *  No dependencies are created.
     *
     *  @see ConglomerateDescriptor
     *  @see SchemaDescriptor
     *	@see ConstantAction#executeConstantAction
     *
     * @exception StandardException		Thrown on failure
     */
    public void executeConstantAction( Activation activation ) throws StandardException {
        SpliceLogUtils.trace(LOG, "executeConstantActivation with activation %s", activation);

        LanguageConnectionContext lcc = activation.getLanguageConnectionContext();
        DataDictionary dd = lcc.getDataDictionary();
        DependencyManager dm = dd.getDependencyManager();
        TransactionController userTransaction = lcc.getTransactionExecute();
        SchemaDescriptor sd = dd.getSchemaDescriptor(schemaName, userTransaction, true);
        TableDescriptor td = dd.getTableDescriptor(tableName, sd, userTransaction);
        if (td == null) {
            throw StandardException.newException(SQLState.LANG_TABLE_NOT_FOUND_DURING_EXECUTION, tableName);
        }

        DistributedDataSetProcessor dsp = EngineDriver.driver().processorFactory().distributedProcessor();
        TxnView parentTxn = ((SpliceTransactionManager)userTransaction).getActiveStateTxn();
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
                throw ErrorState.DDL_ACTIVE_TRANSACTIONS.newException("Add Pin ()",
                        e.getMessage());
            }
            throw e;
        }

        // Change the table name of the table descriptor
        td.setColumnSequence(td.getColumnSequence()+1);

        //Mark the table pined
        td.setPined(true);
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


        SpliceConglomerate conglomerate = (SpliceConglomerate) ((SpliceTransactionManager) activation.getTransactionController()).findConglomerate(td.getHeapConglomerateId());
        int[] baseColumnMap = IntArrays.count(conglomerate.getFormat_ids().length);

        FormatableBitSet accessedCols = new FormatableBitSet(conglomerate.getFormat_ids().length);
        for (int i = 0; i < conglomerate.getFormat_ids().length; i++) {
            accessedCols.set(i);
        }

        FormatableBitSet accessedKeyCols = new FormatableBitSet(conglomerate.getColumnOrdering().length);
        for (int i = 0; i < conglomerate.getColumnOrdering().length; i++) {
            accessedKeyCols.set(i);
        }
        ScanSetBuilder<LocatedRow> builder = dsp.newScanSet(null,Long.toString(td.getHeapConglomerateId()));
            builder.tableDisplayName(tableName)
                .transaction(parentTxn)
                .scan(DDLUtils.createFullScan())
                .keyColumnEncodingOrder(conglomerate.getColumnOrdering())
                .reuseRowLocation(false)
                .keyColumnSortOrder(conglomerate.getAscDescInfo())
                .baseColumnMap(baseColumnMap)
                .keyColumnTypes(ScanOperation.getKeyFormatIds(conglomerate.getColumnOrdering(),
                        conglomerate.getFormat_ids()
                ))
                .keyDecodingMap(ScanOperation.getKeyDecodingMap(accessedKeyCols,
                        conglomerate.getColumnOrdering(),
                        baseColumnMap
                ))
                    .rowDecodingMap(ScanOperation.getRowDecodingMap(accessedKeyCols,conglomerate.getColumnOrdering(),baseColumnMap))
                .accessedKeyColumns(accessedKeyCols)
                .template(td.getEmptyExecRow())
                .escaped(td.getEscaped())
                .lines(td.getLines())
                .delimited(td.getDelimited())
                .location(td.getLocation())
                .storedAs(td.getStoredAs())
                .compression(td.getCompression());
        String scope = this.getScopeName();
        String prefix = StreamUtils.getScopeString(this);
        String userId = activation.getLanguageConnectionContext().getCurrentUserId(activation);
        String jobGroup = userId + " <" +parentTxn.getTxnId() +">";
        try {
            EngineDriver.driver().getOlapClient().execute(new DistributedPopulatePinJob(builder, scope, jobGroup, prefix, conglomerate.getContainerid()));
        } catch (Exception e) {
            throw StandardException.plainWrapException(e);
        }
    }

    @Override
    public String getScopeName() {
        return "CreatePinConstantOperation";
    }
}
