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

package com.splicemachine.derby.impl.sql.execute.actions;

import com.splicemachine.db.catalog.UUID;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.dictionary.DataDescriptorGenerator;
import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;
import com.splicemachine.db.iapi.sql.dictionary.SchemaDescriptor;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.db.impl.sql.execute.DDLConstantAction;
import com.splicemachine.db.shared.common.reference.SQLState;
import com.splicemachine.ddl.DDLMessage;
import com.splicemachine.derby.ddl.DDLController;
import com.splicemachine.derby.ddl.DDLDriver;
import com.splicemachine.derby.impl.store.access.SpliceTransactionManager;
import com.splicemachine.derby.ddl.DDLUtils;
import com.splicemachine.protobuf.ProtoUtil;
import org.apache.log4j.Logger;

import com.splicemachine.utils.SpliceLogUtils;

public class CreateSchemaConstantOperation extends DDLConstantAction {
	private static final Logger LOG = Logger.getLogger(CreateSchemaConstantOperation.class);
	private final String					aid;	// authorization id
	private final String					schemaName;
	/**
	 * Make the ConstantAction for a CREATE SCHEMA statement.
	 * When executed, will set the default schema to the
	 * new schema if the setToDefault parameter is set to
	 * true.
	 *
	 *  @param schemaName	Name of table.
	 *  @param aid			Authorizaton id
	 */
	public CreateSchemaConstantOperation(String schemaName,String aid) {
		SpliceLogUtils.trace(LOG, "CreateSchemaConstantOperation {%s}",schemaName);
		this.schemaName = schemaName;
		this.aid = aid;
	}

	public	String	toString() {
		return "CREATE SCHEMA " + schemaName;
	}

    /**
     *	This is the guts of the Execution-time logic for CREATE SCHEMA.
     *
     * @see com.splicemachine.db.iapi.sql.execute.ConstantAction#executeConstantAction(com.splicemachine.db.iapi.sql.Activation)
     *
     * @exception StandardException		Thrown on failure
     */
    @Override
    public void executeConstantAction( Activation activation ) throws StandardException {
        SpliceLogUtils.trace(LOG, "executeConstantAction");
        executeConstantActionMinion(activation,
                activation.getLanguageConnectionContext().getTransactionExecute());
    }

    /**
     *	This is the guts of the Execution-time logic for CREATE SCHEMA.
     *  This is variant is used when we to pass in a tc other than the default
     *  used in executeConstantAction(Activation).
     *
     * @param activation current activation
     * @param tc transaction controller
     *
     * @exception StandardException		Thrown on failure
     */
    public void executeConstantAction(Activation activation,TransactionController tc) throws StandardException {
        SpliceLogUtils.trace(LOG, "executeConstantAction");
        executeConstantActionMinion(activation, tc);
    }

    private void executeConstantActionMinion(Activation activation,TransactionController tc) throws StandardException {
        SpliceLogUtils.trace(LOG, "executeConstantActionMinion");
        LanguageConnectionContext lcc = activation.getLanguageConnectionContext();
        DataDictionary dd = lcc.getDataDictionary();
        DataDescriptorGenerator ddg = dd.getDataDescriptorGenerator();

        SchemaDescriptor sd = dd.getSchemaDescriptor(schemaName, lcc.getTransactionExecute(), false);

        /*
         * if the schema descriptor is an in-memory schema, we do not throw schema already exists exception for it.
         * This is to handle in-memory SESSION schema for temp tables
         */
        if ((sd != null) && (sd.getUUID() != null)) {
            throw StandardException.newException(SQLState.LANG_OBJECT_ALREADY_EXISTS, "Schema" , schemaName);
        }
        UUID tmpSchemaId = dd.getUUIDFactory().createUUID();

		    /*
		     * AID defaults to connection authorization if not
		     * specified in CREATE SCHEMA (if we had module
	 	     * authorizations, that would be the first check
		     * for default, then session aid).
		     */
        String thisAid = aid;
        if (thisAid == null) {
            thisAid = lcc.getCurrentUserId(activation);
        }

				/*
				 * Inform the data dictionary that we are about to write to it.
				 * There are several calls to data dictionary "get" methods here
				 * that might be done in "read" mode in the data dictionary, but
				 * it seemed safer to do this whole operation in "write" mode.
				 *
				 * We tell the data dictionary we're done writing at the end of
				 * the transaction.
				 */
        dd.startWriting(lcc);
        DDLMessage.DDLChange ddlChange = ProtoUtil.createSchema(((SpliceTransactionManager) tc).getActiveStateTxn().getTxnId());
        // Run Remotely
        tc.prepareDataDictionaryChange(DDLUtils.notifyMetadataChange(ddlChange));

        sd = ddg.newSchemaDescriptor(schemaName,thisAid,tmpSchemaId);

        /*
         * Note on transactional behavior:
         *
         * We want to ensure that Schema creation occurs within an internal transaction, so
         * that we can either commit or rollback the entire operation (in the event of failure).
         * In past versions of Splice, we did this by explicitly constructing a child
         * transaction here, and then manually committing or rolling it back. However,
         * DB-1706 implements transaction savepoints, which allows us to transparently act
         * as if we are a direct user transaction, which in reality we are inside of a savepoint
         * operation, so we are still safe.
         *
         * Therefore, it doesn't look like we are doing much in terms of transaction management
         * here, but in fact we are fully transactional and within a savepoint context.
         */
        dd.addDescriptor(sd, null, DataDictionary.SYSSCHEMAS_CATALOG_NUM, false, lcc.getTransactionExecute(), false);

        /*
         * Notify the DDL mechanism that a table is being created.
         *
         * In previous versions of Splice we would not use this mechanism: after all, the other servers
         *  don't really need to know that a table has been created, right?
         *
         *  The problem is that we rely on this mechanism to tell us when our data dictionary caches are to be
         *  invalidated; If we don't invalidate the caches on commit or rollback, we can get bleed-over
         *  where the table appears to exist even though it was rolled back (see CreateTableTransactionIT for some
         *  Integration tests around this).
         *
         *  To keep things simple we piggy-back on the existing DDL mechanism to ensure that our caches
         *  get cleared on commit/rollback. If this proves too expensive, then a custom local-memory-only trigger
         *  mechanism can be written for this and CREATE_SCHEMA
         */
        long txnId = ((SpliceTransactionManager)tc).getRawTransaction().getActiveStateTxn().getTxnId();
        DDLMessage.DDLChange change =DDLMessage.DDLChange.newBuilder().setDdlChangeType(DDLMessage.DDLChangeType.CREATE_SCHEMA).setTxnId(txnId).build();
        DDLController ddlController=DDLDriver.driver().ddlController();
        String currentDDLChangeId=ddlController.notifyMetadataChange(change);
        tc.prepareDataDictionaryChange(currentDDLChangeId);
    }

    public String getScopeName() {
        return String.format("Create Schema %s", schemaName);
    }
}
