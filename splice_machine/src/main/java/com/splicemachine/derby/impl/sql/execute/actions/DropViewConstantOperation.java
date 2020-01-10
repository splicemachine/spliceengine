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

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.depend.DependencyManager;
import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;
import com.splicemachine.db.iapi.sql.dictionary.SchemaDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.TableDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.ViewDescriptor;
import com.splicemachine.db.iapi.sql.execute.ConstantAction;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.db.impl.services.uuid.BasicUUID;
import com.splicemachine.ddl.DDLMessage;
import com.splicemachine.derby.ddl.DDLUtils;
import com.splicemachine.derby.impl.store.access.SpliceTransactionManager;
import com.splicemachine.protobuf.ProtoUtil;
import org.apache.log4j.Logger;

import com.splicemachine.utils.SpliceLogUtils;

/**
 *	This class  describes actions that are ALWAYS performed for a
 *	DROP VIEW Statement at Execution time.
 */
public class DropViewConstantOperation extends DDLConstantOperation {
	private static final Logger LOG = Logger.getLogger(DropViewConstantOperation.class);
	private String fullTableName;
	private String tableName;
	private SchemaDescriptor sd;

	/**
	 *	Make the ConstantAction for a DROP VIEW statement.
	 *
	 *
	 *	@param	fullTableName		Fully qualified table name
	 *	@param	tableName			Table name.
	 *	@param	sd					Schema that view lives in.
	 *
	 */
	public DropViewConstantOperation(String fullTableName,String tableName, SchemaDescriptor sd) {
		SpliceLogUtils.trace(LOG, "DropViewConstantOperation fullTableName %s, tableName %s, schemaName %s",fullTableName, tableName, sd.getSchemaName());
		this.fullTableName = fullTableName;
		this.tableName = tableName;
		this.sd = sd;
		if (SanityManager.DEBUG)
			SanityManager.ASSERT(sd != null, "SchemaDescriptor is null");
	}

	public	String	toString() {
		return "DROP VIEW " + fullTableName;
	}

	/**
	 *	This is the guts of the Execution-time logic for DROP VIEW.
	 *
	 *	@see ConstantAction#executeConstantAction
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public void executeConstantAction( Activation activation ) throws StandardException {
		SpliceLogUtils.trace(LOG, "executeConstantAction for activation %s",activation);
		TableDescriptor td;
		ViewDescriptor vd;

		LanguageConnectionContext lcc = activation.getLanguageConnectionContext();
		DataDictionary dd = lcc.getDataDictionary();

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

		/* Get the table descriptor.  We're responsible for raising
		 * the error if it isn't found 
		 */
		td = dd.getTableDescriptor(tableName, sd,
                lcc.getTransactionExecute());

		if (td == null) {
			throw StandardException.newException(SQLState.LANG_TABLE_NOT_FOUND_DURING_EXECUTION, fullTableName);
		}

		/* Verify that TableDescriptor represents a view */
		if (td.getTableType() != TableDescriptor.VIEW_TYPE) {
			throw StandardException.newException(SQLState.LANG_DROP_VIEW_ON_NON_VIEW, fullTableName);
		}
		vd = dd.getViewDescriptor(td);
        invalidate(dd.getDependencyManager(),td,DependencyManager.DROP_VIEW,lcc);

        TransactionController tc = lcc.getTransactionExecute();
        DDLMessage.DDLChange ddlChange = ProtoUtil.dropView(((SpliceTransactionManager) tc).getActiveStateTxn().getTxnId(), (BasicUUID) td.getUUID());
        // Run Remotely
        tc.prepareDataDictionaryChange(DDLUtils.notifyMetadataChange(ddlChange));
        drop(lcc, sd, td, DependencyManager.DROP_VIEW, vd);
	}

    public String getScopeName() {
        return String.format("Drop View %s", fullTableName);
    }

    /**
     * Drop this descriptor, if not already done, due to action.
     * If action is not {@code DependencyManager.DROP_VIEW}, the descriptor is
     * dropped due to dropping some other object, e.g. a table column.
     *
     * @param lcc current language connection context
     * @param sd schema descriptor
     * @param td table descriptor for this view
     * @param action action
     * @throws StandardException standard error policy
     */
    public static void drop(
            LanguageConnectionContext lcc,
            SchemaDescriptor sd,
            TableDescriptor td,
            int action,
            ViewDescriptor vd) throws StandardException
    {
        DataDictionary dd = td.getDataDictionary();
        DependencyManager dm = dd.getDependencyManager();
        TransactionController tc = lcc.getTransactionExecute();

		/* Drop the columns */
        dd.dropAllColumnDescriptors(td.getUUID(), tc);

		/* Clear the dependencies for the view */
        dm.clearDependencies(lcc, vd);

		/* Drop the view */
        dd.dropViewDescriptor(vd, tc);

		/* Drop all table and column permission descriptors */
        dd.dropAllTableAndColPermDescriptors(td.getUUID(), tc);

		/* Drop the table */
        dd.dropTableDescriptor(td, sd, tc);
    }

    public void invalidate(DependencyManager dm, TableDescriptor td, int action, LanguageConnectionContext lcc) throws StandardException {
        		/* Prepare all dependents to invalidate.  (This is there chance
		 * to say that they can't be invalidated.  For example, an open
		 * cursor referencing a table/view that the user is attempting to
		 * drop.) If no one objects, then invalidate any dependent objects.
		 */
        dm.invalidateFor(td, action, lcc);
    }
}
