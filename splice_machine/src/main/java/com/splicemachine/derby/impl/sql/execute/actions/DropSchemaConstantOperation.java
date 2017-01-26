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

import com.splicemachine.ddl.DDLMessage;
import com.splicemachine.derby.ddl.DDLUtils;
import com.splicemachine.derby.impl.store.access.SpliceTransactionManager;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;
import com.splicemachine.db.iapi.sql.dictionary.SchemaDescriptor;
import com.splicemachine.db.iapi.sql.execute.ConstantAction;
import com.splicemachine.protobuf.ProtoUtil;

/**
 *	This class  describes actions that are ALWAYS performed for a
 *	DROP SCHEMA Statement at Execution time.
 *
 */

public class DropSchemaConstantOperation extends DDLConstantOperation {
	private final String schemaName;
	/**
	 *	Make the ConstantAction for a DROP TABLE statement.
	 *
	 *	@param	schemaName			Table name.
	 *
	 */
	public DropSchemaConstantOperation(String	schemaName) {
		this.schemaName = schemaName;
	}

    public	String	toString() {
        return "DROP SCHEMA " + schemaName;
    }

    /**
     *	This is the guts of the Execution-time logic for DROP TABLE.
     *
     *	@see ConstantAction#executeConstantAction
     *
     * @exception StandardException		Thrown on failure
     */
    public void executeConstantAction( Activation activation ) throws StandardException {
        LanguageConnectionContext lcc = activation.getLanguageConnectionContext();
        DataDictionary dd = lcc.getDataDictionary();

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
        SpliceTransactionManager tc = (SpliceTransactionManager)lcc.getTransactionExecute();
        SchemaDescriptor sd = dd.getSchemaDescriptor(schemaName, tc, true);
        dd.dropAllSchemaPermDescriptors(sd.getObjectID(),tc);
        sd.drop(lcc, activation);
        DDLMessage.DDLChange ddlChange = ProtoUtil.createDropSchema(tc.getActiveStateTxn().getTxnId(), schemaName);
        tc.prepareDataDictionaryChange(DDLUtils.notifyMetadataChange(ddlChange));
    }

    public String getScopeName() {
        return String.format("Drop Schema %s", schemaName);
    }
}