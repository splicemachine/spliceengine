/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
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