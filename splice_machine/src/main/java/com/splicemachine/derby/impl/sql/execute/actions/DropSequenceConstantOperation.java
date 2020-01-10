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
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.depend.DependencyManager;
import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;
import com.splicemachine.db.iapi.sql.dictionary.SequenceDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.SchemaDescriptor;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.db.shared.common.reference.SQLState;
import com.splicemachine.ddl.DDLMessage;
import com.splicemachine.derby.ddl.DDLUtils;
import com.splicemachine.derby.impl.store.access.SpliceTransactionManager;
import com.splicemachine.protobuf.ProtoUtil;

/**
 * This class  describes actions that are ALWAYS performed for a
 * DROP SEQUENCE Statement at Execution time.
 */
public class DropSequenceConstantOperation extends DDLConstantOperation {
    private final String sequenceName;
    private final SchemaDescriptor schemaDescriptor;

    /**
     * Make the ConstantAction for a DROP SEQUENCE statement.
     *
     * @param sequenceName sequence name to be dropped
     */
    public DropSequenceConstantOperation(SchemaDescriptor sd, String sequenceName) {
        this.sequenceName = sequenceName;
        this.schemaDescriptor = sd;
    }

    public String toString() {
        return "DROP SEQUENCE " + sequenceName;
    }

    /**
     * This is the guts of the Execution-time logic for DROP SEQUENCE.
     *
     * @see com.splicemachine.db.iapi.sql.execute.ConstantAction#executeConstantAction
     */
    public void executeConstantAction(Activation activation) throws StandardException {
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
        SequenceDescriptor sequenceDescriptor = dd.getSequenceDescriptor(schemaDescriptor, sequenceName);
        if (sequenceDescriptor == null) {
            throw StandardException.newException(SQLState.LANG_OBJECT_NOT_FOUND_DURING_EXECUTION, "SEQUENCE",
                    (schemaDescriptor.getObjectName() + "." + sequenceName));
        }
        DependencyManager dm = dd.getDependencyManager();
        TransactionController tc = lcc.getTransactionExecute();
        // invalidate compiled statements which depend on this sequence
        dm.invalidateFor(sequenceDescriptor, DependencyManager.DROP_SEQUENCE, lcc);

        DDLMessage.DDLChange ddlChange = ProtoUtil.dropSequence(((SpliceTransactionManager) tc).getActiveStateTxn().getTxnId(), schemaDescriptor.getSchemaName(),sequenceName);
        // Run Remotely
        tc.prepareDataDictionaryChange(DDLUtils.notifyMetadataChange(ddlChange));
        // drop the sequence
        dd.dropSequenceDescriptor(sequenceDescriptor, tc);
        // Clear the dependencies for the sequence
        dm.clearDependencies(lcc, sequenceDescriptor);
    }
}
