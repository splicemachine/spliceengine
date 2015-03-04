package com.splicemachine.derby.impl.sql.execute.actions;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;
import com.splicemachine.db.iapi.sql.dictionary.SequenceDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.SchemaDescriptor;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.db.shared.common.reference.SQLState;

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
        TransactionController tc = lcc.getTransactionExecute();
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
        sequenceDescriptor.drop(lcc);
    }
}
