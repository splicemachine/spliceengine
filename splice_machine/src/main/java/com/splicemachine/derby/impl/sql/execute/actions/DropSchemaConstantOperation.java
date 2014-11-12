package com.splicemachine.derby.impl.sql.execute.actions;

import com.splicemachine.derby.ddl.DDLChangeType;
import com.splicemachine.derby.ddl.DropSchemaDDLChangeDesc;
import com.splicemachine.derby.impl.store.access.SpliceTransactionManager;
import com.splicemachine.pipeline.ddl.DDLChange;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.SchemaDescriptor;
import org.apache.derby.iapi.sql.execute.ConstantAction;

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

        DDLChange change = new DDLChange(tc.getActiveStateTxn(), DDLChangeType.DROP_SCHEMA);
        change.setTentativeDDLDesc(new DropSchemaDDLChangeDesc(this.schemaName));

        notifyMetadataChangeAndWait(change);

        SchemaDescriptor sd = dd.getSchemaDescriptor(schemaName, tc, true);
        sd.drop(lcc, activation);
    }

}