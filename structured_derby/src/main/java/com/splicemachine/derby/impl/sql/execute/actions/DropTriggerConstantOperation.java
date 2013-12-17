package com.splicemachine.derby.impl.sql.execute.actions;

import org.apache.derby.catalog.UUID;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.depend.DependencyManager;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.SPSDescriptor;
import org.apache.derby.iapi.sql.dictionary.SchemaDescriptor;
import org.apache.derby.iapi.sql.dictionary.TableDescriptor;
import org.apache.derby.iapi.sql.dictionary.TriggerDescriptor;
import org.apache.derby.iapi.sql.execute.ConstantAction;
import org.apache.derby.iapi.store.access.TransactionController;

/**
 *	This class  describes actions that are ALWAYS performed for a
 *	DROP TRIGGER Statement at Execution time.
 *
 */
public class DropTriggerConstantOperation extends DDLSingleTableConstantOperation {
	private final String triggerName;
	private final SchemaDescriptor sd;

	/**
	 *	Make the ConstantAction for a DROP TRIGGER statement.
	 *
	 * @param	sd					Schema that stored prepared statement lives in.
	 * @param	triggerName			Name of the Trigger
	 * @param	tableId				The table upon which the trigger is defined
	 *
	 */
	public DropTriggerConstantOperation(SchemaDescriptor sd, String triggerName, UUID tableId) {
		super(tableId);
		this.sd = sd;
		this.triggerName = triggerName;
		if (SanityManager.DEBUG)
			SanityManager.ASSERT(sd != null, "SchemaDescriptor is null");
	}

	/**
	 *	This is the guts of the Execution-time logic for DROP STATEMENT.
	 *
	 *	@see ConstantAction#executeConstantAction
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public void executeTransactionalConstantAction( Activation activation ) throws StandardException {
		TriggerDescriptor 			triggerd;
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

		TableDescriptor td = dd.getTableDescriptor(tableId);
		if (td == null)
		{
			throw StandardException.newException(
								SQLState.LANG_TABLE_NOT_FOUND_DURING_EXECUTION,
								tableId.toString());
		}
		TransactionController tc = lcc.getTransactionExecute();
		// XXX - TODO NO LOCKING lockTableForDDL(tc, td.getHeapConglomerateId(), true);
		// get td again in case table shape is changed before lock is acquired
		td = dd.getTableDescriptor(tableId);
		if (td == null)
		{
			throw StandardException.newException(
								SQLState.LANG_TABLE_NOT_FOUND_DURING_EXECUTION,
								tableId.toString());
		}

		/* 
		** Get the trigger descriptor.  We're responsible for raising
		** the error if it isn't found 
		*/
		triggerd = dd.getTriggerDescriptor(triggerName, sd);

		if (triggerd == null)
		{
			throw StandardException.newException(SQLState.LANG_OBJECT_NOT_FOUND_DURING_EXECUTION, "TRIGGER",
					(sd.getSchemaName() + "." + triggerName));
		}

		/* 
	 	** Prepare all dependents to invalidate.  (This is there chance
		** to say that they can't be invalidated.  For example, an open
		** cursor referencing a table/trigger that the user is attempting to
		** drop.) If no one objects, then invalidate any dependent objects.
		*/
        triggerd.drop(lcc);
	}

	public String toString()
	{
		// Do not put this under SanityManager.DEBUG - it is needed for
		// error reporting.
		return "DROP TRIGGER "+triggerName;
	}
}
