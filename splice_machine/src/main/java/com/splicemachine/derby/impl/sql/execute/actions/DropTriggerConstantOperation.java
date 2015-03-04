package com.splicemachine.derby.impl.sql.execute.actions;

import com.splicemachine.db.catalog.UUID;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.depend.DependencyManager;
import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;
import com.splicemachine.db.iapi.sql.dictionary.SPSDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.SchemaDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.TableDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.TriggerDescriptor;
import com.splicemachine.db.iapi.sql.execute.ConstantAction;
import com.splicemachine.db.iapi.store.access.TransactionController;

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
	public void executeConstantAction( Activation activation ) throws StandardException {
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
