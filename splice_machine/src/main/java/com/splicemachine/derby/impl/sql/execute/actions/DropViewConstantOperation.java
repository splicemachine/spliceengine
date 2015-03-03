package com.splicemachine.derby.impl.sql.execute.actions;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;
import com.splicemachine.db.iapi.sql.dictionary.SchemaDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.TableDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.ViewDescriptor;
import com.splicemachine.db.iapi.sql.execute.ConstantAction;
import org.apache.log4j.Logger;

import com.splicemachine.utils.SpliceLogUtils;

/**
 *	This class  describes actions that are ALWAYS performed for a
 *	DROP VIEW Statement at Execution time.
 *
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

		if (td == null)
		{
			throw StandardException.newException(SQLState.LANG_TABLE_NOT_FOUND_DURING_EXECUTION, fullTableName);
		}

		/* Verify that TableDescriptor represents a view */
		if (td.getTableType() != TableDescriptor.VIEW_TYPE)
		{
			throw StandardException.newException(SQLState.LANG_DROP_VIEW_ON_NON_VIEW, fullTableName);
		}

		vd = dd.getViewDescriptor(td);

		vd.drop(lcc, sd, td);
	}
}
