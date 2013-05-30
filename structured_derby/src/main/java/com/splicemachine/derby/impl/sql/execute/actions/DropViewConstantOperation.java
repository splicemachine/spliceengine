package com.splicemachine.derby.impl.sql.execute.actions;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.SchemaDescriptor;
import org.apache.derby.iapi.sql.dictionary.TableDescriptor;
import org.apache.derby.iapi.sql.dictionary.ViewDescriptor;
import org.apache.derby.iapi.sql.execute.ConstantAction;

/**
 *	This class  describes actions that are ALWAYS performed for a
 *	DROP VIEW Statement at Execution time.
 *
 */

public class DropViewConstantOperation extends DDLConstantOperation {
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
	public void	executeConstantAction( Activation activation ) throws StandardException {
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
