package com.splicemachine.derby.impl.sql.execute.actions;

import org.apache.derby.catalog.UUID;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.dictionary.DataDescriptorGenerator;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.SchemaDescriptor;
import org.apache.derby.iapi.sql.execute.ConstantAction;
import org.apache.derby.iapi.store.access.TransactionController;
import org.apache.derby.impl.sql.execute.DDLConstantAction;
import org.apache.log4j.Logger;

import com.splicemachine.utils.SpliceLogUtils;

public class CreateSchemaConstantOperation extends DDLConstantAction {
	private static final Logger LOG = Logger.getLogger(CreateSchemaConstantOperation.class);
	private final String					aid;	// authorization id
	private final String					schemaName;

	// CONSTRUCTORS

	/**
	 * Make the ConstantAction for a CREATE SCHEMA statement.
	 * When executed, will set the default schema to the
	 * new schema if the setToDefault parameter is set to
	 * true.
	 *
	 *  @param schemaName	Name of table.
	 *  @param aid			Authorizaton id
	 */
	public CreateSchemaConstantOperation(String schemaName,String aid) {
		this.schemaName = schemaName;
		this.aid = aid;
	}

	public	String	toString() {
		return "CREATE SCHEMA " + schemaName;
	}

	// INTERFACE METHODS


	/**
	 *	This is the guts of the Execution-time logic for CREATE SCHEMA.
	 *
	 *	@see ConstantAction#executeConstantAction
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public void	executeConstantAction( Activation activation ) throws StandardException {
		SpliceLogUtils.trace(LOG, "executeConstantAction");
		executeConstantActionMinion(activation, 
				activation.getLanguageConnectionContext().getTransactionExecute());
	}

	/**
	 *	This is the guts of the Execution-time logic for CREATE SCHEMA.
	 *  This is variant is used when we to pass in a tc other than the default
	 *  used in executeConstantAction(Activation).
	 *
	 * @param activation current activation
	 * @param tc transaction controller
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public void	executeConstantAction(Activation activation,TransactionController tc) throws StandardException {
		SpliceLogUtils.trace(LOG, "executeConstantAction");
		executeConstantActionMinion(activation, tc);
	}

	private void executeConstantActionMinion(Activation activation,TransactionController tc) throws StandardException {
		SpliceLogUtils.trace(LOG, "executeConstantActionMinion");
		LanguageConnectionContext lcc = activation.getLanguageConnectionContext();
		DataDictionary dd = lcc.getDataDictionary();
		DataDescriptorGenerator ddg = dd.getDataDescriptorGenerator();

		SchemaDescriptor sd = dd.getSchemaDescriptor(schemaName, lcc.getTransactionExecute(), false);

		//if the schema descriptor is an in-memory schema, we donot throw schema already exists exception for it.
		//This is to handle in-memory SESSION schema for temp tables
		if ((sd != null) && (sd.getUUID() != null)) {
			throw StandardException.newException(SQLState.LANG_OBJECT_ALREADY_EXISTS, "Schema" , schemaName);
		}
		UUID tmpSchemaId = dd.getUUIDFactory().createUUID();

		/*
		** AID defaults to connection authorization if not 
		** specified in CREATE SCHEMA (if we had module
	 	** authorizations, that would be the first check
		** for default, then session aid).
		*/
		String thisAid = aid;
		if (thisAid == null) {
            thisAid = lcc.getCurrentUserId(activation);
		}

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
		sd = ddg.newSchemaDescriptor(schemaName,thisAid,tmpSchemaId);

		dd.addDescriptor(sd, null, DataDictionary.SYSSCHEMAS_CATALOG_NUM, false, tc);
	}
}
