package com.splicemachine.derby.impl.sql.execute.actions;

import org.apache.derby.iapi.sql.execute.ConstantAction;

import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.SchemaDescriptor;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.ParameterValueSet;
import org.apache.derby.iapi.sql.StatementType;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.reference.Limits;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.reference.SQLState;

/**
 *	This class describes actions that are ALWAYS performed for a
 *	SET SCHEMA Statement at Execution time.
 *
 */

public class SetSchemaConstantOperation implements ConstantAction {
	private final String schemaName;
	private final int type;	
	/**
	 * Make the ConstantAction for a SET SCHEMA statement.
	 *
	 *  @param schemaName	Name of schema.
	 *  @param type		type of set schema (e.g. SET_SCHEMA_DYNAMIC, SET_SCHEMA_USER)
	 */
	public SetSchemaConstantOperation(String schemaName, int type) {
		this.schemaName = schemaName;
		this.type = type;
	}

	public	String	toString() {
		return "SET SCHEMA " + ((type == StatementType.SET_SCHEMA_USER) ? "USER" : 
				((type == StatementType.SET_SCHEMA_DYNAMIC && schemaName == null) ? "?" : schemaName));
	}


	/**
	 *	This is the guts of the Execution-time logic for SET SCHEMA.
	 *
	 *	@see ConstantAction#executeConstantAction
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public void executeConstantAction( Activation activation ) throws StandardException {
		LanguageConnectionContext lcc = activation.getLanguageConnectionContext();
		DataDictionary dd = lcc.getDataDictionary();
		String thisSchemaName = schemaName;
		if (type == StatementType.SET_SCHEMA_DYNAMIC) {
			ParameterValueSet pvs = activation.getParameterValueSet();
			DataValueDescriptor dvs = pvs.getParameter(0);
			thisSchemaName = dvs.getString();
			//null parameter is not allowed
			if (thisSchemaName == null || thisSchemaName.length() > Limits.MAX_IDENTIFIER_LENGTH)
				throw StandardException.newException(SQLState.LANG_DB2_REPLACEMENT_ERROR, "CURRENT SCHEMA");
		}
		else if (type == StatementType.SET_SCHEMA_USER) {
            thisSchemaName = lcc.getCurrentUserId(activation);
		}

        SchemaDescriptor sd = dd.getSchemaDescriptor(thisSchemaName,lcc.getTransactionExecute(), true);
		lcc.setDefaultSchema(activation, sd);
	}
}

