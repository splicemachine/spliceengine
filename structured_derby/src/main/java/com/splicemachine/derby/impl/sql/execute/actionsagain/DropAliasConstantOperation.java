package com.splicemachine.derby.impl.sql.execute.actionsagain;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.dictionary.AliasDescriptor;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.SchemaDescriptor;
import org.apache.derby.iapi.sql.execute.ConstantAction;
import org.apache.log4j.Logger;

import com.splicemachine.utils.SpliceLogUtils;

/**
 *	This class performs actions that are ALWAYS performed for a
 *	DROP FUNCTION/PROCEDURE/SYNONYM statement at execution time.
 *  All of these SQL objects are represented by an AliasDescriptor.
 *
 */

public class DropAliasConstantOperation extends DDLConstantOperation {
	private static final Logger LOG = Logger.getLogger(DropAliasConstantOperation.class);
	private SchemaDescriptor sd;
	private final String aliasName;
	private final char nameSpace;
	/**
	 *	Make the ConstantAction for a DROP  ALIAS statement.
	 *
	 *
	 *	@param	aliasName			Alias name.
	 *	@param	nameSpace			Alias name space.
	 *
	 */
	public DropAliasConstantOperation(SchemaDescriptor sd, String aliasName, char nameSpace) {
		SpliceLogUtils.trace(LOG, "DropAliasConstantOperation for %s.%s", sd.getSchemaName(),aliasName);
		this.sd = sd;
		this.aliasName = aliasName;
		this.nameSpace = nameSpace;
	}
	
	public	String	toString() {
		return	"DROP ALIAS " + aliasName;
	}


	/**
	 *	This is the guts of the Execution-time logic for DROP ALIAS.
	 *
	 *	@see ConstantAction#executeConstantAction
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public void	executeConstantAction( Activation activation ) throws StandardException {
		SpliceLogUtils.trace(LOG, "executeConstantAction for activation {%s}", activation);
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

		/* Get the alias descriptor.  We're responsible for raising
		 * the error if it isn't found 
		 */
		AliasDescriptor ad = dd.getAliasDescriptor(sd.getUUID().toString(), aliasName, nameSpace);
		// RESOLVE - fix error message
		if (ad == null)
			throw StandardException.newException(SQLState.LANG_OBJECT_NOT_FOUND, AliasDescriptor.getAliasType(nameSpace),  aliasName);
        adjustUDTDependencies( lcc, dd, ad, false );
        ad.drop(lcc);
	}
}
