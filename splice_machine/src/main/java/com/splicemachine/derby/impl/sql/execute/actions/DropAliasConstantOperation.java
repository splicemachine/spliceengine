/*
 * Copyright (c) 2012 - 2019 Splice Machine, Inc.
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

import com.splicemachine.db.catalog.AliasInfo;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.depend.DependencyManager;
import com.splicemachine.db.iapi.sql.dictionary.*;
import com.splicemachine.db.iapi.sql.execute.ConstantAction;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.db.impl.sql.catalog.TableKey;
import com.splicemachine.ddl.DDLMessage;
import com.splicemachine.derby.ddl.DDLUtils;
import com.splicemachine.derby.impl.store.access.SpliceTransactionManager;
import com.splicemachine.protobuf.ProtoUtil;
import com.splicemachine.utils.SpliceLogUtils;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.log4j.Logger;

import javax.annotation.Nonnull;

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
	public DropAliasConstantOperation(@Nonnull SchemaDescriptor sd,
                                      String aliasName,
                                      char nameSpace) {
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
	public void executeConstantAction( Activation activation ) throws StandardException {
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
        drop(lcc, ad);
	}

    public void drop(LanguageConnectionContext lcc,AliasDescriptor ad) throws StandardException {

        DataDictionary dd = ad.getDataDictionary();
        TransactionController tc = lcc.getTransactionExecute();
        DependencyManager dm = dd.getDependencyManager();
        invalidate(ad,dm,lcc);
        DDLMessage.DDLChange ddlChange = ProtoUtil.dropAlias(
                ((SpliceTransactionManager) tc).getActiveStateTxn().getTxnId(),
                sd.getUUID().toString(), aliasName, nameSpace+"");
        // Run Remotely
        tc.prepareDataDictionaryChange(DDLUtils.notifyMetadataChange(ddlChange));

    if (ad.getAliasType() == AliasInfo.ALIAS_TYPE_SYNONYM_AS_CHAR) {
            SchemaDescriptor sd = dd.getSchemaDescriptor(ad.getSchemaUUID(), tc);

            // Drop the entry from SYSTABLES as well.
            DataDescriptorGenerator ddg = dd.getDataDescriptorGenerator();
            TableDescriptor td = ddg.newTableDescriptor(aliasName, sd,
                    TableDescriptor.SYNONYM_TYPE, TableDescriptor.DEFAULT_LOCK_GRANULARITY,-1,
                    null,null,null,null,null,null,false,false);
            dd.dropTableDescriptor(td, sd, tc);
        }
        else
            dd.dropAllRoutinePermDescriptors(ad.getUUID(), tc);
        /* Drop the alias */
        dd.dropAliasDescriptor(ad, tc);
    }


    @SuppressFBWarnings(value = "SF_SWITCH_NO_DEFAULT",justification = "Intentional")
    public static void invalidate(AliasDescriptor ad, DependencyManager dm, LanguageConnectionContext lcc) throws StandardException {
        int invalidationType = 0;
        switch (ad.getAliasType()) {
            case AliasInfo.ALIAS_TYPE_PROCEDURE_AS_CHAR:
            case AliasInfo.ALIAS_TYPE_FUNCTION_AS_CHAR:
                invalidationType = DependencyManager.DROP_METHOD_ALIAS;
                break;

            case AliasInfo.ALIAS_TYPE_SYNONYM_AS_CHAR:
                invalidationType = DependencyManager.DROP_SYNONYM;
                break;

            case AliasInfo.ALIAS_TYPE_UDT_AS_CHAR:
                invalidationType = DependencyManager.DROP_UDT;
                break;
            case AliasInfo.ALIAS_TYPE_AGGREGATE_AS_CHAR:
                invalidationType = DependencyManager.DROP_AGGREGATE;
                break;
        }

        dm.invalidateFor(ad, invalidationType, lcc);
        if (ad.getAliasType() == AliasInfo.ALIAS_TYPE_SYNONYM_AS_CHAR) {
            // invalidate nameTdCache
            TableKey key = new TableKey(ad.getSchemaUUID(), ad.getName());
            lcc.getDataDictionary().getDataDictionaryCache().nameTdCacheRemove(key);
        }
    }

    public String getScopeName() {
        return String.format("Drop Alias %s", aliasName);
    }
}
