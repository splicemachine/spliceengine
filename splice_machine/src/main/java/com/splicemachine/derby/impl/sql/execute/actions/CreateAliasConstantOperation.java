/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
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
import com.splicemachine.db.catalog.UUID;
import com.splicemachine.db.catalog.types.RoutineAliasInfo;
import com.splicemachine.db.catalog.types.SynonymAliasInfo;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.dictionary.AliasDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.DataDescriptorGenerator;
import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;
import com.splicemachine.db.iapi.sql.dictionary.SchemaDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.TableDescriptor;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.ddl.DDLMessage;
import com.splicemachine.derby.ddl.DDLUtils;
import com.splicemachine.derby.impl.store.access.SpliceTransactionManager;
import com.splicemachine.protobuf.ProtoUtil;
import org.apache.log4j.Logger;

import com.splicemachine.utils.SpliceLogUtils;

/**
 *	This class performs actions that are ALWAYS performed for a
 *	CREATE FUNCTION, PROCEDURE or SYNONYM Statement at execution time.
 *  These SQL objects are stored in the SYS.SYSALIASES table and
 *  represented as AliasDescriptors.
 *
 */
public class CreateAliasConstantOperation extends DDLConstantOperation {
	private static final Logger LOG = Logger.getLogger(CreateAliasConstantOperation.class);
	private final String aliasName;
	private final String schemaName;
	private final String javaClassName;
	private final char aliasType;
	private final char nameSpace;
	private final AliasInfo aliasInfo;
	/**
	 *	Make the ConstantAction for a CREATE alias statement.
	 *
	 *  @param aliasName		Name of alias.
	 *  @param schemaName		Name of alias's schema.
	 *  @param javaClassName	Name of java class.
	 *  @param aliasInfo		AliasInfo
	 *  @param aliasType		The type of the alias
	 */
	public CreateAliasConstantOperation(String aliasName, String schemaName, String javaClassName,
			AliasInfo aliasInfo,char aliasType) {
		SpliceLogUtils.trace(LOG, "CreateAliasConstantOperation with alias {%s} on schema %s with aliasInfo %s",aliasName, schemaName, aliasInfo);
		this.aliasName = aliasName;
		this.schemaName = schemaName;
		this.javaClassName = javaClassName;
		this.aliasInfo = aliasInfo;
		this.aliasType = aliasType;
		switch (aliasType) {
            case AliasInfo.ALIAS_TYPE_AGGREGATE_AS_CHAR:
                nameSpace = AliasInfo.ALIAS_NAME_SPACE_AGGREGATE_AS_CHAR;
                break;
			case AliasInfo.ALIAS_TYPE_PROCEDURE_AS_CHAR:
				nameSpace = AliasInfo.ALIAS_NAME_SPACE_PROCEDURE_AS_CHAR;
				break;
			case AliasInfo.ALIAS_TYPE_FUNCTION_AS_CHAR:
				nameSpace = AliasInfo.ALIAS_NAME_SPACE_FUNCTION_AS_CHAR;
				break;
			case AliasInfo.ALIAS_TYPE_SYNONYM_AS_CHAR:
				nameSpace = AliasInfo.ALIAS_NAME_SPACE_SYNONYM_AS_CHAR;
				break;
			case AliasInfo.ALIAS_TYPE_UDT_AS_CHAR:
				nameSpace = AliasInfo.ALIAS_NAME_SPACE_UDT_AS_CHAR;
				break;
			default:
				if (SanityManager.DEBUG)
					SanityManager.THROWASSERT("Unexpected value for aliasType (" + aliasType + ")");
				nameSpace = '\0';
				break;
		}
	}

	public	String	toString() {
		String type = null;
		switch (aliasType) {
            case AliasInfo.ALIAS_TYPE_AGGREGATE_AS_CHAR:
                type = "CREATE DERBY AGGREGATE ";
                break;
			case AliasInfo.ALIAS_TYPE_PROCEDURE_AS_CHAR:
				type = "CREATE PROCEDURE ";
				break;
			case AliasInfo.ALIAS_TYPE_FUNCTION_AS_CHAR:
				type = "CREATE FUNCTION ";
				break;
			case AliasInfo.ALIAS_TYPE_SYNONYM_AS_CHAR:
				type = "CREATE SYNONYM ";
				break;
			case AliasInfo.ALIAS_TYPE_UDT_AS_CHAR:
				type = "CREATE TYPE ";
				break;
			default:
				if (SanityManager.DEBUG)
					SanityManager.THROWASSERT("Unexpected value for aliasType (" + aliasType + ")");
		}
		return	type + aliasName;
	}

	/**
	 *	This is the guts of the Execution-time logic for
     *  CREATE FUNCTION, PROCEDURE, SYNONYM, and TYPE.
     *  <P>
     *  A function, procedure, or udt is represented as:
     *  <UL>
     *  <LI> AliasDescriptor
     *  </UL>
     *  Routine dependencies are created as:
     *  <UL>
     *  <LI> None
     *  </UL>
     *  
     *  <P>
     *  A synonym is represented as:
     *  <UL>
     *  <LI> AliasDescriptor
     *  <LI> TableDescriptor
     *  </UL>
     *  Synonym dependencies are created as:
     *  <UL>
     *  <LI> None
     *  </UL>
     *  
     *  In both cases a SchemaDescriptor will be created if
     *  needed. No dependency is created on the SchemaDescriptor.
     *  
	 * @see ConstantAction#executeConstantAction
     * @see AliasDescriptor
     * @see TableDescriptor
     * @see SchemaDescriptor
	 *
	 * @exception StandardException		Thrown on failure
	 */
	@Override
	public void executeConstantAction( Activation activation ) throws StandardException {
		SpliceLogUtils.trace(LOG, "executeConstantAction with activation %s",activation);
		LanguageConnectionContext lcc = activation.getLanguageConnectionContext();
		DataDictionary dd = lcc.getDataDictionary();
		TransactionController tc = lcc.getTransactionExecute();

		// For routines no validity checking is made
        // on the Java method, that is checked when the
        // routine is executed.
        
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

		
		SchemaDescriptor sd = DDLConstantOperation.getSchemaDescriptorForCreate(dd, activation, schemaName);

        DDLMessage.DDLChange ddlChange = ProtoUtil.createAlias(((SpliceTransactionManager) tc).getActiveStateTxn().getTxnId());
        // Run Remotely
        tc.prepareDataDictionaryChange(DDLUtils.notifyMetadataChange(ddlChange));


        //
		// Create a new alias descriptor with aliasID filled in.
		// 
		UUID aliasID = dd.getUUIDFactory().createUUID();

		AliasDescriptor ads = new AliasDescriptor(dd, aliasID,
									 aliasName,
									 sd.getUUID(),
									 javaClassName,
									 aliasType,
									 nameSpace,
									 false,
									 aliasInfo, null);

		// perform duplicate rule checking
		switch (aliasType) {
        case AliasInfo.ALIAS_TYPE_AGGREGATE_AS_CHAR:

            AliasDescriptor duplicateAlias = dd.getAliasDescriptor( sd.getUUID().toString(), aliasName, nameSpace );
            if ( duplicateAlias != null )
            {
                throw StandardException.newException( SQLState.LANG_OBJECT_ALREADY_EXISTS, ads.getDescriptorType(), aliasName );
            }

            // also don't want to collide with 1-arg functions by the same name
            java.util.List funcList = dd.getRoutineList( sd.getUUID().toString(), aliasName, AliasInfo.ALIAS_TYPE_FUNCTION_AS_CHAR );
            for ( int i = 0; i < funcList.size(); i++ )
            {
                AliasDescriptor func = (AliasDescriptor) funcList.get(i);

                RoutineAliasInfo funcInfo = (RoutineAliasInfo) func.getAliasInfo();
                if ( funcInfo.getParameterCount() == 1 )
                {
                    throw StandardException.newException
                            ( SQLState.LANG_BAD_UDA_OR_FUNCTION_NAME, schemaName, aliasName );
                }
            }
            break;

            case AliasInfo.ALIAS_TYPE_UDT_AS_CHAR:

            AliasDescriptor duplicateUDT = dd.getAliasDescriptor( sd.getUUID().toString(), aliasName, nameSpace );
            if ( duplicateUDT != null ) { throw StandardException.newException( SQLState.LANG_OBJECT_ALREADY_EXISTS, ads.getDescriptorType(), aliasName ); }
            break;
            
		case AliasInfo.ALIAS_TYPE_PROCEDURE_AS_CHAR:
		case AliasInfo.ALIAS_TYPE_FUNCTION_AS_CHAR:
		{

			java.util.List list = dd.getRoutineList(
				sd.getUUID().toString(), aliasName, aliasType);
			for (int i = list.size() - 1; i >= 0; i--) {

				AliasDescriptor proc = (AliasDescriptor) list.get(i);

				RoutineAliasInfo procedureInfo = (RoutineAliasInfo) proc.getAliasInfo();
				int parameterCount = procedureInfo.getParameterCount();
				if (parameterCount != ((RoutineAliasInfo) aliasInfo).getParameterCount())
					continue;

				// procedure duplicate checking is simple, only
				// one procedure with a given number of parameters.
				throw StandardException.newException(SQLState.LANG_OBJECT_ALREADY_EXISTS,
												ads.getDescriptorType(),
												aliasName);
			}
		}
		break;
		case AliasInfo.ALIAS_TYPE_SYNONYM_AS_CHAR:
			// If target table/view exists already, error.
			TableDescriptor targetTD = dd.getTableDescriptor(aliasName, sd, tc);
			if (targetTD != null)
			{
				throw StandardException.newException(
								SQLState.LANG_OBJECT_ALREADY_EXISTS,
								targetTD.getDescriptorType(),
								targetTD.getDescriptorName());
			}

			// Detect synonym cycles, if present.
			String nextSynTable = ((SynonymAliasInfo)aliasInfo).getSynonymTable();
			String nextSynSchema = ((SynonymAliasInfo)aliasInfo).getSynonymSchema();
			SchemaDescriptor nextSD;
			for (;;)
			{
				nextSD = dd.getSchemaDescriptor(nextSynSchema, tc, false);
				if (nextSD == null)
					break;
				
				AliasDescriptor nextAD = dd.getAliasDescriptor(nextSD.getUUID().toString(),
						 nextSynTable, nameSpace);
				if (nextAD == null)
					break;

				SynonymAliasInfo info = (SynonymAliasInfo) nextAD.getAliasInfo();
				nextSynTable = info.getSynonymTable();
				nextSynSchema = info.getSynonymSchema();

				if (aliasName.equals(nextSynTable) && schemaName.equals(nextSynSchema))
					throw StandardException.newException(SQLState.LANG_SYNONYM_CIRCULAR,
							aliasName, ((SynonymAliasInfo)aliasInfo).getSynonymTable());
			}

			// If synonym final target is not present, raise a warning
			if (nextSD != null)
				targetTD = dd.getTableDescriptor(nextSynTable, nextSD, tc);
			if (nextSD == null || targetTD == null)
				activation.addWarning(
					StandardException.newWarning(SQLState.LANG_SYNONYM_UNDEFINED,
								aliasName, nextSynSchema+"."+nextSynTable));

			// To prevent any possible deadlocks with SYSTABLES, we insert a row into
			// SYSTABLES also for synonyms. This also ensures tables/views/synonyms share
			// same namespace
			TableDescriptor td;
			DataDescriptorGenerator ddg = dd.getDataDescriptorGenerator();
			td = ddg.newTableDescriptor(aliasName, sd, TableDescriptor.SYNONYM_TYPE,
						TableDescriptor.DEFAULT_LOCK_GRANULARITY,-1,
					null,null,null,null,null,null,false,false,null);
			dd.addDescriptor(td, sd, DataDictionary.SYSTABLES_CATALOG_NUM, false, tc, false);
            break;
		
		default:
			break;
		}

		dd.addDescriptor(ads, null, DataDictionary.SYSALIASES_CATALOG_NUM,
						 false, tc, false);

        adjustUDTDependencies( lcc, dd, ads, true );
	}

	public String getScopeName() {
		return String.format("Create Alias %s", aliasName);
	}
}
