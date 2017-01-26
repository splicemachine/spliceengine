/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
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

import com.splicemachine.db.iapi.sql.execute.ConstantAction;
import com.splicemachine.db.iapi.sql.dictionary.DataDescriptorGenerator;
import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;
import com.splicemachine.db.iapi.sql.dictionary.ColumnDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.ColumnDescriptorList;
import com.splicemachine.db.iapi.sql.dictionary.SchemaDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.TableDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.ViewDescriptor;
import com.splicemachine.db.iapi.sql.depend.DependencyManager;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.depend.Provider;
import com.splicemachine.db.iapi.sql.depend.ProviderInfo;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.impl.sql.execute.ColumnInfo;
import com.splicemachine.db.catalog.UUID;
import com.splicemachine.db.shared.common.reference.SQLState;
import com.splicemachine.ddl.DDLMessage;
import com.splicemachine.derby.ddl.DDLUtils;
import com.splicemachine.derby.impl.store.access.SpliceTransactionManager;
import com.splicemachine.protobuf.ProtoUtil;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.log4j.Logger;
import com.splicemachine.utils.SpliceLogUtils;

import java.util.Collections;

/**
 *	This class  describes actions that are ALWAYS performed for a
 *	CREATE VIEW Statement at Execution time.
 *  A view is represented as:
 *  <UL>
 *  <LI> TableDescriptor with the name of the view and type VIEW_TYPE
 *  <LI> Set of ColumnDescriptor's for the column names and types
 *  <LI> ViewDescriptor describing the SQL query that makes up the view.
 *  </UL>
 *  Dependencies are created as:
 *  <UL>
 *  <LI> ViewDescriptor depends on the Providers that its compiled
 *  query depends on.
 *  <LI> ViewDescriptor depends on the privileges required to execute the view.
 *  </UL>
 *  Note there are no dependencies created between the ViewDescriptor, TableDecriptor
 *  and the ColumnDescriptor's.
 *
 */

public class CreateViewConstantOperation extends DDLConstantOperation {
	private static final Logger LOG = Logger.getLogger(CreateViewConstantOperation.class);
	private final String tableName;
	private final String schemaName;
	private final String viewText;
	private final int tableType;
	private final int checkOption;
	private final ColumnInfo[] columnInfo;
	private final ProviderInfo[] providerInfo;
	private final UUID compSchemaId;

	/**
	 *	Make the ConstantAction for a CREATE VIEW statement.
	 *
	 *  @param schemaName			name for the schema that view lives in.
	 *  @param tableName	Name of view.
	 *  @param tableType	Type of table (ie. TableDescriptor.VIEW_TYPE).
	 *	@param viewText		Text of query expression for view definition
	 *  @param checkOption	Check option type
	 *  @param columnInfo	Information on all the columns in the table.
	 *  @param providerInfo Information on all the Providers
	 *  @param compSchemaId 	Compilation Schema Id
	 */
	@SuppressFBWarnings(value = "EI_EXPOSE_REP2",justification = "Intentional")
	public CreateViewConstantOperation(String schemaName, String tableName, int tableType, String viewText,
			int checkOption, ColumnInfo[] columnInfo, ProviderInfo[] providerInfo, UUID compSchemaId) {
		SpliceLogUtils.trace(LOG, "CreateViewConstantOperation for %s.%s with view creation text {%s}",schemaName, tableName, viewText);
		this.schemaName = schemaName;
		this.tableName = tableName;
		this.tableType = tableType;
		this.viewText = viewText;
		this.checkOption = checkOption;
		this.columnInfo = columnInfo;
		this.providerInfo = providerInfo;
		this.compSchemaId = compSchemaId;
		if (SanityManager.DEBUG)
			SanityManager.ASSERT(schemaName != null, "Schema name is null");
	}

	public	String	toString() {
		return constructToString("CREATE VIEW ", tableName);
	}

	/**
	 *	This is the guts of the Execution-time logic for CREATE VIEW.
	 *
	 *	@see ConstantAction#executeConstantAction
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public void executeConstantAction( Activation activation ) throws StandardException {
		SpliceLogUtils.trace(LOG, "executeConstantAction for activation {%s}",activation);
		TableDescriptor 			td;
		UUID 						toid;
		ColumnDescriptor			columnDescriptor;
		ViewDescriptor				vd;
		LanguageConnectionContext lcc = activation.getLanguageConnectionContext();
		DataDictionary dd = lcc.getDataDictionary();
		DependencyManager dm = dd.getDependencyManager();
		TransactionController tc = lcc.getTransactionExecute();

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
        TableDescriptor existingDescriptor = dd.getTableDescriptor(tableName, sd, tc);
        if (existingDescriptor != null) {
            throw StandardException.newException(SQLState.LANG_OBJECT_ALREADY_EXISTS_IN_OBJECT,
                    existingDescriptor.getDescriptorType(),
                    existingDescriptor.getDescriptorName(),
                    sd.getDescriptorType(),
                    sd.getDescriptorName());
        }

        DDLMessage.DDLChange ddlChange = ProtoUtil.createView(((SpliceTransactionManager) tc).getActiveStateTxn().getTxnId());
        // Run Remotely
        tc.prepareDataDictionaryChange(DDLUtils.notifyMetadataChange(ddlChange));


		/* Create a new table descriptor.
		 * (Pass in row locking, even though meaningless for views.)
		 */
		DataDescriptorGenerator ddg = dd.getDataDescriptorGenerator();
		td = ddg.newTableDescriptor(tableName,sd,tableType,TableDescriptor.ROW_LOCK_GRANULARITY,-1,null,null,null,null,null,null,false);

		dd.addDescriptor(td, sd, DataDictionary.SYSTABLES_CATALOG_NUM, false, tc);
		toid = td.getUUID();

		// for each column, stuff system.column
		ColumnDescriptor[] cdlArray = new ColumnDescriptor[columnInfo.length];
		int index = 1;
		for (int ix = 0; ix < columnInfo.length; ix++) {
            index++;
			columnDescriptor = new ColumnDescriptor(
				                   columnInfo[ix].name,
								   index,
					index,
								   columnInfo[ix].dataType,
								   columnInfo[ix].defaultValue,
								   columnInfo[ix].defaultInfo,
								   td,
								   (UUID) null,
								   columnInfo[ix].autoincStart,
								   columnInfo[ix].autoincInc,
                                    index
							   );
			cdlArray[ix] = columnDescriptor;
		}

		dd.addDescriptorArray(cdlArray, td,DataDictionary.SYSCOLUMNS_CATALOG_NUM, false, tc);

		// add columns to the column descriptor list.
		ColumnDescriptorList cdl = td.getColumnDescriptorList();
		Collections.addAll(cdl, cdlArray);

		/* Get and add a view descriptor */
		vd = ddg.newViewDescriptor(toid, tableName, viewText, 
									checkOption, 
									(compSchemaId == null) ?
										lcc.getDefaultSchema().getUUID() :
										compSchemaId);

		for (int ix = 0; ix < providerInfo.length; ix++) {
			/* We should always be able to find the Provider */
				Provider provider = (Provider) providerInfo[ix].
										getDependableFinder().
											getDependable(dd,
												providerInfo[ix].getObjectId());
				dm.addDependency(vd, provider, lcc.getContextManager());
		}
		//store view's dependency on various privileges in the dependeny system
		storeViewTriggerDependenciesOnPrivileges(activation, vd);

		dd.addDescriptor(vd, sd, DataDictionary.SYSVIEWS_CATALOG_NUM, true, tc);
	}

	public String getScopeName() {
		return String.format("Create View %s", tableName);
	}
}
