/*
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 *
 * Some parts of this source code are based on Apache Derby, and the following notices apply to
 * Apache Derby:
 *
 * Apache Derby is a subproject of the Apache DB project, and is licensed under
 * the Apache License, Version 2.0 (the "License"); you may not use these files
 * except in compliance with the License. You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * Splice Machine, Inc. has modified the Apache Derby code in this file.
 *
 * All such Splice Machine modifications are Copyright 2012 - 2020 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */

package com.splicemachine.db.impl.sql.execute;

import com.splicemachine.db.iapi.sql.execute.ConstantAction;

import com.splicemachine.db.iapi.store.access.TransactionController;

import com.splicemachine.db.iapi.sql.dictionary.DataDescriptorGenerator;
import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;
import com.splicemachine.db.iapi.sql.dictionary.SchemaDescriptor;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;

import com.splicemachine.db.iapi.reference.SQLState;

import com.splicemachine.db.iapi.sql.Activation;

import com.splicemachine.db.iapi.error.StandardException;

import com.splicemachine.db.catalog.UUID;

/**
 *	This class describes actions that are ALWAYS performed for a
 *	CREATE SCHEMA Statement at Execution time.
 *
 */

class CreateSchemaConstantAction extends DDLConstantAction
{

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
	CreateSchemaConstantAction(
								String			schemaName,
								String			aid)
	{
		this.schemaName = schemaName;
		this.aid = aid;
	}

	///////////////////////////////////////////////
	//
	// OBJECT SHADOWS
	//
	///////////////////////////////////////////////

	public	String	toString()
	{
		// Do not put this under SanityManager.DEBUG - it is needed for
		// error reporting.
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
	public void	executeConstantAction( Activation activation )
						throws StandardException
	{
		TransactionController tc = activation.
			getLanguageConnectionContext().getTransactionExecute();

		executeConstantActionMinion(activation, tc);
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
	public void	executeConstantAction(Activation activation,
									  TransactionController tc)
			throws StandardException {

		executeConstantActionMinion(activation, tc);
	}

	private void executeConstantActionMinion(Activation activation,
											 TransactionController tc)
			throws StandardException {

		LanguageConnectionContext lcc = activation.getLanguageConnectionContext();
		DataDictionary dd = lcc.getDataDictionary();
		DataDescriptorGenerator ddg = dd.getDataDescriptorGenerator();

		SchemaDescriptor sd = dd.getSchemaDescriptor(schemaName, lcc.getTransactionExecute(), false);

		//if the schema descriptor is an in-memory schema, we donot throw schema already exists exception for it.
		//This is to handle in-memory SESSION schema for temp tables
		if ((sd != null) && (sd.getUUID() != null))
		{
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
		if (thisAid == null)
		{
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

		sd = ddg.newSchemaDescriptor(schemaName,
									thisAid,
									tmpSchemaId);

		dd.addDescriptor(sd, null, DataDictionary.SYSSCHEMAS_CATALOG_NUM, false, tc, false);
	}
}
