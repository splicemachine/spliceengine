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
 * All such Splice Machine modifications are Copyright 2012 - 2017 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */

package com.splicemachine.db.iapi.sql.dictionary;

import com.splicemachine.db.iapi.services.io.Formatable;

import com.splicemachine.db.iapi.error.StandardException;

/**
 *	This interface describes the columns in a referenced constraint. Added
 *	to be the protocol version of ConstraintInfo.
 *
 * @version 0.1
 */

public interface ConsInfo	extends	Formatable
{
	/**
	  *	This ConsInfo describes columns in a referenced table. What is
	  *	the schema that the referenced table lives in?
	  *
	  *	@param	dd	data dictionary to search for schema
	  *
	  *	@return	schema that referenced table lives in
	  *	@exception	StandardException thrown on oops
	  */
	public SchemaDescriptor getReferencedTableSchemaDescriptor(DataDictionary dd)
		throws StandardException;

	/**
	  *	This ConsInfo describes columns in a referenced table. What is
	  *	that table?
	  *
	  *	@param	dd	data dictionary to search for table
	  *
	  *	@return	referenced table
	  *	@exception	StandardException thrown on oops
	  */
	public TableDescriptor getReferencedTableDescriptor(DataDictionary dd)
		throws StandardException;

	/**
	  *	This ConsInfo describes columns in a referenced table. What are
	  *	their names?
	  *
	  *	@return	array of referenced column names
	  */
	public String[] getReferencedColumnNames();

	/**
	  *	Get the name of the table that these column live in.
	  *
	  *	@return	referenced table name
	  */
	public String getReferencedTableName();


	/**
	  *	Get the referential Action for an Update.
	  *
	  *	@return	referential Action for update
	  */

	public int getReferentialActionUpdateRule();
	
	/**
	  *	Get the referential Action for a Delete.
	  *
	  *	@return	referential Action Delete rule
	  */
	public int getReferentialActionDeleteRule();

}



