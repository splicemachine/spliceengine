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

package com.splicemachine.db.catalog;

/**
 *
 * An interface for describing an alias in Derby systems.
 * 
 * In a Derby system, an alias can be one of the following:
 * <ul>
 * <li>method alias
 * <li>UDT alias
 * <li>class alias
 * <li>synonym
 * <li>user-defined aggregate
 * </ul>
 *
 */
public interface AliasInfo
{
	/**
	 * Public statics for the various alias types as both char and String.
	 */
	char ALIAS_TYPE_UDT_AS_CHAR		= 'A';
	char ALIAS_TYPE_AGGREGATE_AS_CHAR		= 'G';
	char ALIAS_TYPE_PROCEDURE_AS_CHAR		= 'P';
	char ALIAS_TYPE_FUNCTION_AS_CHAR		= 'F';
	char ALIAS_TYPE_SYNONYM_AS_CHAR             = 'S';

	String ALIAS_TYPE_UDT_AS_STRING		= "A";
	String ALIAS_TYPE_AGGREGATE_AS_STRING		= "G";
	String ALIAS_TYPE_PROCEDURE_AS_STRING		= "P";
	String ALIAS_TYPE_FUNCTION_AS_STRING		= "F";
	String ALIAS_TYPE_SYNONYM_AS_STRING  		= "S";

	/**
	 * Public statics for the various alias name spaces as both char and String.
	 */
	char ALIAS_NAME_SPACE_UDT_AS_CHAR	= 'A';
	char ALIAS_NAME_SPACE_AGGREGATE_AS_CHAR	= 'G';
	char ALIAS_NAME_SPACE_PROCEDURE_AS_CHAR	= 'P';
	char ALIAS_NAME_SPACE_FUNCTION_AS_CHAR	= 'F';
	char ALIAS_NAME_SPACE_SYNONYM_AS_CHAR       = 'S';

	String ALIAS_NAME_SPACE_UDT_AS_STRING	= "A";
	String ALIAS_NAME_SPACE_AGGREGATE_AS_STRING	= "G";
	String ALIAS_NAME_SPACE_PROCEDURE_AS_STRING	= "P";
	String ALIAS_NAME_SPACE_FUNCTION_AS_STRING	= "F";
	String ALIAS_NAME_SPACE_SYNONYM_AS_STRING   = "S";

	/**
	 * Get the name of the static method that the alias 
	 * represents at the source database.  (Only meaningful for
	 * method aliases )
	 *
	 * @return The name of the static method that the alias 
	 * represents at the source database.
	 */
	String getMethodName();

	/**
	 * Return true if this alias is a Table Function.
	 */
	boolean isTableFunction();

}
