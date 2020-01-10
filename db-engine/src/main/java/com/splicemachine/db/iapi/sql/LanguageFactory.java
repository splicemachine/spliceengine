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

package com.splicemachine.db.iapi.sql;

import com.splicemachine.db.iapi.services.loader.ClassInspector;

/**
 * Factory interface for the Language.Interface protocol.
 * This is used via the Database API by users, and is presented
 * as a System Module (not a service module).  That could change,
 * but for now this is valid for any database. 
 *
 */
public interface LanguageFactory
{
	/**
		Used to locate this factory by the Monitor basic service.
		There needs to be a language factory per database.
	 */
	String MODULE = "com.splicemachine.db.iapi.sql.LanguageFactory";

	/**
	 * Get a ParameterValueSet
	 *
	 * @param numParms	The number of parameters in the
	 *			ParameterValueSet
	 * @param hasReturnParam	true if this parameter set
	 *			has a return parameter.  The return parameter
	 *			is always the 1st parameter in the list.  It
	 *			is due to a callableStatement like this: <i>
	 *			? = CALL myMethod()</i>
	 *
	 * @return	A new ParameterValueSet with the given number of parms
	 */
	ParameterValueSet newParameterValueSet(ClassInspector ci, int numParms, boolean hasReturnParam);

	/**
	 * Get a new result description from the input result
	 * description.  Picks only the columns in the column
	 * array from the inputResultDescription.
	 *
 	 * @param inputResultDescription the input rd
	 * @param theCols non null array of ints
	 *
	 * @return ResultDescription the rd
	 */
	ResultDescription getResultDescription
	(
			ResultDescription inputResultDescription,
			int[] theCols
	);

	/**
	 * Get a new result description
	 *
 	 * @param cols an array of col descriptors
	 * @param type the statement type
	 *
	 * @return ResultDescription the rd
	 */
	ResultDescription getResultDescription
	(
			ResultColumnDescriptor[] cols,
			String type
	);
}
