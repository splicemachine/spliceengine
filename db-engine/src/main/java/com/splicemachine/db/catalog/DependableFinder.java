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

package com.splicemachine.db.catalog;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;

/**
	
  A DependableFinder is an object that can find an in-memory
  Dependable, given the Dependable's ID.
  
  
  <P>
  The DependableFinder is able to write itself to disk and,
  once read back into memory, locate the in-memory Dependable that it
  represents.

  <P>
  DependableFinder objects are stored in SYS.SYSDEPENDS to record
  dependencies between database objects.
  */
public interface DependableFinder
{
	/**
	  *	Get the in-memory object associated with the passed-in object ID.
	  *
      * @param dd DataDictionary to use for lookup.
	  *	@param	dependableObjectID the ID of a Dependable. Used to locate that Dependable.
	  *
	  *	@return	the associated Dependable
	  * @exception StandardException		thrown if the object cannot be found or on error o
	  */
    public	Dependable	getDependable(DataDictionary dd,
            UUID dependableObjectID) throws StandardException;

	/**
	  * The name of the class of Dependables as a "SQL Object" which this
	  * Finder can find.
	  * This is a value like "Table" or "View".
	  *	Every DependableFinder can find some class of Dependables. 
	  *
	  *
	  *	@return	String type of the "SQL Object" which this Finder can find.
	  * @see Dependable
	  */
	public	String	getSQLObjectType();
}
