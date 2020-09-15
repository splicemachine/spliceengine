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

  * A Dependable is an in-memory representation of an object managed
  *	by the Dependency System.
  * 
  * There are two kinds of Dependables:
  * Providers and Dependents. Dependents depend on Providers and
  *	are responsible for executing compensating logic when their
  *	Providers change.
  * <P>
  * The fields represent the known Dependables.
  * <P>
  * Persistent dependencies (those between database objects) are
  * stored in SYS.SYSDEPENDS.
  *
  * @see com.splicemachine.db.catalog.DependableFinder
  */
public interface Dependable
{
	/*
	  *	Universe of known Dependables.
	  */

	String ALIAS						= "Alias";
	String CONGLOMERATE					= "Conglomerate";
	String CONSTRAINT					= "Constraint";
	String DEFAULT						= "Default";
	String HEAP							= "Heap";
	String INDEX						= "Index";
	String PREPARED_STATEMENT 			= "PreparedStatement";
	String ACTIVATION                   = "Activation";
	String FILE                         = "File";
	String STORED_PREPARED_STATEMENT	= "StoredPreparedStatement";
	String TABLE						= "Table";
	String COLUMNS_IN_TABLE				= "ColumnsInTable";
	String TRIGGER						= "Trigger";
	String VIEW							= "View";
	String SCHEMA						= "Schema";
	String TABLE_PERMISSION             = "TablePrivilege";
	String SCHEMA_PERMISSION            = "SchemaPrivilege";
	String COLUMNS_PERMISSION           = "ColumnsPrivilege";
	String ROUTINE_PERMISSION           = "RoutinePrivilege";
	String ROLE_GRANT                   = "RoleGrant";
    String SEQUENCE                     = "Sequence";
    String PERM                         = "Perm";


    /**
	  *	Get an object which can be written to disk and which,
	  *	when read from disk, will find or reconstruct this in-memory
	  * Dependable.
	  *
	  *	@return		A Finder object that can be written to disk if this is a
	  *					Persistent Dependable.
	  *				Null if this is not a persistent dependable.
	  */
	DependableFinder	getDependableFinder();


	/**
	  *	Get the name of this Dependable OBJECT. This is useful
	  *	for diagnostic messages.
	  *
	  *	@return	Name of Dependable OBJECT.
	  */
	String	getObjectName();


	/**
	  *	Get the UUID of this Dependable OBJECT.
	  *
	  *	@return	UUID of this OBJECT.
	  */
	UUID	getObjectID();


	/**
	  *	Return whether or not this Dependable is persistent. Persistent
	  *	dependencies are stored in SYS.SYSDEPENDS.
	  *
	  *	@return	true if this Dependable is persistent.
	  */
	boolean	isPersistent();


	/**
	  * Get the unique class id for the Dependable.
	  *	Every Dependable belongs to a class of Dependables.
	  *
	  *	@return	type of this Dependable.
	  */
	String	getClassType();
}
