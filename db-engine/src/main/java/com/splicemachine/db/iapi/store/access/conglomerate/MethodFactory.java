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

package com.splicemachine.db.iapi.store.access.conglomerate;

import java.util.Properties;
import com.splicemachine.db.catalog.UUID;
import com.splicemachine.db.iapi.services.monitor.ModuleSupportable;

/**

  The interface of all access method factories.  Specific method factories
  (sorts, conglomerates), extend this interface.

**/

public interface MethodFactory extends ModuleSupportable
{
	/**
	Used to identify this interface when finding it with the Monitor.
	**/
	public static final String MODULE = 
	  "com.splicemachine.db.iapi.store.access.conglomerate.MethodFactory";

	/**
	Return the default properties for this access method.
	**/
	Properties defaultProperties();

	/**
	Return whether this access method implements the implementation
	type given in the argument string.
	**/
	boolean supportsImplementation(String implementationId);

	/**
	Return the primary implementation type for this access method.
	Although an access method may implement more than one implementation
	type, this is the expected one.  The access manager will put the
	primary implementation type in a hash table for fast access.
	**/
	String primaryImplementationType();

	/**
	Return whether this access method supports the format supplied in
	the argument.
	**/
	boolean supportsFormat(UUID formatid);

	/**
	Return the primary format that this access method supports.
	Although an access method may support more than one format, this
	is the usual one.  the access manager will put the primary format
	in a hash table for fast access to the appropriate method.
	**/
	UUID primaryFormat();
}

