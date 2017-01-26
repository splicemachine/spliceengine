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

import com.splicemachine.db.catalog.DependableFinder;
import com.splicemachine.db.catalog.UUID;
import com.splicemachine.db.iapi.sql.depend.Dependent;
import com.splicemachine.db.iapi.sql.depend.Provider;

/**
 * DependencyDescriptor represents a persistent dependency between
 * SQL objects, such as a TRIGGER being dependent on a TABLE.
 * 
 * A DependencyDescriptor is stored in SYSDEPENDS as four
 * separate columms corresponding to the getters of this class. 
 * 
 * 
 */
public class DependencyDescriptor extends TupleDescriptor 
	implements UniqueTupleDescriptor
{
	/** public interface for this class is:
		<ol>
		<li>public DependableFinder getDependentFinder();</li>
		<li>public UUID getProviderID();</li>
		<li>public DependableFinder getProviderFinder();</li>
		</ol>
	*/

	// implementation
	private final UUID					dependentID;
	private final DependableFinder		dependentBloodhound;
	private final UUID					providerID;
	private final DependableFinder		providerBloodhound;

	/**
	 * Constructor for a DependencyDescriptor
	 *
	 * @param dependent			The Dependent
	 * @param provider			The Provider
	 */

	public DependencyDescriptor(
			Dependent dependent,
			Provider provider
			)
	{
		dependentID = dependent.getObjectID();
		dependentBloodhound = dependent.getDependableFinder();
		providerID = provider.getObjectID();
		providerBloodhound = provider.getDependableFinder();
	}

	/**
	 * Constructor for a DependencyDescriptor
	 *
	 * @param dependentID			The Dependent ID
	 * @param dependentBloodhound	The bloodhound for finding the Dependent
	 * @param providerID			The Provider ID
	 * @param providerBloodhound	The bloodhound for finding the Provider
	 */

	public DependencyDescriptor(
			UUID dependentID, DependableFinder dependentBloodhound,
			UUID providerID, DependableFinder providerBloodhound
			)
	{
		this.dependentID = dependentID;
		this.dependentBloodhound = dependentBloodhound;
		this.providerID = providerID;
		this.providerBloodhound = providerBloodhound;
	}

	// DependencyDescriptor interface

	/**
	 * Get the dependent's ID for the dependency.
	 *
	 * @return 	The dependent's ID.
	 */
	public UUID getUUID()
	{
		return dependentID;
	}

	/**
	 * Get the dependent's type for the dependency.
	 *
	 * @return The dependent's type.
	 */
	public DependableFinder getDependentFinder()
	{
		return dependentBloodhound;
	}

	/**
	 * Get the provider's ID for the dependency.
	 *
	 * @return 	The provider's ID.
	 */
	public UUID getProviderID()
	{
		return providerID;
	}

	/**
	 * Get the provider's type for the dependency.
	 *
	 * @return The provider's type.
	 */
	public DependableFinder getProviderFinder()
	{
		return providerBloodhound;
	}
}
