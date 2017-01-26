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


import com.splicemachine.db.catalog.UUID;
import com.splicemachine.db.iapi.services.sanity.SanityManager;

/**
 * This interface is used to get information from a SubKeyConstraintDescriptor.
 * A SubKeyConstraintDescriptor is used within the DataDictionary to 
 * get auxiliary constraint information from the system table
 * that is auxiliary to sysconstraints.
 *
 * @version 0.1
 */

public class SubKeyConstraintDescriptor extends SubConstraintDescriptor
{
	/** Interface for SubKeyConstraintDescriptor is 
		<ol>
		<li>public UUID getIndexId();</li>
		<li>public UUID getKeyConstraintId();</li>
		</ol>
	*/

	// Implementation
	UUID					indexId;
	UUID					keyConstraintId;

	int                     raDeleteRule; //referential action rule for a DELETE 
	int                     raUpdateRule; //referential action rule for a UPDATE


	/**
	 * Constructor for a SubConstraintDescriptorImpl
	 *
	 * @param constraintId		The UUID of the constraint.
	 * @param indexId			The UUID of the backing index.
	 */
	public SubKeyConstraintDescriptor(UUID constraintId, UUID indexId)
	{
		super(constraintId);
		this.indexId = indexId;
	}

	/**
	 * Constructor for a SubConstraintDescriptor
	 *
	 * @param constraintId		The UUID of the constraint.
	 * @param indexId			The UUID of the backing index.
	 * @param keyConstraintId	The UUID of the referenced constraint (fks)
	 */
	public SubKeyConstraintDescriptor(UUID constraintId, UUID indexId, UUID keyConstraintId)
	{
		this(constraintId, indexId);
		this.keyConstraintId = keyConstraintId;
	}


	/**
	 * Constructor for a SubConstraintDescriptor
	 *
	 * @param constraintId		The UUID of the constraint.
	 * @param indexId			The UUID of the backing index.
	 * @param keyConstraintId	The UUID of the referenced constraint (fks)
	 * @param raDeleteRule      The referential action for delete
	 * @param raUpdateRule      The referential action for update
	 */
	public SubKeyConstraintDescriptor(UUID constraintId, UUID indexId, UUID
									  keyConstraintId, int raDeleteRule, int raUpdateRule)
	{
		this(constraintId, indexId);
		this.keyConstraintId = keyConstraintId;
		this.raDeleteRule = raDeleteRule;
		this.raUpdateRule = raUpdateRule;
	}





	/**
	 * Gets the UUID of the backing index.
	 *
	 * @return	The UUID of the backing index.
	 */
	public UUID	getIndexId()
	{
		return indexId;
	}

	/**
	 * Gets the UUID of the referenced key constraint
	 *
	 * @return	The UUID of the referenced key constraint
	 */
	public UUID	getKeyConstraintId()
	{
		return keyConstraintId;
	}

	/**
	 * Does this constraint have a backing index?
	 *
	 * @return boolean	Whether or not there is a backing index for this constraint.
	 */
	public boolean hasBackingIndex()
	{
		return true;
	}

	/**
	 * Gets a referential action rule on a  DELETE
	 * @return referential rule defined by the user during foreign key creattion
	 * for a delete (like CASCDE , RESTRICT ..etc)
	 */
	public int	getRaDeleteRule()
	{
		return raDeleteRule;
	}
	
	
	/**
	 * Gets a referential action rule on a UPDATE
	 * @return referential rule defined by the user during foreign key creattion
	 * for an UPDATE (like CASCDE , RESTRICT ..etc)
	 */
	public int	getRaUpdateRule()
	{
		return raUpdateRule;
	}
	


	/**
	 * Convert the SubKeyConstraintDescriptor to a String.
	 *
	 * @return	A String representation of this SubConstraintDescriptor
	 */

	public String	toString()
	{
		if (SanityManager.DEBUG)
		{
			return "indexId: " + indexId + "\n" +
				"keyConstraintId: " + keyConstraintId + "\n" +
				"raDeleteRule: " + raDeleteRule + "\n" +
				"raUpdateRule: " + raUpdateRule + "\n" +
				super.toString();
		}
		else
		{
			return "";
		}
	}

}
