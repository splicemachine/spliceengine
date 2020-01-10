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

package com.splicemachine.db.iapi.sql.dictionary;


import com.splicemachine.db.catalog.ReferencedColumns;
import com.splicemachine.db.catalog.UUID;

import com.splicemachine.db.iapi.services.sanity.SanityManager;
/**
 * This interface is used to get information from a SubCheckConstraintDescriptor.
 * A SubCheckConstraintDescriptor is used within the DataDictionary to 
 * get auxiliary constraint information from the system table
 * that is auxiliary to sysconstraints.
 *
 * @version 0.1
 */

public class SubCheckConstraintDescriptor extends SubConstraintDescriptor
{
	/** public interface to this class:
		<ol>
		<li>public String getConstraintText();</li>
		<li>public ReferencedColumns getReferencedColumnsDescriptor();</li>
		</ol>
	*/

	// Implementation
	private ReferencedColumns referencedColumns;
	private String						constraintText;

	/**
	 * Constructor for a SubCheckConstraintDescriptor
	 *
	 * @param constraintId		The UUID of the constraint.
	 * @param constraintText	The text of the constraint definition.
	 * @param referencedColumns	The columns referenced by the check constraint
	 */

	public SubCheckConstraintDescriptor(UUID constraintId, String constraintText,
									 ReferencedColumns referencedColumns)
	{
		super(constraintId);
		this.constraintText = constraintText;
		this.referencedColumns = referencedColumns;
	}

	/**
	 * Get the text of the check constraint definition.
	 *
	 * @return The text of the check constraint definition.
	 */
	public String getConstraintText()
	{
		return constraintText;
	}

	/**
	 * Get the ReferencedColumns.
	 *
	 * @return The ReferencedColumns.
	 */
	public ReferencedColumns getReferencedColumnsDescriptor()
	{
		return referencedColumns;
	}

	/**
	 * Does this constraint have a backing index?
	 *
	 * @return boolean	Whether or not there is a backing index for this constraint.
	 */
	public boolean hasBackingIndex()
	{
		return false;
	}

	/**
	 * Convert the SubCheckConstraintDescriptor to a String.
	 *
	 * @return	A String representation of this SubCheckConstraintDescriptor
	 */

	public String	toString()
	{
		if (SanityManager.DEBUG)
		{
			return "constraintText: " + constraintText + "\n" +
				super.toString();
		}
		else
		{
			return "";
		}
	}

}
