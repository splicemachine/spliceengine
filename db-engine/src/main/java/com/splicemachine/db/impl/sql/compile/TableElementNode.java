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

package com.splicemachine.db.impl.sql.compile;

import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.sql.compile.C_NodeTypes;

/**
 * A TableElementNode is an item in a TableElementList, and represents
 * a single table element such as a column or constraint in a CREATE TABLE
 * or ALTER TABLE statement.
 *
 */

public class TableElementNode extends QueryTreeNode
{
    /////////////////////////////////////////////////////////////////////////
	//
	//	CONSTANTS
	//
	/////////////////////////////////////////////////////////////////////////

	public	static	final	int	AT_UNKNOWN						= 0;
	public	static	final	int	AT_ADD_FOREIGN_KEY_CONSTRAINT	= 1;
	public	static	final	int	AT_ADD_PRIMARY_KEY_CONSTRAINT	= 2;
	public	static	final	int	AT_ADD_UNIQUE_CONSTRAINT		= 3;
	public	static	final	int	AT_ADD_CHECK_CONSTRAINT			= 4;
	public	static	final	int	AT_DROP_CONSTRAINT				= 5;
	public	static	final	int	AT_MODIFY_COLUMN				= 6;
	public	static	final	int	AT_DROP_COLUMN					= 7;


	/////////////////////////////////////////////////////////////////////////
	//
	//	STATE
	//
	/////////////////////////////////////////////////////////////////////////

	String	name;
	int		elementType;	// simple element nodes can share this class,
							// eg., drop column and rename table/column/index
							// etc., no need for more classes, an effort to
							// minimize footprint

	/////////////////////////////////////////////////////////////////////////
	//
	//	BEHAVIOR
	//
	/////////////////////////////////////////////////////////////////////////

	/**
	 * Initializer for a TableElementNode
	 *
	 * @param name	The name of the table element, if any
	 */

	public void init(Object name)
	{
		this.name = (String) name;
	}

	/**
	 * Initializer for a TableElementNode
	 *
	 * @param name	The name of the table element, if any
	 */

	public void init(Object name, Object elementType)
	{
		this.name = (String) name;
		this.elementType = ((Integer) elementType).intValue();
	}

	/**
	 * Convert this object to a String.  See comments in QueryTreeNode.java
	 * for how this should be done for tree printing.
	 *
	 * @return	This object as a String
	 */

	public String toString()
	{
		if (SanityManager.DEBUG)
		{
			return "name: " + name + "\n" +
                "elementType: " + getElementType() + "\n" +
				super.toString();
		}
		else
		{
			return "";
		}
	}

	/**
	 * Does this element have a primary key constraint.
	 *
	 * @return boolean	Whether or not this element has a primary key constraint
	 */
	boolean hasPrimaryKeyConstraint()
	{
		return false;
	}

	/**
	 * Does this element have a unique key constraint.
	 *
	 * @return boolean	Whether or not this element has a unique key constraint
	 */
	boolean hasUniqueKeyConstraint()
	{
		return false;
	}

	/**
	 * Does this element have a foreign key constraint.
	 *
	 * @return boolean	Whether or not this element has a foreign key constraint
	 */
	boolean hasForeignKeyConstraint()
	{
		return false;
	}

	/**
	 * Does this element have a check constraint.
	 *
	 * @return boolean	Whether or not this element has a check constraint
	 */
	boolean hasCheckConstraint()
	{
		return false;
	}

	/**
	 * Does this element have a constraint on it.
	 *
	 * @return boolean	Whether or not this element has a constraint on it
	 */
	boolean hasConstraint()
	{
		return false;
	}

	/**
	 * Get the name from this node.
	 *
	 * @return String	The name.
	 */
	public String getName()
	{
		return name;
	}

	/**
	  *	Get the type of this table element.
	  *
	  *	@return	one of the constants at the front of this file
	  */
	int	getElementType()
	{
		if ( hasForeignKeyConstraint() ) { return AT_ADD_FOREIGN_KEY_CONSTRAINT; }
		else if ( hasPrimaryKeyConstraint() ) { return AT_ADD_PRIMARY_KEY_CONSTRAINT; }
		else if ( hasUniqueKeyConstraint() ) { return AT_ADD_UNIQUE_CONSTRAINT; }
		else if ( hasCheckConstraint() ) { return AT_ADD_CHECK_CONSTRAINT; }
		else if ( this instanceof ConstraintDefinitionNode ) { return AT_DROP_CONSTRAINT; }
		else if ( this instanceof ModifyColumnNode )
        {
            if ( getNodeType() == C_NodeTypes.DROP_COLUMN_NODE ) { return AT_DROP_COLUMN; }
            else { return AT_MODIFY_COLUMN; }
        }
		else { return elementType; }
	}

}
