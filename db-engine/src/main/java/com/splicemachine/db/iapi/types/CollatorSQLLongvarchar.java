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

package com.splicemachine.db.iapi.types;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.sanity.SanityManager;

import java.text.RuleBasedCollator;

/**
 * CollatorSQLLongvarchar class differs from SQLLongvarchar based on how the 2 classes  
 * use different collations to collate their data. SQLLongvarchar uses Derby's 
 * default collation which is UCS_BASIC. Whereas, this class uses the 
 * RuleBasedCollator object that was passed to it in it's constructor and that 
 * RuleBasedCollator object decides the collation.
 * 
 * In Derby 10.3, this class will be passed a RuleBasedCollator which is based 
 * on the database's territory. In future releases of Derby, this class can be 
 * used to do other kinds of collations like case-insensitive collation etc by  
 * just passing an appropriate RuleBasedCollator object for that kind of 
 * collation.
 */
class CollatorSQLLongvarchar extends SQLLongvarchar implements CollationElementsInterface
{
	private WorkHorseForCollatorDatatypes holderForCollationSensitiveInfo;

	/*
	 * constructors
	 */


    /**
     * Create SQL LONG VARCHAR value initially set to NULL that
     * performs collation according to collatorForCharacterDatatypes 
     */
	CollatorSQLLongvarchar(RuleBasedCollator collatorForCharacterDatatypes)
	{
        setCollator(collatorForCharacterDatatypes);
	}
    
    /**
     * Create SQL LONG VARCHAR value initially set to value that
     * performs collation according to collatorForCharacterDatatypes 
     */
	CollatorSQLLongvarchar(String val, RuleBasedCollator collatorForCharacterDatatypes)
	{
		super(val);
		setCollator(collatorForCharacterDatatypes);
	}

	/**
	 * Set the RuleBasedCollator for this instance of CollatorSQLLongvarchar. 
	 * It will be used to do the collation.
	 */
	protected void setCollator(RuleBasedCollator collatorForCharacterDatatypes)
	{
		holderForCollationSensitiveInfo = 
			new WorkHorseForCollatorDatatypes(collatorForCharacterDatatypes, this);
	}

	/**
	 * Get the RuleBasedCollator for this instance of CollatorSQLLongvarchar. 
	 * It will be used to do the collation.
	 * 
	 * @return	The Collator object which should be used for collation 
	 * operation on this object
	 */
	protected RuleBasedCollator getCollatorForCollation() throws StandardException
	{
		return holderForCollationSensitiveInfo.getCollatorForCollation();
	}
	
	/** @see CollationElementsInterface#getCollationElementsForString */
	public int[] getCollationElementsForString() throws StandardException 
	{
		return holderForCollationSensitiveInfo.getCollationElementsForString();
	}

	/** @see CollationElementsInterface#getCountOfCollationElements */
	public int getCountOfCollationElements()
	{
		return holderForCollationSensitiveInfo.getCountOfCollationElements();
	}

	/*
	 * DataValueDescriptor interface
	 */

    /**
     * @see DataValueDescriptor#cloneValue
     */
    public DataValueDescriptor cloneValue(boolean forceMaterialization)
	{
		try
		{
			return new CollatorSQLLongvarchar(getString(), 
					holderForCollationSensitiveInfo.getCollatorForCollation());
		}
		catch (StandardException se)
		{
			if (SanityManager.DEBUG)
				SanityManager.THROWASSERT("Unexpected exception", se);
			return null;
		}
	}

	/**
	 * @see DataValueDescriptor#getNewNull
	 */
	public DataValueDescriptor getNewNull()
	{
		return new CollatorSQLLongvarchar(
				holderForCollationSensitiveInfo.getCollatorForCollation());
	}

	/**
	 * We do not anticipate this method on collation sensitive DVD to be
	 * ever called in Derby 10.3 In future, when Derby will start supporting
	 * SQL standard COLLATE clause, this method might get called on the
	 * collation sensitive DVDs.
	 *  
	 * @see StringDataValue#getValue(RuleBasedCollator) 
	 */
	public StringDataValue getValue(RuleBasedCollator collatorForComparison)
	{
		if (collatorForComparison != null)
		{
			//non-null collatorForComparison means use this collator sensitive
			//implementation of SQLLongvarchar
		    setCollator(collatorForComparison);
		    return this;			
		} else {
			//null collatorForComparison means use UCS_BASIC for collation.
			//For that, we need to use the base class SQLLongvarchar
			SQLLongvarchar s = new SQLLongvarchar();
			s.copyState(this);
			return s;
		}
	}
	
	/** @see SQLChar#stringCompare(SQLChar, SQLChar) */
	 protected int stringCompare(SQLChar char1, SQLChar char2)
	 throws StandardException
	 {
		 return holderForCollationSensitiveInfo.stringCompare(char1, char2);
	 }

    /**
      * Return a hash code that is consistent with
      * {@link #stringCompare(SQLChar, SQLChar)}.
      *
      * @return hash code
      */
     public int hashCode() {
         return hashCodeForCollation();
     }

	/**
	 * This method implements the like function for char (with no escape value).
	 * The difference in this method and the same method in superclass is that
	 * here we use special Collator object to do the comparison rather than
	 * using the Collator object associated with the default jvm locale.
	 *
	 * @param pattern		The pattern to use
	 *
	 * @return	A SQL boolean value telling whether the first operand is
	 *			like the second operand
	 *
	 * @exception StandardException		Thrown on error
	 */
	public BooleanDataValue like(DataValueDescriptor pattern)
								throws StandardException
	{
		return(holderForCollationSensitiveInfo.like(pattern));
	}
	
	/**
	 * This method implements the like function for char with an escape value.
	 * 
	 * @param pattern		The pattern to use
	 * 								 
	 * @return	A SQL boolean value telling whether the first operand is
	 * like the second operand
	 *
	 * @exception StandardException		Thrown on error
	 */
	public BooleanDataValue like(DataValueDescriptor pattern,
			DataValueDescriptor escape) throws StandardException
	{
		return(holderForCollationSensitiveInfo.like(pattern, escape));
	}
}
