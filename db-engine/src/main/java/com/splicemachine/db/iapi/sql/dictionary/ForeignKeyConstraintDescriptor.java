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
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.sql.StatementType;

/**
 * A foreign key.
 *
 */
public class ForeignKeyConstraintDescriptor extends KeyConstraintDescriptor
{
	/**
	   interface to this descriptor
	   <ol>
	   <li>public ReferencedKeyConstraintDescriptor getReferencedConstraint()
	   throws StandardException;
	   <li>public UUID getReferencedConstraintId()  
	   throws StandardException;
	   <li>public boolean isSelfReferencingFK()
	   throws StandardException;
	   <ol>
	*/

	// Implementation
	ReferencedKeyConstraintDescriptor	referencedConstraintDescriptor;
	UUID								referencedConstraintId;
	int                                 raDeleteRule;
	int                                 raUpdateRule;
	/**
	 * Constructor for a ForeignKeyConstraintDescriptor
	 *
	 * @param dataDictionary		The data dictionary that this descriptor lives in
	 * @param table		The descriptor of the table the constraint is on
	 * @param constraintName	The name of the constraint.
	 * @param deferrable		If the constraint can be deferred.
	 * @param initiallyDeferred If the constraint starts life deferred.
	 * @param fkColumns 			columns in the foreign key
	 * @param constraintId		UUID of constraint
	 * @param indexId			The UUID for the backing index
	 * @param schemaDesc		The SchemaDescriptor for the constraint
	 * @param referencedConstraintDescriptor	is referenced constraint descriptor
	 * @param isEnabled			is the constraint enabled?
	 */
	protected ForeignKeyConstraintDescriptor(
		    DataDictionary dataDictionary,
			TableDescriptor table,
			String constraintName,
			boolean deferrable,
			boolean initiallyDeferred,
			int[] fkColumns,
			UUID constraintId,
			UUID indexId,
			SchemaDescriptor schemaDesc,
			ReferencedKeyConstraintDescriptor referencedConstraintDescriptor,
			boolean isEnabled,
			int raDeleteRule,
			int raUpdateRule
			)							
	{
		super(dataDictionary, table, constraintName, deferrable,
			  initiallyDeferred, fkColumns,
			  constraintId, indexId, schemaDesc, isEnabled);

		this.referencedConstraintDescriptor = referencedConstraintDescriptor;
		this.raDeleteRule = raDeleteRule;
		this.raUpdateRule = raUpdateRule;
	}

	/**
	 * Constructor for a ForeignKeyConstraintDescriptor
	 *
	 * @param dataDictionary		The data dictionary that this descriptor lives in
	 * @param table		The descriptor of the table the constraint is on
	 * @param constraintName	The name of the constraint.
	 * @param deferrable		If the constraint can be deferred.
	 * @param initiallyDeferred If the constraint starts life deferred.
	 * @param fkColumns 			columns in the foreign key
	 * @param constraintId		UUID of constraint
	 * @param indexId			The UUID for the backing index
	 * @param schemaDesc		The SchemaDescriptor for the constraint
	 * @param referencedConstraintId	is referenced constraint id
	 * @param isEnabled			is the constraint enabled?
	 */
	ForeignKeyConstraintDescriptor(
		    DataDictionary dataDictionary,
			TableDescriptor table,
			String constraintName,
			boolean deferrable,
			boolean initiallyDeferred,
			int[] fkColumns,
			UUID constraintId,
			UUID indexId,
			SchemaDescriptor schemaDesc,
			UUID referencedConstraintId,
			boolean isEnabled,
			int raDeleteRule,
			int raUpdateRule
			)							
	{
		super(dataDictionary, table, constraintName, deferrable,
			  initiallyDeferred, fkColumns,
			  constraintId, indexId, schemaDesc, isEnabled);
		this.referencedConstraintId = referencedConstraintId;
		this.raDeleteRule = raDeleteRule;
		this.raUpdateRule = raUpdateRule;

	}

	/**
	 * Get the constraint that this FK references.  Will
	 * return either a primary key or a unique key constriant.
	 *
	 * @return	the constraint
	 *
	 * @exception StandardException on error
	 */
	public ReferencedKeyConstraintDescriptor getReferencedConstraint() 
		throws StandardException
	{
		if (referencedConstraintDescriptor != null)
		{
			return referencedConstraintDescriptor;
		}

		if (referencedConstraintId == null)
		{
			getReferencedConstraintId();
		}

		TableDescriptor refTd = getDataDictionary().getConstraintTableDescriptor(referencedConstraintId);

		if (SanityManager.DEBUG)
		{
			if (refTd == null)
			{
				SanityManager.THROWASSERT("not able to find "+referencedConstraintId+
							" in SYS.SYSCONSTRAINTS");
			}
		}

		ConstraintDescriptorList cdl = getDataDictionary().getConstraintDescriptors(refTd);
		referencedConstraintDescriptor = (ReferencedKeyConstraintDescriptor)
									cdl.getConstraintDescriptorById(referencedConstraintId);

		if (SanityManager.DEBUG)
		{
			if (referencedConstraintDescriptor == null)
			{
				SanityManager.THROWASSERT("not able to find "
					+referencedConstraintDescriptor+ " off of table descriptor "
					+refTd.getName());
			}
		}

		return referencedConstraintDescriptor;
	}

	
	/**
	 * Get the constraint id for the constraint that this FK references.  
	 * Will return either a primary key or a unique key constriant.
	 *
	 * @return	the constraint id
	 *
	 * @exception StandardException on error
	 */
	public UUID getReferencedConstraintId()  throws StandardException
	{
		if (referencedConstraintDescriptor != null)
		{
			return referencedConstraintDescriptor.getUUID();
		}

		SubKeyConstraintDescriptor subKey;
		subKey = getDataDictionary().getSubKeyConstraint(constraintId,
										DataDictionary.FOREIGNKEY_CONSTRAINT);
		if (SanityManager.DEBUG)
		{
			if (subKey == null)
			{
				SanityManager.THROWASSERT("not able to find "+constraintName+
							" in SYS.SYSFOREIGNKEYS");
			}
		}
		referencedConstraintId = subKey.getKeyConstraintId();
		return referencedConstraintId;
	}

	/**
	 * Gets an identifier telling what type of descriptor it is
	 * (UNIQUE, PRIMARY KEY, FOREIGN KEY, CHECK).
	 *
	 * @return	An identifier telling what type of descriptor it is
	 *		(UNIQUE, PRIMARY KEY, FOREIGN KEY, CHECK).
	 */
	public int	getConstraintType()
	{
		return DataDictionary.FOREIGNKEY_CONSTRAINT;
	}

	/**
	 * Does this constraint need to fire on this type of
	 * DML?  True if insert or update and columns intersect
	 *
	 * @param stmtType	the type of DML 
	 * (StatementType.INSERT|StatementType.UPDATE|StatementType.DELETE)
	 * @param modifiedCols	the columns modified, or null for all
	 *
	 * @return true/false
	 */
	public boolean needsToFire(int stmtType, int[] modifiedCols)
	{
		/*
		** If we are disabled, we never fire
		*/
		if (!isEnabled)
		{
			return false;
		}

		if (stmtType == StatementType.DELETE)
		{
			return false;
		}
		if (stmtType == StatementType.INSERT)
		{
			return true;
		}

		// if update, only relevant if columns intersect
		return doColumnsIntersect(modifiedCols, getReferencedColumns());
	}

	/**
	 * Am I a self-referencing FK?  True if my referenced
	 * constraint is on the same table as me.
	 *
	 * @return	true/false
	 *
	 * @exception StandardException on error
	 */
	public boolean isSelfReferencingFK()
		throws StandardException
	{
		ReferencedKeyConstraintDescriptor refcd = getReferencedConstraint();
		return (refcd.getTableId().equals(getTableId()));
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
	
}







