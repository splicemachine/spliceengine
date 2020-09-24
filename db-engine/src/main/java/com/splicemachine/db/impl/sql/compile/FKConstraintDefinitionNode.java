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

package com.splicemachine.db.impl.sql.compile;

import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;
import com.splicemachine.db.iapi.sql.dictionary.SchemaDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.TableDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.ColumnDescriptor;
import com.splicemachine.db.iapi.sql.conn.Authorizer;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.sanity.SanityManager;

import com.splicemachine.db.iapi.reference.SQLState;

import com.splicemachine.db.impl.sql.execute.ConstraintInfo;

import com.splicemachine.db.iapi.util.ReuseFactory;

/**
 * A FKConstraintDefintionNode represents table constraint definitions.
 *
 */

public final class FKConstraintDefinitionNode extends ConstraintDefinitionNode
{
	TableName 			refTableName;
	ResultColumnList	refRcl;
	SchemaDescriptor	refTableSd;
	int                 refActionDeleteRule;  // referential action on  delete
	int                 refActionUpdateRule;  // referential action on update
	public void init(
						Object 			constraintName, 
						Object 			refTableName, 
						Object			fkRcl,
						Object			refRcl,
						Object          refActions)
	{
		super.init(
				constraintName,
				ReuseFactory.getInteger(DataDictionary.FOREIGNKEY_CONSTRAINT),
				fkRcl, 
				null,
				null,
				null);
		this.refRcl = (ResultColumnList) refRcl;
		this.refTableName = (TableName) refTableName;

		this.refActionDeleteRule = ((int[]) refActions)[0];
		this.refActionUpdateRule = ((int[]) refActions)[1];
	}

	/**
	 * Bind this constraint definition.  Figure out some
	 * information about the table we are binding against.
	 *
	 * @param dd DataDictionary
	 * 
	 * @exception StandardException on error
	 */
	protected void bind(DDLStatementNode ddlNode, DataDictionary dd)	throws StandardException
	{
		super.bind(ddlNode, dd);

		refTableSd = getSchemaDescriptor(refTableName.getSchemaName());

		if (refTableSd.isSystemSchema())
		{
			throw StandardException.newException(SQLState.LANG_NO_FK_ON_SYSTEM_SCHEMA);
		}

		// check the referenced table, unless this is a self-referencing constraint
		if (refTableName.equals(ddlNode.getObjectName()))
			return;

		// error when the referenced table does not exist
		TableDescriptor td = getTableDescriptor(refTableName.getTableName(), refTableSd);
		if (td == null)
			throw StandardException.newException(SQLState.LANG_INVALID_FK_NO_REF_TAB, 
												getConstraintMoniker(), 
												refTableName.getTableName());

		// Verify if REFERENCES_PRIV is granted to columns referenced
		getCompilerContext().pushCurrentPrivType(getPrivType());

		// Indicate that this statement has a dependency on the
		// table which is referenced by this foreign key:
		getCompilerContext().createDependency(td);

		// If references clause doesn't have columnlist, get primary key info
		if (refRcl.isEmpty() && (td.getPrimaryKey() != null))
		{
			// Get the primary key columns
			int[] refCols = td.getPrimaryKey().getReferencedColumns();
            for (int refCol : refCols) {
                ColumnDescriptor cd = td.getColumnDescriptor(refCol);
                // Set tableDescriptor for this column descriptor. Needed for adding required table
                // access permission. Column descriptors may not have this set already.
                cd.setTableDescriptor(td);
                if (isPrivilegeCollectionRequired())
                    getCompilerContext().addRequiredColumnPriv(cd);
            }

		}
		else
		{
			for (int i=0; i<refRcl.size(); i++)
			{
				ResultColumn rc = (ResultColumn) refRcl.elementAt(i);
				ColumnDescriptor cd = td.getColumnDescriptor(rc.getName());
				if (cd != null)
				{
					// Set tableDescriptor for this column descriptor. Needed for adding required table
					// access permission. Column descriptors may not have this set already.
					cd.setTableDescriptor(td);
					if (isPrivilegeCollectionRequired())
						getCompilerContext().addRequiredColumnPriv(cd);
				}
			}
		}
		getCompilerContext().popCurrentPrivType();
	}

	public ConstraintInfo getReferencedConstraintInfo()
	{
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(refTableSd != null, 
					"You must call bind() before calling getConstraintInfo");
		}
		return new ConstraintInfo(refTableName.getTableName(), refTableSd,
								  refRcl.getColumnNames(), refActionDeleteRule,
								  refActionUpdateRule);
	}

	public	TableName	getRefTableName() { return refTableName; }

	int getPrivType()
	{
		return Authorizer.REFERENCES_PRIV;
	}
}
