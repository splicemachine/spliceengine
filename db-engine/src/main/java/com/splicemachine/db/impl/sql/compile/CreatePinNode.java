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

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.compile.CompilerContext;
import com.splicemachine.db.iapi.sql.compile.Visitor;
import com.splicemachine.db.iapi.sql.dictionary.ConglomerateDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.TableDescriptor;
import com.splicemachine.db.iapi.sql.execute.ConstantAction;

/**
 * Create Pin Node can Pin either a Table or a Schema in memory.  Schema is still a todo...  JL
 */

public class CreatePinNode extends DDLStatementNode  {
	private TableDescriptor td;

	 /**
	 * Initializer for a CreateTableNode for a base table
	 *
	 * @param tableName			Table to Pin
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void init(Object tableName) throws StandardException {
		initAndCheck(tableName);
	}

	/**
	 * Convert this object to a String.
	 *
	 * @return	This object as a String
	 */

	public String toString() {
        return String.format("schemaName=%s, tableName=%s",objectName.getSchemaName(),objectName.getTableName());
	}

	/**
	 * Prints the sub-nodes of this object.  See QueryTreeNode.java for
	 * how tree printing is supposed to work.
	 * @param depth		The depth to indent the sub-nodes
	 */
	public void printSubNodes(int depth) {
		// No Sub Nodes
	}


	public String statementToString() {
		return "PIN TABLE";
	}

	// We inherit the generate() method from DDLStatementNode.

	/**
	 * Bind this CreateTableNode.  This means doing any static error checking that can be
	 * done before actually creating the base table or declaring the global temporary table.
	 * For eg, verifying that the TableElementList does not contain any duplicate column names.
	 *
	 *
	 * @exception StandardException		Thrown on error
	 */

	public void bindStatement() throws StandardException {
		CompilerContext cc = getCompilerContext();

		td = getTableDescriptor();

		/* Get the base conglomerate descriptor */
		ConglomerateDescriptor cd = td.getConglomerateDescriptor(td.getHeapConglomerateId());

		/* Statement is dependent on the TableDescriptor and ConglomerateDescriptor */
		cc.createDependency(td);
		cc.createDependency(cd);
	}

	/**
	 * Create the Constant information that will drive the guts of Execution.
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public ConstantAction	makeConstantAction() throws StandardException {
           return getGenericConstantActionFactory().getPinTableConstantAction(
				   getSchemaDescriptor(td.getTableType() !=
								   TableDescriptor.GLOBAL_TEMPORARY_TABLE_TYPE,
						   true).getSchemaName(),
					getObjectName().getTableName());
	}

	/**
	 * Accept the visitor for all visitable children of this node.
	 * 
	 * @param v the visitor
	 */
    @Override
	public void acceptChildren(Visitor v) throws StandardException {
		super.acceptChildren(v);
	}
}
