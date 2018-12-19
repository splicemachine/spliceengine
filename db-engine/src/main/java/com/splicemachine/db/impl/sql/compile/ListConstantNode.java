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
 * All such Splice Machine modifications are Copyright 2012 - 2018 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */

package com.splicemachine.db.impl.sql.compile;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.ClassName;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.services.classfile.VMOpcode;
import com.splicemachine.db.iapi.services.compiler.LocalField;
import com.splicemachine.db.iapi.services.compiler.MethodBuilder;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.sql.compile.Optimizable;
import com.splicemachine.db.iapi.sql.compile.Visitor;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.ListDataType;
import com.splicemachine.db.iapi.types.TypeId;

public final class ListConstantNode extends ConstantNode {
	
	ValueNodeList constantsList = null;
	
	public ValueNode getValue(int index) {
		if (index < 0 || index > constantsList.size())
			return null;
		return (ValueNode) constantsList.elementAt(index);
	}
	
	public int numConstants() {
		return constantsList.size();
	}
	
	@Override
	public boolean isConstantExpression() {
		final int listLen = constantsList.size();
		for (int i = 0; i < listLen; i++) {
			if (!((ValueNode) constantsList.elementAt(i)).isConstantExpression())
				return false;
		}
		return true;
	}
	
	public boolean containsAllConstantNodes() {
		
		final int listLen = constantsList.size();
		for (int i = 0; i < listLen; i++) {
			if (!(constantsList.elementAt(i) instanceof ConstantNode))
				return false;
		}
		return true;
	}
	
	/**
	 * Initializer for a ListConstantNode.
	 *
	 * @param arg1 A ListDataType containing the values of the constant.
	 * @throws StandardException
	 */
	public void init(Object arg1, Object arg2)
		throws StandardException {
		if (arg1 == null) {
			throw StandardException.newException(SQLState.LANG_INVALID_FUNCTION_ARGUMENT);
		} else if (arg1 instanceof ListDataType) {
			/* Fill in the type information in the parent ValueNode */
			super.init(TypeId.BOOLEAN_ID,
				((ListDataType) arg1).isNull(),
				Integer.MAX_VALUE);
			
			super.setValue((DataValueDescriptor) arg1);
		} else
			throw StandardException.newException(SQLState.LANG_INVALID_FUNCTION_ARGUMENT);
		
		if (arg2 == null || !(arg2 instanceof ValueNodeList))
			throw StandardException.newException(SQLState.LANG_INVALID_FUNCTION_ARGUMENT);
		
		constantsList = (ValueNodeList) arg2;
	}

	
	// Push a ListDataType DVD on the stack.
	@Override
	public void generateExpression
	(
		ExpressionClassBuilder acb,
		MethodBuilder mb
	) throws StandardException {
		/* Are we generating a SQL null value? */
		if (isNull()) {
			acb.generateNull(mb, getTypeCompiler(),
				getTypeServices());
		} else {
			// Build a new ListDataType, and place on the stack.
			int numValsInSet = this.numConstants();
			LocalField dvdField = PredicateList.generateListData(acb, numValsInSet);
			ValueNode dataLiteral;
			for (int constIdx = 0; constIdx < numValsInSet; constIdx++) {
				mb.getField(dvdField);
				dataLiteral = this.getValue(constIdx);
				dataLiteral.generateExpression(acb, mb);
				mb.upCast(ClassName.DataValueDescriptor);
				mb.push(constIdx);
				mb.callMethod(VMOpcode.INVOKEVIRTUAL,
					(String) null,
					"setFrom",
					"void", 2);
				
			}
			mb.getField(dvdField);
			mb.upCast(ClassName.DataValueDescriptor);
		}
	}
	
	
	/**
	 * Return the value as a string.
	 *
	 * @return The value as a string.
	 */
	String getValueAsString() {
		return value.toString();
	}
	
	
	@Override
	public double selectivity(Optimizable optTable) {
		double sel = 0;
		try {
			sel = value.getLength() * .0001;
		} catch (Exception e) {
			sel = .001;
		}
		if (sel > .2)
			sel = .2;
		if (sel < .001)
			sel = .001;
		return sel;  // TODO   Find a better formula for selectivity.
	}
	
	
	// We don't expect generateConstant to be called, because there is no
	// actual constant value associated with a ListDataType.  It is just
	// a logical grouping.  Instead, we override generateExpression which
	// calls generateConstant on all of the constants in the set.
	void generateConstant(ExpressionClassBuilder acb, MethodBuilder mb) {
	}
	
	
	public int hashCode() {
		final int prime = 37;
		int result = 17;
		
		for (int i = 0; i < numConstants(); i++) {
			result = result * prime + constantsList.elementAt(i).hashCode();
		}
		return result;
	}
	
	@Override
	public String toHTMLString() {
		return "value: " + getValueAsString() + "<br>" + super.toHTMLString();
	}
	
	
	/**
	 * Accept the visitor for all visitable children of this node.
	 *
	 * @param v the visitor
	 */
	@Override
	public void acceptChildren(Visitor v) throws StandardException {
		super.acceptChildren(v);
		
		if (constantsList != null) {
			constantsList = (ValueNodeList)constantsList.accept(v, this);
		}
	}
	
	@Override
	protected boolean isEquivalent(ValueNode o) throws StandardException {
		if (isSameNodeType(o)) {
			ListConstantNode other = (ListConstantNode) o;
			
			if (constantsList == other.constantsList)
				return true;
			
			if (constantsList.size() != other.constantsList.size())
				return false;
			
			for (int i = 0; i < constantsList.size(); i++) {
				if (!((ValueNode) constantsList.elementAt(i)).
					isEquivalent((ValueNode) other.constantsList.elementAt(i)))
					return false;
			}
			return true;
		}
		return false;
	}
	
	/**
	 * Prints the sub-nodes of this object.  See QueryTreeNode.java for
	 * how tree printing is supposed to work.
	 *
	 * @param depth The depth of this node in the tree
	 */
	
	public void printSubNodes(int depth) {
		if (SanityManager.DEBUG) {
			super.printSubNodes(depth);
			
			if (constantsList != null) {
				printLabel(depth, "Constants list: ");
				constantsList.treePrint(depth + 1);
			}
		}
	}
	
	/**
	 * Remap all ColumnReferences in this tree to be clones of the
	 * underlying expression.
	 *
	 * @return ValueNode            The remapped expression tree.
	 * @throws StandardException Thrown on error
	 */
	public ValueNode remapColumnReferencesToExpressions()
		throws StandardException {
		constantsList = constantsList.remapColumnReferencesToExpressions();
		return this;
	}
}