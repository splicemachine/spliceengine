/*
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
 * Splice Machine, Inc. has modified this file.
 *
 * All Splice Machine modifications are Copyright 2012 - 2019 Splice Machine, Inc.,
 * and are licensed to you under the License; you may not use this file except in
 * compliance with the License.
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 */

package com.splicemachine.db.impl.sql.compile;

import com.splicemachine.db.catalog.types.TypeDescriptorImpl;
import com.splicemachine.db.iapi.services.classfile.VMOpcode;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.compiler.LocalField;
import com.splicemachine.db.iapi.services.compiler.MethodBuilder;
import com.splicemachine.db.iapi.sql.compile.Visitor;
import com.splicemachine.db.iapi.types.ArrayDataValue;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.TypeId;
import com.splicemachine.db.iapi.util.JBitSet;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;

/**
 * This node represents an Array Initialization Node.  It supports arrays in the format
 * [1,1,1] and will set them in the corresponding row to a SQLArray
 *
 *
 */

public class ArrayOperatorNode extends ValueNode {
	public int extractField = -1;
	public ValueNode operand;

	/**
	 * Initializer for a ExtractOperatorNode
	 *
	 * @param field   The field to extract
	 * @param operand The operand
	 */
	public void init(Object field, Object operand) throws StandardException {
		this.extractField = ((NumericConstantNode) field).value.getInt();
		this.operand = (ValueNode) operand;
	}

	/**
	 * Initializer for a ArrayNode
	 *
	 * @param functionName  Tells if the function was called with name COALESCE or with name VALUE
	 * @param argumentsList The list of arguments to the coalesce/value function
	 */
	public void init(Object argumentsList) {
	}

	/**
	 * Binding this expression means setting the result DataTypeServices.
	 * In this case, the result type is based on the rules in the table listed earlier.
	 *
	 * @param fromList        The FROM list for the statement.
	 * @param subqueryList    The subquery list being built as we find SubqueryNodes.
	 * @param aggregateVector The aggregate vector being built as we find AggregateNodes.
	 * @throws StandardException Thrown on error
	 * @return The new top of the expression tree.
	 */
	@Override
	public ValueNode bindExpression(FromList fromList,
									SubqueryList subqueryList,
									List<AggregateNode> aggregateVector) throws StandardException {
		//bind the operand
		operand = operand.bindExpression(fromList, subqueryList, aggregateVector);
		DataTypeDescriptor arrayDTD = operand.getTypeServices();
		TypeDescriptorImpl typeDescriptor = (TypeDescriptorImpl) ((TypeDescriptorImpl) arrayDTD.getCatalogType()).getChildren()[0];
		setType(new DataTypeDescriptor(
				TypeId.getBuiltInTypeId(typeDescriptor.getTypeId().getJDBCTypeId()),
				true
		));

		return this;
	}

	/**
	 * Do code generation for coalese/value
	 *
	 * @param acb The ExpressionClassBuilder for the class we're generating
	 * @param mb  The method the expression will go into
	 * @throws StandardException Thrown on error
	 */

	public void generateExpression(ExpressionClassBuilder acb,
								   MethodBuilder mb)
			throws StandardException {


		String resultTypeName = getTypeCompiler().interfaceName();

		String receiverType = ArrayDataValue.class.getCanonicalName();
		acb.generateNull(mb, getTypeCompiler(getTypeId()),
				getTypeServices());
		LocalField field = acb.newFieldDeclaration(Modifier.PRIVATE, resultTypeName);
		mb.setField(field);
		operand.generateExpression(acb, mb);
		mb.cast(receiverType);

		mb.push(extractField);
		mb.getField(field);
		mb.cast(DataValueDescriptor.class.getCanonicalName());
		mb.callMethod(VMOpcode.INVOKEINTERFACE, null,
				"arrayElement", DataValueDescriptor.class.getCanonicalName(), 2);
		mb.cast(resultTypeName);
	}

	/*
		print the non-node subfields
	 */
	public String toString() {
		if (SanityManager.DEBUG) {
			return
					"array: \n" +
							"element: " + extractField;
		} else {
			return "";
		}
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
			printLabel(depth, "array: " + extractField);
		}
	}


	/**
	 * {@inheritDoc}
	 */
	protected boolean isEquivalent(ValueNode o) throws StandardException {
		if (!isSameNodeType(o)) {
			return false;
		}

		ArrayOperatorNode other = (ArrayOperatorNode) o;

		return operand.isEquivalent(other.operand);

	}

	/**
	 * Accept the visitor for all visitable children of this node.
	 *
	 * @param v the visitor
	 */
	@Override
	public void acceptChildren(Visitor v) throws StandardException {
		super.acceptChildren(v);
		if (operand != null)
			operand = (ValueNode)operand.accept(v, this);
	}

	/**
	 * Categorize this predicate.
	 *
	 * @see ValueNode#categorize(JBitSet, boolean)
	 */
	public boolean categorize(JBitSet referencedTabs, boolean simplePredsOnly)
			throws StandardException {
		return operand.categorize(referencedTabs, simplePredsOnly);
	}

	/**
	 * Preprocess an expression tree.  We do a number of transformations
	 * here (including subqueries, IN lists, LIKE and BETWEEN) plus
	 * subquery flattening.
	 * NOTE: This is done before the outer ResultSetNode is preprocessed.
	 *
	 * @throws StandardException Thrown on error
	 * @param    numTables            Number of tables in the DML Statement
	 * @param    outerFromList        FromList from outer query block
	 * @param    outerSubqueryList    SubqueryList from outer query block
	 * @param    outerPredicateList    PredicateList from outer query block
	 * @return The modified expression
	 */
	public ValueNode preprocess(int numTables,
								FromList outerFromList,
								SubqueryList outerSubqueryList,
								PredicateList outerPredicateList)
			throws StandardException {
		operand.preprocess(numTables, outerFromList, outerSubqueryList, outerPredicateList);
		return this;
	}

	/**
	 * Remap all the {@code ColumnReference}s in this tree to be clones of
	 * the underlying expression.
	 *
	 * @return the remapped tree
	 * @throws StandardException if an error occurs
	 */
	public ValueNode remapColumnReferencesToExpressions()
			throws StandardException {
		operand.remapColumnReferencesToExpressions();
		return this;
	}

	public List getChildren() {
		return new ArrayList<>();
	}


	@Override
	public long nonZeroCardinality(long numberOfRows) throws StandardException {
		return numberOfRows; // No Cardinality Estimte for now...
	}

	@Override
	public List<ColumnReference> getHashableJoinColumnReference() {
		return operand.getHashableJoinColumnReference();
	}

	@Override
	public boolean checkCRLevel(int level){
		return operand.checkCRLevel(level);
	}

	@Override
	public void setHashableJoinColumnReference(ColumnReference cr) {
		if (operand instanceof ColumnReference)
			operand = cr;
		else
			operand.setHashableJoinColumnReference(cr);
	}


}
