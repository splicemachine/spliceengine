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

import com.splicemachine.db.iapi.sql.depend.ProviderList;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.services.compiler.MethodBuilder;
import com.splicemachine.db.iapi.services.sanity.SanityManager;

import com.splicemachine.db.iapi.error.StandardException;

import java.util.LinkedList;
import java.util.List;
import java.util.Vector;

/**
 * This node describes a Generation Clause in a column definition.
 *
 */
public class GenerationClauseNode extends ValueNode
{
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTANTS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // STATE
    //
    ///////////////////////////////////////////////////////////////////////////////////

    private ValueNode _generationExpression;
    private String      _expressionText;

    private ValueNode _boundExpression;
	private ProviderList _apl;

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // INITIALIZATION
    //
    ///////////////////////////////////////////////////////////////////////////////////


	public void init( Object generationExpression, Object expressionText )
    {
        _generationExpression = (ValueNode) generationExpression;
        _expressionText = (String) expressionText;
	}

    ///////////////////////////////////////////////////////////////////////////////////
    //
    //  ACCESSORS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /** Get the defining text of this generation clause */
    public  String  getExpressionText() { return _expressionText; }
    
	/** Set the auxiliary provider list. */
	void setAuxiliaryProviderList(ProviderList apl) { _apl = apl; }

	/** Return the auxiliary provider list. */
	public ProviderList getAuxiliaryProviderList() { return _apl; }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // QueryTreeNode BEHAVIOR
    //
    ///////////////////////////////////////////////////////////////////////////////////

	/**
	 * Binding the generation clause.
	 */
    @Override
	public ValueNode bindExpression(FromList fromList,
                                    SubqueryList subqueryList,
                                    List<AggregateNode>	aggregateVector ) throws StandardException {
        _boundExpression = _generationExpression.bindExpression( fromList, subqueryList, aggregateVector );

        return _boundExpression;
	}

	/**
	 * Generate code for this node.
	 *
	 * @param acb	The ExpressionClassBuilder for the class being built
	 * @param mb	The method the code to place the code
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void generateExpression(ExpressionClassBuilder acb,
											MethodBuilder mb)
									throws StandardException
	{
        throw StandardException.newException( SQLState.HEAP_UNIMPLEMENTED_FEATURE );
	}

	protected boolean isEquivalent(ValueNode other)
		throws StandardException
    {
        if ( !( other instanceof GenerationClauseNode) ) { return false; }

        GenerationClauseNode    that = (GenerationClauseNode) other;

        return this._generationExpression.isEquivalent( that._generationExpression );
    }
    
	/**
	 * Return a vector of columns referenced in the generation expression.
	 *
	 * @exception StandardException		Thrown on error
	 */
    public Vector findReferencedColumns()
        throws StandardException
    {
        CollectNodesVisitor visitor = new CollectNodesVisitor( ColumnReference.class );

        _generationExpression.accept( visitor );

        Vector result = visitor.getList();

        if ( result == null ) { result = new Vector(); }

        return result;
    }

	/*
		Stringify.
	 */
	public String toString()
    {
        return
            "expressionText: GENERATED ALWAYS AS ( " +
            _expressionText + " )\n" +
            super.toString();
	}
        

    /**
	 * Prints the sub-nodes of this object.  See QueryTreeNode.java for
	 * how tree printing is supposed to work.
	 *
	 * @param depth		The depth of this node in the tree
	 */
	public void printSubNodes(int depth)
	{
		if (SanityManager.DEBUG)
		{
			super.printSubNodes(depth);

            printLabel(depth, "generationExpression: ");
            _generationExpression.treePrint(depth + 1);

            if (_boundExpression != null) {
                printLabel(depth, "boundExpression. ");
                _boundExpression.treePrint(depth + 1);
            }
		}
	}

	public List getChildren() {

		return new LinkedList(){{
			add(_generationExpression);
			add(_boundExpression);
		}};
	}

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // MINIONS
    //
    ///////////////////////////////////////////////////////////////////////////////////

}
