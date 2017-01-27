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

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.compiler.MethodBuilder;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.sql.ResultColumnDescriptor;
import com.splicemachine.db.iapi.sql.compile.CostEstimate;
import com.splicemachine.db.iapi.sql.compile.OptimizablePredicateList;
import com.splicemachine.db.iapi.sql.compile.Optimizer;
import com.splicemachine.db.iapi.sql.compile.RowOrdering;
import com.splicemachine.db.iapi.sql.dictionary.ConglomerateDescriptor;

public class OrderByNode extends SingleChildResultSetNode {
    OrderByList		orderByList;
    @Override
    public boolean isParallelizable() {
        return true; //represented by a sort operation
    }



    /**
     * Initializer for a OrderByNode.
     *
     * @param childResult	The child ResultSetNode
     * @param orderByList	The order by list.
     * @param tableProperties	Properties list associated with the table
     *
     * @exception StandardException		Thrown on error
     */
    public void init(
            Object childResult,
            Object orderByList,
            Object tableProperties) throws StandardException {
        ResultSetNode child = (ResultSetNode) childResult;

        super.init(childResult, tableProperties);

        this.orderByList = (OrderByList) orderByList;

        ResultColumnList prRCList;

		/*
			We want our own resultColumns, which are virtual columns
			pointing to the child result's columns.

			We have to have the original object in the distinct node,
			and give the underlying project the copy.
		 */

		/* We get a shallow copy of the ResultColumnList and its
		 * ResultColumns.  (Copy maintains ResultColumn.expression for now.)
		 */
        prRCList = child.getResultColumns().copyListAndObjects();
        resultColumns = child.getResultColumns();
        child.setResultColumns(prRCList);

		/* Replace ResultColumn.expression with new VirtualColumnNodes
		 * in the DistinctNode's RCL.  (VirtualColumnNodes include
		 * pointers to source ResultSetNode, this, and source ResultColumn.)
		 */
        resultColumns.genVirtualColumnNodes(this, prRCList);
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

            if (orderByList != null)
            {
                printLabel(depth, "orderByList: ");
                orderByList.treePrint(depth + 1);
            }
        }
    }
    @Override
    public ResultColumnDescriptor[] makeResultDescriptors() {
        return childResult.makeResultDescriptors();
    }

    @Override
    public CostEstimate getFinalCostEstimate() throws StandardException{
        if(costEstimate==null) {
            costEstimate = childResult.getFinalCostEstimate().cloneMe();
            orderByList.estimateCost(optimizer, null, costEstimate);
        }
        return costEstimate;
    }

    @Override
    public void generate(ActivationClassBuilder acb, MethodBuilder mb) throws StandardException {
        // Get the cost estimate for the child
        if (costEstimate == null) {
            costEstimate = getFinalCostEstimate();
        }

        orderByList.generate(acb, mb, this, childResult,costEstimate);

        // We need to take note of result set number if ORDER BY is used in a
        // subquery for the case where a PRN is inserted in top of the select's
        // PRN to project away a sort column that is not part of the select
        // list, e.g.
        //
        //     select * from (select i from t order by j desc) s
        //
        // If the resultSetNumber is not correctly set in our resultColumns,
        // code generation for the PRN above us will fail when calling
        // resultColumns.generateCore -> VCN.generateExpression, cf. the Sanity
        // assert in VCN.generateExpression on sourceResultSetNumber >= 0.
        resultSetNumber = orderByList.getResultSetNumber();
        resultColumns.setResultSetNumber(resultSetNumber);
    }

    @Override
    protected CostEstimate getCostEstimate(Optimizer optimizer) {
        return super.getCostEstimate(optimizer);
    }

    @Override
    public CostEstimate estimateCost(OptimizablePredicateList predList,
                                     ConglomerateDescriptor cd, CostEstimate outerCost,
                                     Optimizer optimizer, RowOrdering rowOrdering)
            throws StandardException {
        return super.estimateCost(predList,cd,outerCost,optimizer,rowOrdering);
    }

    @Override
    public String printExplainInformation(String attrDelim, int order) throws StandardException {
        StringBuilder sb = new StringBuilder();
        sb = sb.append(spaceToLevel())
                .append("OrderBy").append("(")
                .append("n=").append(order);
        sb.append(attrDelim).append(costEstimate.prettyProcessingString(attrDelim));
        sb = sb.append(")");
        return sb.toString();
    }

}
