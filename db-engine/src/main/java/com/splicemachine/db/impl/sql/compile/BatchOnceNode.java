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
import com.splicemachine.db.iapi.reference.ClassName;
import com.splicemachine.db.iapi.services.classfile.VMOpcode;
import com.splicemachine.db.iapi.services.compiler.MethodBuilder;
import com.splicemachine.db.iapi.services.io.FormatableIntHolder;
import com.splicemachine.db.iapi.sql.ResultColumnDescriptor;
import com.splicemachine.db.iapi.sql.compile.Visitor;
import java.util.Collection;

/**
 * BatchOnceNode replaces a ProjectRestrictNode below an update that would otherwise invoke a Subquery for each row from
 * the source.  BatchOnce reads multiple rows from the source then executes the subquery once.
 *
 * Example Query: update A set A.name = (select B.name from B where B.id=A.id)
 *
 * <pre>
 *  BEFORE:
 *
 * Update -> ProjectRestrict -> ChildResultSet
 *              \
 *              \
 *             Subquery
 *
 *  AFTER:
 *
 * Update -> BatchOnceNode -> ChildResultSet
 *              \
 *              \
 *             Subquery
 * </pre>
 */
public class BatchOnceNode extends SingleChildResultSetNode {

    private SubqueryNode subqueryNode;

    /* The column position in the source referenced by the correlated subquery column reference */
    private int[] sourceCorrelatedColumnPositions;

    /* The column position of the correlated column in the subquery result set */
    private int[] subqueryCorrelatedColumnPositions;


    /* Used by NodeFactory */
    public BatchOnceNode() {
    }

    @Override
    public void init(Object projectRestrictNode,
                     Object subqueryNode,
                     Object sourceCorrelatedColumnPosition,
                     Object subqueryCorrelatedColumnPosition
    ) {
        this.childResult = (ResultSetNode) projectRestrictNode;
        this.subqueryNode = (SubqueryNode) subqueryNode;
        this.sourceCorrelatedColumnPositions = (int[]) sourceCorrelatedColumnPosition;
        this.subqueryCorrelatedColumnPositions = (int[]) subqueryCorrelatedColumnPosition;
    }

    @Override
    protected void generate(ActivationClassBuilder acb, MethodBuilder mb) throws StandardException {

        // push: method call to get ResultSetFactory
        acb.pushGetResultSetFactoryExpression(mb);

        // push: parameters to method of ResultSetFactory
        this.childResult.generate(acb, mb);                      // ARG 1 - childResultSet
        acb.pushThisAsActivation(mb);                            // ARG 2 - activation
        mb.push(getCompilerContext().getNextResultSetNumber());  // ARG 3 - resultSetNumber
        this.subqueryNode.getResultSet().generate(acb, mb);      // ARG 4 - the subquery result set
        mb.push(acb.getRowLocationScanResultSetName());          // ARG 5 - name of the activation field containing update RS
        int sourceCorrelatedColumnItem=acb.addItem(FormatableIntHolder.getFormatableIntHolders(sourceCorrelatedColumnPositions));
        mb.push(sourceCorrelatedColumnItem);                 // ARG 6 - position of the correlated CR in the update source RS
        int subqueryCorrelatedColumnItem=acb.addItem(FormatableIntHolder.getFormatableIntHolders(subqueryCorrelatedColumnPositions));
        mb.push(subqueryCorrelatedColumnItem);               // ARG 7 - position of the correlated CR in subquery RS

        // push: method to invoke on ResultSetFactory
        mb.callMethod(VMOpcode.INVOKEINTERFACE, null, "getBatchOnceResultSet", ClassName.NoPutResultSet, 7);
    }


    @Override
    public ResultColumnDescriptor[] makeResultDescriptors() {
        return childResult.makeResultDescriptors();
    }

    @Override
    public ResultColumnList getResultColumns() {
        return this.childResult.getResultColumns();
    }

    @Override
    public void acceptChildren(Visitor v) throws StandardException {
        super.acceptChildren(v);
        subqueryNode = (SubqueryNode) subqueryNode.accept(v);
    }


    @Override
    public String printExplainInformation(String attrDelim, int order) throws StandardException {
        StringBuilder sb = new StringBuilder();
        sb.append(spaceToLevel())
                .append("BatchOnce").append("(")
                .append("n=").append(order)
                .append(attrDelim).append(getFinalCostEstimate().prettyProcessingString(attrDelim));
        sb.append(")");
        return sb.toString();
    }
    @Override
    public void buildTree(Collection<QueryTreeNode> tree, int depth) throws StandardException {
        setDepth(depth);
        tree.add(this);
        subqueryNode.buildTree(tree,depth+1);
        childResult.buildTree(tree,depth+1);
    }

}
