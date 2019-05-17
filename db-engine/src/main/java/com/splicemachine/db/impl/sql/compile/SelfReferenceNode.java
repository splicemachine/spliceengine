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
 * All such Splice Machine modifications are Copyright 2012 - 2019 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */

package com.splicemachine.db.impl.sql.compile;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.ClassName;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.services.classfile.VMOpcode;
import com.splicemachine.db.iapi.services.compiler.LocalField;
import com.splicemachine.db.iapi.services.compiler.MethodBuilder;
import com.splicemachine.db.iapi.sql.compile.*;
import com.splicemachine.db.iapi.sql.dictionary.*;
import com.splicemachine.db.iapi.util.JBitSet;
import com.splicemachine.db.iapi.util.StringUtil;

import java.lang.reflect.Modifier;
import java.util.Collection;
import java.util.Enumeration;
import java.util.Objects;

/**
 * Created by yxia on 3/20/19.
 */
/* place holder for a FromSubquery, it is used for recursive with  */
public class SelfReferenceNode extends FromTable {

    // reference to a select query block, specifically, it points to the seed of a recursive query
    ResultSetNode subquery;
    // reference to the recursive query
    ResultSetNode recursiveUnionRoot;
    TableDescriptor viewDescriptor;
    LocalField selfReferenceResultSetRef;

    @Override
    public void init(
            Object subquery,
            Object root,
            Object correlationName,
            Object viewDescriptor,
            Object tableProperties) {
        super.init(correlationName, tableProperties);
        this.subquery = (ResultSetNode) subquery;
        this.viewDescriptor = (TableDescriptor)viewDescriptor;
        this.recursiveUnionRoot = (ResultSetNode)root;
        // the necessary privilege should be collected through the underlying subquery
        disablePrivilegeCollection();
    }

    @Override
    public void verifyProperties(DataDictionary dDictionary) throws StandardException {
        if(tableProperties==null){
            return;
        }
        
        Enumeration e = tableProperties.keys();
        while (e.hasMoreElements()) {
            String key = (String) e.nextElement();
            String value = (String) tableProperties.get(key);

            // currently only support joinStrategy, others will be ignored
            if (key.equals("joinStrategy")) {
                userSpecifiedJoinStrategy = StringUtil.SQLToUpperCase(value);
            } else {
                // No other "legal" values at this time
                throw StandardException.newException(SQLState.LANG_INVALID_FROM_TABLE_PROPERTY, key,
                        "joinStrategy");
            }
        }
    }


    public ResultSetNode bindNonVTITables(DataDictionary dataDictionary,
                                          FromList fromListParam)
            throws StandardException {
		/* Assign the tableNumber */
        if (tableNumber == -1)  // allow re-bind, in which case use old number
            tableNumber = getCompilerContext().getNextTableNumber();

        // we don't need to bind the referenced subquery, when we get here,
        // the referenced subquery should have been bound already
        // subquery = subquery.bindNonVTITables(dataDictionary, fromListParam);

        return this;
    }

    public ResultSetNode bindVTITables(FromList fromListParam)
            throws StandardException {
        // we don't need to bind the referenced subquery, when we get here,
        // the referenced subquery should have been bound already
        // subquery = subquery.bindVTITables(fromListParam);

        return this;
    }

    public void bindExpressions(FromList fromListParam) throws StandardException {
        ResultColumnList subqueryRCL = subquery.getResultColumns();
        /*
         * Create RCL based on subquery, adding a level of VCNs.
         */
        ResultColumnList newRcl = subqueryRCL.copyListAndObjects();
        if (getCompilerContext().isProjectionPruningEnabled())
            newRcl.genVirtualColumnNodes(subquery, subquery.getResultColumns(), false);
        else
            newRcl.genVirtualColumnNodes(subquery, subquery.getResultColumns());
        resultColumns = newRcl;

        if (viewDescriptor != null) {
            // set the column type using that from the view definition
            ColumnDescriptorList cdl = viewDescriptor.getColumnDescriptorList();
            int cdlSize = cdl.size();

            for (int index = 0; index < cdlSize; index++) {
                ColumnDescriptor colDesc = cdl.elementAt(index);
                resultColumns.elementAt(index).setType(colDesc.getType());
            }
        }

        return;
    }

    public ResultColumn getMatchingColumn(ColumnReference columnReference) throws StandardException {
        ResultColumn resultColumn = null;
        String columnsTableName;

        columnsTableName = columnReference.getTableName();

        if (columnReference.getGeneratedToReplaceAggregate())
        {
            resultColumn = resultColumns.getResultColumn(columnReference.getColumnName());
        } else if (columnsTableName == null || columnsTableName.equals(correlationName))
        {
            resultColumn = resultColumns.getAtMostOneResultColumn(columnReference, correlationName, false, true);
        }


        if (resultColumn != null) {
            columnReference.setTableNumber(tableNumber);
            columnReference.setColumnNumber(resultColumn.getColumnPosition());
        }

        return resultColumn;
    }

    public ResultSetNode preprocess(int numTables,
                                    GroupByList gbl,
                                    FromList fromList)
            throws StandardException {

		/* Generate the referenced table map */
        referencedTableMap = new JBitSet(numTables);
        referencedTableMap.set(tableNumber);

        return genProjectRestrict(numTables);
    }

    protected ResultSetNode genProjectRestrict(int numTables)
            throws StandardException{
		/* We get a shallow copy of the ResultColumnList and its
		 * ResultColumns.  (Copy maintains ResultColumn.expression for now.)
		 */
        ResultColumnList prRCList=resultColumns;
        resultColumns=resultColumns.copyListAndObjects();

		/* Replace ResultColumn.expression with new VirtualColumnNodes
		 * in the ProjectRestrictNode's ResultColumnList.  (VirtualColumnNodes include
		 * pointers to source ResultSetNode, this, and source ResultColumn.)
		 * NOTE: We don't want to mark the underlying RCs as referenced, otherwise
		 * we won't be able to project out any of them.
		 */
        prRCList.genVirtualColumnNodes(this,resultColumns,false);

		/* Project out any unreferenced columns.  If there are no referenced
		 * columns, generate and bind a single ResultColumn whose expression is 1.
		 */
        prRCList.doProjection(false);

        /* Finally, we create the new ProjectRestrictNode */
        return (ResultSetNode)getNodeFactory().getNode(
                C_NodeTypes.PROJECT_RESTRICT_NODE,
                this,
                prRCList,
                null,	/* Restriction */
                null,   /* Restriction as PredicateList */
                null,	/* Project subquery list */
                null,	/* Restrict subquery list */
                null,
                getContextManager());
    }

    @Override
    public CostEstimate estimateCost(OptimizablePredicateList predList,
                                     ConglomerateDescriptor cd,
                                     CostEstimate outerCost,
                                     Optimizer optimizer,
                                     RowOrdering rowOrdering) throws StandardException {
        costEstimate = subquery.costEstimate.cloneMe();

        getCurrentAccessPath().getJoinStrategy().estimateCost(this, predList, cd, outerCost, optimizer, costEstimate);


        return costEstimate;
    }

    @Override
    public boolean legalJoinOrder(JBitSet assignedTableMap) {
        // Only an issue for EXISTS FBTs and table converted from SSQ
        /* Have all of our dependencies been satisfied? */
        return dependencyMap==null || assignedTableMap.contains(dependencyMap);
    }


    public Visitable projectionListPruning(boolean considerAllRCs) throws StandardException {
        // we should not pruning the subquery based on references in the resultColumn list of this node,
        // as the subquery is just a reference to the leftside of the Union-all (the seed of the recursive view)
        return this;
    }

    @Override
    public void buildTree(Collection<QueryTreeNode> tree, int depth) throws StandardException {
        setDepth(depth);
        tree.add(this);
    }

    public String getExposedName(){
        if(correlationName!=null)
            return correlationName;
        else
            return getOrigTableName().getFullTableName();
    }

    @Override
    public String toHTMLString() {
        return "" +
                "resultSetNumber: " + getResultSetNumber() + "<br/>" +
                "level: " + getLevel() + "<br/>" +
                "correlationName: " + getCorrelationName() + "<br/>" +
                "corrTableName: " + Objects.toString(corrTableName) + "<br/>" +
                "tableNumber: " + getTableNumber() + "<br/>" +
                "existsTable: " + existsTable + "<br/>" +
                "dependencyMap: " + Objects.toString(dependencyMap) +
                super.toHTMLString();
    }

    @Override
    public String printExplainInformation(int order, boolean fromPlanPrinter) throws StandardException {
        return printExplainInformation(",", order, fromPlanPrinter);
    }

    @Override
    public String printExplainInformation(String attrDelim, int order) throws StandardException {
        return printExplainInformation(attrDelim, order, false);
    }

    public String printExplainInformation(String attrDelim, int order, boolean fromPlanPrinter) throws StandardException {
        StringBuilder sb = new StringBuilder();
        sb.append(spaceToLevel())
                .append("SelfReference").append("(")
                .append("n=").append(order)
                .append(attrDelim);

        sb.append(getFinalCostEstimate(false).prettyProcessingString(attrDelim));
        sb.append(",reference=").append((fromPlanPrinter?((UnionNode)recursiveUnionRoot).getStepNumInExplain():recursiveUnionRoot.getResultSetNumber()));
        sb.append(")");
        return sb.toString();
    }

    @Override
    public void generate(ActivationClassBuilder acb, MethodBuilder mb) throws StandardException {
        assignResultSetNumber();

        // Get our final cost estimate based on the child estimates.
        costEstimate=getFinalCostEstimate(false);

        // push in a local variable,
        selfReferenceResultSetRef = acb.newFieldDeclaration(Modifier.PRIVATE, ClassName.NoPutResultSet);

		/* Generate the SelfReferenceResultSet:
		 *  arg1: Activation
		 *  arg2: resultSetNumber
		 *  arg3: estimated row count
		 *  arg4: estimated cost
		 *  arg5: close method
		 */
        acb.pushGetResultSetFactoryExpression(mb);
        acb.pushThisAsActivation(mb);

        resultColumns.generateHolder(acb,mb);

        mb.push(resultSetNumber);
        mb.push(costEstimate.rowCount());
        mb.push(costEstimate.getEstimatedCost());
        mb.push(printExplainInformationForActivation());

        mb.callMethod(VMOpcode.INVOKEINTERFACE,null,"getSelfReferenceResultSet", ClassName.NoPutResultSet,6);

        mb.setField(selfReferenceResultSetRef);
        mb.getField(selfReferenceResultSetRef);
    }

    @Override
    public ResultColumnList getAllResultColumns(TableName allTableName) throws StandardException{
        return getResultColumnsForList(allTableName,resultColumns,
                getOrigTableName());
    }

    @Override
    void adjustForSortElimination(){
    }

    @Override
    void adjustForSortElimination(RequiredRowOrdering rowOrdering) throws StandardException{
    }

    public LocalField getResultSetRef() {
        return selfReferenceResultSetRef;
    }

    public ResultSetNode getRecursiveUnionRoot() {
        return recursiveUnionRoot;
    }
}
