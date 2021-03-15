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
import com.splicemachine.db.iapi.sql.compile.AccessPath;
import com.splicemachine.db.iapi.sql.compile.Optimizable;
import com.splicemachine.db.iapi.sql.compile.Optimizer;
import com.splicemachine.db.impl.ast.RSUtils;

import static com.splicemachine.db.impl.sql.compile.ColumnReference.isBaseRowIdOrRowId;

/**
 * Predicate Selectivity Computation
 *
 */
public class DefaultPredicateSelectivity extends AbstractSelectivityHolder {
    private final Optimizable baseTable;
    private final double selectivityFactor;
    private final Optimizer optimizer;
    public DefaultPredicateSelectivity(Predicate p, Optimizable baseTable, QualifierPhase phase, double selectivityFactor, Optimizer optimizer){
        super(false,0,phase,p);
        this.p = p;
        this.baseTable = baseTable;
        this.selectivityFactor = selectivityFactor;
        this.optimizer = optimizer;
    }

    public double getSelectivity() throws StandardException {
        if (selectivity == -1.0d) {
            if (isRowIdBinding() && optimizer != null && optimizer.getOuterTable() != null)
                selectivity = getRowIdPredSelectivity();
            else
                selectivity = p.selectivity(baseTable);
            if (selectivityFactor > 0) // we may hint to adjust selectivity by a factor
                selectivity *= selectivityFactor;
        }
        return selectivity;
    }

    // A ROWID = ROWID join is driven from the outer table, which reduces the number of rows qualified.
    // Find the number of qualified outer table rows to use as a reduction ratio to get the inner table selectivity.
    private double getRowIdPredSelectivity() {
        double outerRowCount = optimizer.getOuterTable().getCurrentAccessPath().getCostEstimate().rowCount();
        double innerRowCount = ((FromBaseTable)baseTable).getSingleScanRowCount();
        double lesserRowCount = Double.min(outerRowCount, innerRowCount);
        double selectivity = lesserRowCount / innerRowCount;
        return selectivity;
    }

    // Is this a ROWID = ROWID join predicate between 2 tables?
    private boolean isRowIdBinding() {
        if (!(baseTable instanceof FromBaseTable))
            return false;
        if (p.getReferencedSet().cardinality() != 2)
            return false;
        AndNode andNode = p.getAndNode();
        if (!andNode.getRightOperand().isBooleanTrue())
            return false;
        ValueNode operator = andNode.getLeftOperand();
        if (!(operator instanceof BinaryRelationalOperatorNode))
            return false;
        BinaryRelationalOperatorNode bron = (BinaryRelationalOperatorNode)operator;
        if (bron.getOperator() != RelationalOperator.EQUALS_RELOP)
            return false;
        ValueNode left = bron.getLeftOperand();
        ValueNode right = bron.getRightOperand();
        if (!(left instanceof ColumnReference))
            return false;
        if (!(right instanceof ColumnReference))
            return false;
        ColumnReference leftCR = (ColumnReference)left;
        ColumnReference rightCR = (ColumnReference)right;
        if (!isBaseRowIdOrRowId(leftCR.getColumnName()))
            return false;
        if (!isBaseRowIdOrRowId(rightCR.getColumnName()))
            return false;
        return true;
    }

    @Override
    public boolean shouldApplySelectivity() {
        return
            getNumReferencedTables() < 2             ||
            baseTable.getCurrentAccessPath() == null ||
            canPushJoinPredAsInnerTablePred(baseTable.getCurrentAccessPath());
    }

    private boolean canPushJoinPredAsInnerTablePred(AccessPath accessPath) {
        if (accessPath == null)
            return false;
        return accessPath.getJoinStrategy().getJoinStrategyType().isAllowsJoinPredicatePushdown();
    }


    public Predicate getPredicate() { return p; }
}
