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

package com.splicemachine.db.impl.sql.compile.subquery;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.context.ContextManager;
import com.splicemachine.db.iapi.sql.compile.C_NodeTypes;
import com.splicemachine.db.iapi.sql.compile.NodeFactory;
import com.splicemachine.db.impl.ast.PredicateUtils;
import com.splicemachine.db.impl.sql.compile.BinaryRelationalOperatorNode;
import com.splicemachine.db.impl.sql.compile.ColumnReference;
import com.splicemachine.db.impl.sql.compile.FromSubquery;
import com.splicemachine.db.impl.sql.compile.ResultColumn;

/**
 * Shared code for creating a ColumnReference to a ResultColumn in a FromSubquery.
 */
public class FromSubqueryColRefFactory {

    /**
     * We have a recently created FromSubquery node and we want to create a new ColumnReference to it.
     *
     * EXAMPLE:
     * <pre>
     *     select A.*
     *       from A
     *       join (select 1,b1,sum(b2) from B where b2 > x) foo
     *       where a1 > 1;
     * </pre>
     *
     * Here we are creating a column reference that can be used in the outer query to reference foo.b1 so that we can
     * add predicates to the outer query such as a1 = foo.b1
     *
     * @param outerSelectNestingLevel select node of the outer query.
     * @param newFromSubquery         the FromSubquery we are creating as part of subquery flattening.
     * @param fromSubqueryColumnToRef 0-based index of the FromSubquery column we want to reference.
     */
    public static ColumnReference build(int outerSelectNestingLevel,
                                        FromSubquery newFromSubquery,
                                        int fromSubqueryColumnToRef,
                                        NodeFactory nodeFactory,
                                        ContextManager contextManager) throws StandardException {

        ResultColumn resultColumn = newFromSubquery.getResultColumns().elementAt(fromSubqueryColumnToRef);

        ColumnReference colRef = (ColumnReference) nodeFactory.getNode(C_NodeTypes.COLUMN_REFERENCE,
                resultColumn.getName(),
                newFromSubquery.getTableName(),
                contextManager);

        colRef.setSource(resultColumn);
        colRef.setTableNumber(newFromSubquery.getTableNumber());
        colRef.setTableNameNode(newFromSubquery.getTableName());
        colRef.setColumnNumber(resultColumn.getVirtualColumnId());
        colRef.setNestingLevel(outerSelectNestingLevel);
        colRef.setSourceLevel(outerSelectNestingLevel);

        return colRef;
    }

    /**
     * Modify one side of the given predicate to contain the new FromSubquery ColumnReference.
     *
     * The passed BRON is a predicate like 'a1 = b1' which we are moving from a subquery to the outer query.  This
     * method finds the subquery level CR (b1 below) and replaces it with a column reference to the new FromSubquery.
     *
     * <pre>
     * select * form A where exists(select 1 from B where a1 = b1);
     * </pre>
     *
     * @param pred                In this BRON
     * @param colRef              Substitute this colRef
     * @param subquerySourceLevel for the existing colRef at this source level.
     */
    public static void replace(BinaryRelationalOperatorNode pred, ColumnReference colRef, int subquerySourceLevel) {
        ColumnReference leftOperand = (ColumnReference) pred.getLeftOperand();
        ColumnReference rightOperand = (ColumnReference) pred.getRightOperand();
        // b1 = a1 TO foo.b1 = a1
        if (PredicateUtils.isLeftColRef(pred, subquerySourceLevel)) {
            pred.setLeftOperand(colRef);
            rightOperand.setNestingLevel(rightOperand.getSourceLevel());
        }
        // a1 = b1 TO a1 = foo.b1
        else if (PredicateUtils.isRightColRef(pred, subquerySourceLevel)) {
            pred.setRightOperand(colRef);
            leftOperand.setNestingLevel(leftOperand.getSourceLevel());
        }
    }
}
