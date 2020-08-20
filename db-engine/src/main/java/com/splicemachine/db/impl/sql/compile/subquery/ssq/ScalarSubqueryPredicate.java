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
package com.splicemachine.db.impl.sql.compile.subquery.ssq;

/**
 * Created by yxia on 10/11/17.
 */

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.compile.Visitable;
import com.splicemachine.db.iapi.util.JBitSet;
import com.splicemachine.db.impl.ast.CollectingVisitorBuilder;
import com.splicemachine.db.impl.ast.ColumnUtils;
import com.splicemachine.db.impl.sql.compile.*;
import org.apache.log4j.Logger;

import java.util.List;

/**
 * This predicate determines if we attempt to flatten a given scalar subquery in select clause or not.
 */
public class ScalarSubqueryPredicate implements splice.com.google.common.base.Predicate<SubqueryNode> {
    private static Logger LOG = Logger.getLogger(ScalarSubqueryPredicate.class);
    private SelectNode outerSelect;
    private JBitSet innerTables;

    public ScalarSubqueryPredicate(SelectNode outerSelect) {
        this.outerSelect = outerSelect;
    }

    @Override
    public boolean apply(SubqueryNode subqueryNode) {
        try {
            return doWeHandle(subqueryNode);
        } catch (StandardException e) {
            LOG.error("unexpected exception while considering scalar subquery flattening", e);
            return false;
        }
    }

    private boolean doWeHandle(SubqueryNode subqueryNode) throws StandardException {
        if (subqueryNode.isHintNotFlatten())
            return false;

        /* subquery cannot contain limit n/top n except for top 1 without order by*/
        if (subqueryNode.getFetchFirst() != null) {
            ValueNode fetchFirstNode = subqueryNode.getFetchFirst();
            long fetchFirstValue = -1;
            if (fetchFirstNode instanceof NumericConstantNode) {
                fetchFirstValue = ((NumericConstantNode)fetchFirstNode).getValue().getLong();
            }

            if (fetchFirstValue != 1 || subqueryNode.getOrderByList() != null)
                return false;
        }
        if (subqueryNode.getOffset() != null)
            return false;
        
        ResultSetNode subqueryResultSet = subqueryNode.getResultSet();

        /* subquery must be a select node */
        if (!(subqueryResultSet instanceof SelectNode)) {
            return false;
        }

        /* subquery does not have nested subqueries */
        SelectNode subSelect = (SelectNode)subqueryResultSet;
        List<SubqueryNode> nestedSubqueries = CollectingVisitorBuilder.forClass(SubqueryNode.class).collect(subSelect);
        if (!nestedSubqueries.isEmpty())
            return false;

        /* no aggregation */
        /* This could be relaxed, SSQ with aggregation could be rewritten similarly to flattened aggregate where subquery */
        if (!subSelect.getSelectAggregates().isEmpty() || subSelect.getHavingAggregates() != null && !subSelect.getHavingAggregates().isEmpty())
            return false;

        if (subSelect.getGroupByList() != null && !subSelect.getGroupByList().isEmpty())
            return false;

        /* No external table involved */
        List<SelectNode> externalTables = CollectingVisitorBuilder.<SelectNode>forPredicate(
                new splice.com.google.common.base.Predicate<Visitable>() {
            @Override
            public boolean apply(Visitable input) {
                return (input instanceof FromBaseTable) && ((FromBaseTable) input).getTableDescriptor().getStoredAs() != null;
            }
        }).collect(subSelect.getFromList());

        if (!externalTables.isEmpty())
            return false;

        /* correlation does not happen in the select clause */
        if (ColumnUtils.isSubtreeCorrelated(subSelect.getResultColumns()))
            return false;

        /* collect the set of inner tables of outer joins in the outer query block*/
        if (innerTables == null) {
            innerTables = new JBitSet(subqueryNode.getCompilerContext().getNumTables());
            for (int i = 0; i < outerSelect.getFromList().size(); i++) {
                collectInnerTableSet(innerTables, false, (ResultSetNode) outerSelect.getFromList().elementAt(i));
            }
        }

        // currently only support correlation in BinaryRelationalPredicate which can be relaxed in the future
        // also, correlation can not be with inner of an outer join
        ValueNode whereClause = subSelect.getWhereClause();
        if (whereClause != null) {
            ScalarSubqueryWhereVisitor scalarSubqueryWhereVisitor = new ScalarSubqueryWhereVisitor(innerTables);
            whereClause.accept(scalarSubqueryWhereVisitor);
            return scalarSubqueryWhereVisitor.isFoundEligibleCorrelation() && !scalarSubqueryWhereVisitor.isFoundUnsupported();
        } else
            return false;
    }

    private void collectInnerTableSet(JBitSet innerSet, boolean collect, ResultSetNode fromTable) {
        if (fromTable instanceof HalfOuterJoinNode) {
            if (((HalfOuterJoinNode) fromTable).isRightOuterJoin()) {
                collectInnerTableSet(innerSet, true, ((JoinNode) fromTable).getLeftResultSet());
                collectInnerTableSet(innerSet, collect, ((JoinNode) fromTable).getRightResultSet());
            } else {
                collectInnerTableSet(innerSet, collect, ((JoinNode) fromTable).getLeftResultSet());
                collectInnerTableSet(innerSet, true, ((JoinNode) fromTable).getRightResultSet());
            }
        } else if (fromTable instanceof FullOuterJoinNode) {
            collectInnerTableSet(innerSet, true, ((JoinNode) fromTable).getLeftResultSet());
            collectInnerTableSet(innerSet, true, ((JoinNode) fromTable).getRightResultSet());
        } else if (fromTable instanceof TableOperatorNode) {
            collectInnerTableSet(innerSet, collect, ((TableOperatorNode) fromTable).getLeftResultSet());
            collectInnerTableSet(innerSet, collect, ((TableOperatorNode) fromTable).getRightResultSet());
        } else if (fromTable instanceof FromBaseTable) {
            if (collect)
                innerSet.set(((FromBaseTable) fromTable).getTableNumber());
        } else if (fromTable instanceof FromSubquery) {
            if (collect)
                innerSet.set(((FromSubquery) fromTable).getTableNumber());
        } else if (fromTable instanceof ProjectRestrictNode) {
            collectInnerTableSet(innerSet, collect, ((ProjectRestrictNode) fromTable).getChildResult());
        }

        return;
    }
}
