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

package com.splicemachine.db.impl.sql.compile.subquery.exists;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.compile.Visitable;
import com.splicemachine.db.iapi.sql.compile.Visitor;
import com.splicemachine.db.impl.ast.CollectingVisitorBuilder;
import com.splicemachine.db.impl.ast.ColumnUtils;
import com.splicemachine.db.impl.sql.compile.*;
import com.splicemachine.db.impl.sql.compile.subquery.CorrelatedBronPredicate;
import com.splicemachine.db.impl.sql.compile.subquery.CorrelatedColumnPredicate;
import com.splicemachine.db.impl.sql.compile.subquery.CorrelatedEqualityBronPredicate;
import com.splicemachine.db.impl.sql.compile.subquery.FlatteningUtils;
import org.apache.log4j.Logger;
import org.spark_project.guava.collect.Iterables;
import org.spark_project.guava.collect.Lists;

import java.util.List;

/**
 * Determines if the where clause of an exists subquery is one that we can flatten.
 *
 * EXAMPLE: Why can flatten exists but not not-exists subqueries with type C predicates?  Consider columns A.a1 and B.b1
 * where A.a1 contains [1,9] and B.b1 contains [1,8]:
 *
 * <pre>
 *
 * this         : select a1 from A where EXISTS (select 1 from B where a1=b1 and a1=5);  -- returns '5'
 * equivalent to: select a1 from A where a1=5 and EXISTS (select 1 from B where a1=b1);  -- returns '5'
 *
 * however for NOT-EXISTS
 *
 * this             : select a1 from A where NOT EXISTS (select 1 from B where a1=b1 and a1=5);  -- all in A but '5'
 * NOT equivalent to: select a1 from A where a1!=5 and NOT EXISTS (select 1 from B where a1=b1); -- returns '9'
 *
 * </pre>
 */
class ExistsSubqueryWhereVisitor implements Visitor {

    private static Logger LOG = Logger.getLogger(ExistsSubqueryWhereVisitor.class);

    /* We flatten exists subqueries with either type of BRON */
    private final CorrelatedEqualityBronPredicate typeDPredicate;
    /* For EXISTS subqueries we can move this type of predicate up one level (but not for NOT EXISTS subqueries). */
    private final CorrelatedBronPredicate typeCPredicate;

    private final boolean isNotExistsSubquery;

    /* If true indicates that we found something in the subquery where clause that cannot be flattened in any case. */
    private boolean foundUnsupported;

    /* For UNION subqueries we only flatten if typeDCount = 1 and typeCCount = 0 */
    private int typeCCount;

    private List<ColumnReference> typeDCorrelatedColumnReference = Lists.newArrayList();

    private int outerNestingLevel;

    private final CorrelatedColumnPredicate correlatedColumnPredicate;

    /**
     * @param subqueryLevel       The level of the subquery we are considering flattening in the enclosing predicate
     * @param isNotExistsSubquery Are we looking at a NOT EXISTS subquery.
     */
    public ExistsSubqueryWhereVisitor(int subqueryLevel, boolean isNotExistsSubquery) {
        this.isNotExistsSubquery = isNotExistsSubquery;
        this.outerNestingLevel = subqueryLevel - 1;
        this.typeDPredicate = new CorrelatedEqualityBronPredicate(this.outerNestingLevel);
        this.typeCPredicate = new CorrelatedBronPredicate(this.outerNestingLevel);
        this.correlatedColumnPredicate = new CorrelatedColumnPredicate(this.outerNestingLevel);
    }

    @Override
    public Visitable visit(Visitable node, QueryTreeNode parent) throws StandardException {
        if (node instanceof AndNode) {
            /* do nothing */
        } else if (node instanceof BinaryRelationalOperatorNode) {
            BinaryRelationalOperatorNode bron = (BinaryRelationalOperatorNode) node;

            boolean correlatedBron = typeCPredicate.apply(bron);

            /*
             * CASE 1: a1 = constant;  where a1 is a result col from the outer query.  If this is a NOT-EXISTS then the
             * predicate cannot be moved and this subquery cannot be flattened.  For EXISTS subqueries it is ok.
             */
            if (correlatedBron) {
                typeCCount++;
                foundUnsupported = isNotExistsSubquery;
                return node;
            }

            /*
             * CASE 2: a1 = b1; where one CR is at subquery level and one from outer query.
             */
            if (typeDPredicate.apply(bron)) {
                typeDCorrelatedColumnReference.add(FlatteningUtils.findColumnReference(bron, outerNestingLevel ));
                return node;
            }

            /*
             * CASE 3: anything else is OK as long as not correlated.
             */
            foundUnsupported = ColumnUtils.isSubtreeCorrelated(node);

        } else {
            /* Current node is not an AndNode or BinaryRelationalOperatorNode -- ok as long as there are no
             * predicates correlated to table above us, we can move/flatten any type of subtree */
            foundUnsupported = node instanceof OrNode;
            if (!foundUnsupported) {
                List<ColumnReference> columnReferences = CollectingVisitorBuilder.forClass(ColumnReference.class).collect(node);
                foundUnsupported = Iterables.any(columnReferences, correlatedColumnPredicate);
            }
        }
        return node;
    }

    @Override
    public boolean skipChildren(Visitable node) throws StandardException {
        // AndNode -> skipChildren = false
        // Else    -> skipChildren = true
        return !(node instanceof AndNode);
    }

    @Override
    public boolean visitChildrenFirst(Visitable node) {
        return false;
    }

    @Override
    public boolean stopTraversal() {
        return foundUnsupported;
    }

    public boolean isFoundUnsupported() {
        return foundUnsupported;
    }

    public List<ColumnReference> getTypeDCorrelatedColumnReference() {
        return typeDCorrelatedColumnReference;
    }

    public int getTypeCCount() {
        return typeCCount;
    }
}
