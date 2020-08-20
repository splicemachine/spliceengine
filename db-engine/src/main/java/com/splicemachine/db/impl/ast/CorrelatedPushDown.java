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

package com.splicemachine.db.impl.ast;


import com.carrotsearch.hppc.LongArrayList;
import splice.com.google.common.base.Function;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.compile.Optimizable;
import com.splicemachine.db.iapi.sql.compile.Visitable;
import com.splicemachine.db.impl.sql.compile.*;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.log4j.Logger;
import splice.com.google.common.collect.Collections2;
import splice.com.google.common.collect.Iterables;
import java.util.*;

/**
 * When Derby encounters an IN subquery that cannot be flattened, it converts the expression
 * `expr IN (select col from subquery)` into `IS NOT NULL (subquery where expr = col)`. This
 * conversion makes an uncorrelated subquery into a correlated one (where expr contains
 * the correlated reference).
 *
 * For whatever reason, the predicate expr = col does not get pushed down as far as it could
 * be in the subquery tree. (TPCH Q20 is the triggering case here.) This visitor identifies
 * such correlated predicates and attempts to push them down as far as possible. Below is
 * the example from TPCH Q20.
 *
 * Plan for Q20 (omitting subquery n=15):
 *
 *   ScrollInsensitiveResultSetNode ({n=20})
 *    OrderByNode ({n=19})
 *      ProjectRestrictNode ({quals=[is not null(subq=17)], n=18})
 *        JoinNode ({n=4, preds=[(S_NATIONKEY[4:4] = N_NATIONKEY[4:5])], exe=SORTMERGE})
 *          ProjectRestrictNode ({n=3})
 *            FromBaseTable ({n=2, table=NATION,1280})
 *        ProjectRestrictNode ({n=1})
 *          FromBaseTable ({n=0, table=SUPPLIER,1248})
 *
 *   Subquery n=17: expression?=false, invariant?=true, correlated?=true
 *     ProjectRestrictNode ({quals=[(S_SUPPKEY[4:1] = PS_SUPPKEY[16:1])], n=17})
 *       ProjectRestrictNode ({quals=[(PS_AVAILQTY[9:3] > subq=15)], n=16})
 *         JoinNode ({n=9, preds=[(PS_PARTKEY[9:1] = P_PARTKEY[9:4])], exe=SORTMERGE})
 *           ProjectRestrictNode ({n=8})
 *             FromBaseTable ({n=7, table=PART,1264})
 *           ProjectRestrictNode ({quals=[true], n=6})
 *             FromBaseTable ({n=5, table=PARTSUPP,1232})
 *
 * The predicate targeted by this visitor is the one on PRNode n=17. Its optimial
 * placement is on FBT n=5. (Moving the predicate to n=5 can drastically reduce the
 * size of the join at n=9 and consequently the number of times subquery n=15 needs
 * to be executed.)
 *
 * The logic is to look for these kinds of predicates on the root node of a subquery
 * result set. They're identified by two operands, one of which is correlated and the
 * other is a column reference. Follow the column reference through appropriate nodes
 * until it (a) bottoms out on a BaseColumn on a FBT node or (b) is involved in an
 * expression (which, for now, we'll consdier as opaque). Appropriate nodes are all
 * but aggregates and unions (we could push down to both sides of a union, but we
 * won't for now).
 *
 * @author P Trolard
 *         Date: 03/03/2014
 */
public class CorrelatedPushDown extends AbstractSpliceVisitor {
    private static Logger LOG = Logger.getLogger(CorrelatedPushDown.class);

    public Visitable visit(SubqueryNode subq) throws StandardException {
        if (!(subq.getResultSet() instanceof ProjectRestrictNode)){
            // bail
            return subq;
        }
        ProjectRestrictNode pr = (ProjectRestrictNode) subq.getResultSet();
        PredicateList preds = RSUtils.getPreds(pr);
        if (preds.size() != 1) {
            // bail
            return subq;
        }
        Predicate pred = (Predicate) preds.getOptPredicate(0);
        List<ValueNode> colRefs = binaryOperands.apply(pred);
        if (colRefs.size() != 2) {
            // bail
            return subq;
        }
        /* revisit, thinking about non-colrefs or colrefs w/o ResultCols underneath */
        splice.com.google.common.base.Predicate<ValueNode> isUncorrelated =
            RSUtils.refPointsTo(pr);
        Collection<ValueNode> uncorrelated =
            Collections2.filter(colRefs, isUncorrelated);

        if (uncorrelated.size() != 1){
            // bail
            return subq;
        }

        boolean didPush = pushdownPredWithColumn(pr, pred, uncorrelated.iterator().next());
        if (didPush){
            preds.removeOptPredicate(pred);
        }

        return subq;
    }

    public static final Function<Predicate,List<ValueNode>> binaryOperands = new Function<Predicate, List<ValueNode>>() {
        @Override
        @SuppressFBWarnings(value = "NP_PARAMETER_MUST_BE_NONNULL_BUT_MARKED_AS_NULLABLE", justification = "DB-9844")
        public List<ValueNode> apply(Predicate pred) {
            ValueNode operator = pred.getAndNode().getLeftOperand();
            if (operator instanceof BinaryRelationalOperatorNode) {
                BinaryRelationalOperatorNode boperator = (BinaryRelationalOperatorNode) operator;
                return (List<ValueNode>)boperator.getChildren();
            }
            return null;
        }
    };

    public boolean pushdownPredWithColumn(ResultSetNode rsn, Predicate pred, ValueNode colRef)
            throws StandardException {
        try {
            ResultColumn rc = RSUtils.refToRC.apply(colRef);
            LongArrayList chain = rc.chain();
            long lastLink = chain.get(chain.size() - 1);
            List<ResultSetNode> subTree = RSUtils.getSelfAndDescendants(rsn);
            Map<Integer, ResultSetNode> nodeMap = zipMap(Iterables.transform(subTree, RSUtils.rsNum), subTree);
            int left = (int) (lastLink >> 32);
            int right = (int) lastLink;
            ResultSetNode targetRSN = nodeMap.get(left);
            rc.setResultSetNumber(left);
            rc.setVirtualColumnId(right);
            ((Optimizable)targetRSN).pushOptPredicate(pred);
        } catch (StandardException e) {
            LOG.warn("Exception pushing down topmost subquery predicate:", e);
            return false;
        }
        return true;
    }

    public static <K,V> Map<K,V> zipMap(Iterable<K> keys, Iterable<V> vals) {
        Map<K,V> m = new HashMap<>();
        Iterator<V> valsIterator = vals.iterator();
        for (K key: keys){
            if (valsIterator.hasNext()) {
                m.put(key, valsIterator.next());
            }
        }
        return m;
    }

}
