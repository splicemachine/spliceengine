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

package com.splicemachine.db.impl.ast;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.compile.Optimizable;
import com.splicemachine.db.iapi.sql.compile.Visitable;
import com.splicemachine.db.impl.sql.compile.*;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.log4j.Logger;
import org.spark_project.guava.collect.Iterables;
import org.spark_project.guava.collect.Lists;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author P Trolard
 *         Date: 22/10/2013
 */
public class FixSubqueryColRefs extends AbstractSpliceVisitor {

    private static final Logger LOG = Logger.getLogger(FixSubqueryColRefs.class);

    private Map<Integer,List<SubqueryNode>> correlatedSubQs;

    public static <K,V> Map<K,List<V>> appendVal(Map<K,List<V>> m, K k, V v){
        List<V> vals = m.get(k);
        if (vals == null) {
            vals = Lists.newLinkedList();
            m.put(k, vals);
        }
        vals.add(v);
        return m;
    }

    public FixSubqueryColRefs(){
        correlatedSubQs = new HashMap<Integer, List<SubqueryNode>>();
    }

    @Override
    public SubqueryNode visit(SubqueryNode node) throws StandardException {
        if (node.getResultSet() instanceof Optimizable &&
                node.hasCorrelatedCRs()){
            correlatedSubQs = appendVal(correlatedSubQs, node.getPointOfAttachment(), node);
        }
        return node;
    }

    @Override
    public Visitable visit(ProjectRestrictNode node) throws StandardException {
        int num = node.getResultSetNumber();
        if (correlatedSubQs.containsKey(num)){
            final org.spark_project.guava.base.Predicate<ResultColumn> pointsToPrimaryTree = RSUtils.pointsTo(node.getChildResult());
            ResultColumnList rcl = node.getChildResult().getResultColumns();
            Map<Pair<Integer,Integer>,ResultColumn> colMap = ColumnUtils.rsnChainMap(rcl);
            for (SubqueryNode sub: correlatedSubQs.get(num)){
                Iterable<ColumnReference> crs =
                        Iterables.filter(RSUtils.collectNodes(sub, ColumnReference.class),
                                new org.spark_project.guava.base.Predicate<ColumnReference>() {
                                    @Override
                                    public boolean apply(ColumnReference cr) {
                                        return cr.getSource() != null &&
                                                pointsToPrimaryTree.apply(cr.getSource());
                                    }
                                });
                for (ColumnReference cr: crs){
                    Pair<Integer,Integer> coord = ColumnUtils.RSCoordinate(cr.getSource());
                    ResultColumn rcInScope = colMap.get(coord);
                    if (LOG.isDebugEnabled()) {
                        LOG.debug(String.format("Translating column %s to %s", coord,
                                                   ColumnUtils.RSCoordinate(rcInScope)));
                    }
                    cr.setSource(rcInScope);
                }
            }
        }
        return node;
    }
}
