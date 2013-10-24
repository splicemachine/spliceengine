package com.splicemachine.derby.impl.ast;

import com.google.common.base.*;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.compile.Optimizable;
import org.apache.derby.iapi.sql.compile.Visitable;
import org.apache.derby.impl.sql.compile.*;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.log4j.Logger;

import javax.annotation.Nullable;
import java.util.*;

/**
 * @author P Trolard
 *         Date: 22/10/2013
 */
public class FixSubqueryColRefs extends AbstractSpliceVisitor {

    private static final Logger LOG = Logger.getLogger(FixSubqueryColRefs.class);

    private final Map<Integer,List<SubqueryNode>> correlatedSubQs;

    public static <K,V> Map<K,List<V>> appendVal(Map<K,List<V>> m, K k, V v){
        if (m.containsKey(k)){
            m.get(k).add(v);
        } else {
            m.put(k, Collections.singletonList(v));
        }
        return m;
    }

    public FixSubqueryColRefs(){
        correlatedSubQs = new HashMap<Integer, List<SubqueryNode>>();
    }

    @Override
    public SubqueryNode visit(SubqueryNode node) throws StandardException {
        if (node.getResultSet() instanceof Optimizable &&
                node.hasCorrelatedCRs()){
            appendVal(correlatedSubQs, node.getPointOfAttachment(), node);
        }
        return node;
    }

    @Override
    public Visitable visit(ProjectRestrictNode node) throws StandardException {
        int num = node.getResultSetNumber();
        if (correlatedSubQs.containsKey(num)){
            final Predicate<ResultColumn> pointsToPrimaryTree = RSUtils.pointsTo(node.getChildResult());
            ResultColumnList rcl = node.getChildResult().getResultColumns();
            Map<Pair<Integer,Integer>,ResultColumn> colMap = ColumnUtils.rsnChainMap(rcl);
            for (SubqueryNode sub: correlatedSubQs.get(num)){
                Iterable<ColumnReference> crs =
                        Iterables.filter(Sets.newHashSet(RSUtils.collectNodes(sub, ColumnReference.class)),
                                new Predicate<ColumnReference>() {
                                    @Override
                                    public boolean apply(ColumnReference cr) {
                                        return pointsToPrimaryTree.apply(cr.getSource());
                                    }
                                });
                for (ColumnReference cr: crs){
                    Pair<Integer,Integer> coord = ColumnUtils.RSCoordinate(cr.getSource());
                    ResultColumn rcInScope = colMap.get(coord);
                    LOG.info(String.format("Translating column %s to %s", coord,
                            ColumnUtils.RSCoordinate(rcInScope)));
                    cr.setSource(rcInScope);
                }
            }
        }
        return node;
    }
}
