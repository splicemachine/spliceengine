package com.splicemachine.derby.impl.ast;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.compile.Optimizable;
import org.apache.derby.iapi.sql.compile.Visitable;
import org.apache.derby.impl.sql.compile.*;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.log4j.Logger;

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
				List<V> objects = m.get(k);
				if(objects==null){
						objects = Lists.newArrayListWithExpectedSize(1);
						m.put(k,objects);
				}
				objects.add(v);
//        if (m.containsKey(k)){
//            m.get(k).add(v);
//        } else {
//						//noinspection unchecked
//						List<V> objects = Lists.newArrayListWithExpectedSize(1);
//						objects.add(v);
//						m.put(k, objects);
//        }
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
            final Predicate<ResultColumn> pointsToPrimaryTree = RSUtils.pointsTo(node.getChildResult());
            ResultColumnList rcl = node.getChildResult().getResultColumns();
            Map<Pair<Integer,Integer>,ResultColumn> colMap = ColumnUtils.rsnChainMap(rcl);
            for (SubqueryNode sub: correlatedSubQs.get(num)){
                Iterable<ColumnReference> crs =
                        Iterables.filter(RSUtils.collectNodes(sub, ColumnReference.class),
                                new Predicate<ColumnReference>() {
                                    @Override
                                    public boolean apply(ColumnReference cr) {
                                        return cr.getSource() != null &&
                                                pointsToPrimaryTree.apply(cr.getSource());
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
