package com.splicemachine.derby.impl.ast;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.compile.Visitable;
import org.apache.derby.impl.sql.compile.AndNode;
import org.apache.derby.impl.sql.compile.BinaryRelationalOperatorNode;
import org.apache.derby.impl.sql.compile.OrNode;
import org.apache.derby.impl.sql.compile.ValueNode;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * This Visitor finds the first ValueNode (which is typically an Or/And node and attempts
 * to find duplicate predicates.  A good example is a join condition contained in every path
 * of the where clause, such as:
 *
 * select *
 * from table1 t1, table2 t2
 * where { {
 *           t1.foo = t2.foo
 *           and t1.bar = 'bar'
 *           and t2.baz = 'baz'
 *
 *         } OR {
 *
 *           t1.foo = t2.foo
 *           and t1.bar = 'not bar'
 *           and t2.baz = 'not baz'
 *
 *          } OR {
 *
 *           t1.foo = t2.foo
 *           and t1.bar = 'really bar'
 *           and t2.baz = 'really baz'
 *
 *          }
 *       }
 *
 * Derby's optimizer doesn't currently spot the repeated join condition and thus will keep the whole predicate
 * together.  This does not allow the join condition to be pushed down to the right side and will then not
 * allow a Merge Sort Join to be used.  This code will spot the repeated BinaryRelationalOperation (i.e. t1.foo = t2.foo)
 * and break that out from the ORs, move it to an and clause, so t1.foo = t2.foo AND {...} OR {...} OR {...}
 *
 */
public class RepeatedPredicateVisitor extends AbstractSpliceVisitor {

    private boolean foundWhereClause = false;

    /**
     * Finds any node in the predicate part of the query tree that occurs more than once.
     *
     * @param pred
     * @return a map of the predicate ValueNode and how many times it occurs (only including ValueNodes that occur more than once
     * @throws StandardException
     */
    private Map<ValueNode,Integer> nodesWithMultipleOccurrences(ValueNode pred) throws StandardException {
        Map<ValueNode, Integer> m = new HashMap<ValueNode,Integer>();

        List<BinaryRelationalOperatorNode> binNodes = ColumnUtils.collectNodes(pred, BinaryRelationalOperatorNode.class);

        for(BinaryRelationalOperatorNode node : binNodes ){

            if(m.containsKey(node)){
                Integer count = m.get(node);
                m.put(node, Integer.valueOf( count.intValue() + 1));
            }else{
                m.put(node, Integer.valueOf(1));
            }
        }

        Iterator<Map.Entry<ValueNode, Integer>> mapIt = m.entrySet().iterator();

        while(mapIt.hasNext()){

            Map.Entry<ValueNode,Integer> me = mapIt.next();
            Integer i = me.getValue();

            if(i.intValue() <= 1){
                mapIt.remove();
            }
        }

        return m;
    }

    /**
     * Counts the number of distinct paths in a predicate.  An
     * OR statement will create an additional path and an AND
     * node's children could potentially add another possible
     * path.
     *
     * @param vn
     * @return
     */
    private int countPaths(ValueNode vn){

        List <ValueNode> childNodes = new LinkedList<ValueNode>();
        childNodes.add(vn);

        int totalPaths = 1;

        while(childNodes != null && !childNodes.isEmpty()){

            List<ValueNode> nextChildren = new LinkedList<ValueNode>();

            for(ValueNode childNode : childNodes){
                if(childNode instanceof OrNode){
                    totalPaths++;
                    nextChildren.addAll(childNode.getChildren());
                }else if(childNode instanceof AndNode){
                    nextChildren.addAll(childNode.getChildren());
                }
            }

            childNodes = nextChildren;

        }

        return totalPaths;
    }

    /**
     * For a given node, check the number of possible paths in the predicate tree
     * and check for any duplicated predicates.  If a predicate occurs X times, and we
     * find there are only X possible paths, that predicate can be simplified, by pulling
     * it out and ANDing it with the rest of the tree.
     *
     * @param node
     * @return
     * @throws StandardException
     */
    @Override
    public Visitable defaultVisit(Visitable node) throws StandardException {

        Visitable updatedNode = null;

        if(node instanceof ValueNode){

            foundWhereClause = true;

            ValueNode whereClause = (ValueNode) node;

            int totalPaths = countPaths(whereClause);
            Map<ValueNode, Integer> m = nodesWithMultipleOccurrences(whereClause);

            for(Map.Entry<ValueNode,Integer> me : m.entrySet()){
                if(me.getValue().intValue() == totalPaths){
                    AndOrReplacementVisitor aor = new AndOrReplacementVisitor(me.getKey());
                    whereClause.accept(new SpliceDerbyVisitorAdapter(aor));
                    AndNode newAndNode = new AndNode();
                    newAndNode.init(me.getKey(), whereClause);
                    updatedNode = newAndNode;
                }
            }
        }

        if(updatedNode == null){
            updatedNode = node;
        }

        return updatedNode;
    }


    @Override
    public boolean isPostOrder() {
        return false;
    }

    @Override
    public boolean skipChildren(Visitable node) {
       return foundWhereClause;
    }
}
