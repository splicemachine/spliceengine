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

public class RepeatedPredicateVisitor extends AbstractSpliceVisitor {

    private boolean foundWhereClause = false;

    public Map nodesWithMultipleOccurrences(ValueNode pred) throws StandardException {
        Map<Object, Integer> m = new HashMap<Object,Integer>();

        List<BinaryRelationalOperatorNode> binNodes = ColumnUtils.collectNodes(pred, BinaryRelationalOperatorNode.class);

        for(BinaryRelationalOperatorNode node : binNodes ){

            if(m.containsKey(node)){
                Integer count = m.get(node);
                m.put(node, Integer.valueOf( count.intValue() + 1));
            }else{
                m.put(node, Integer.valueOf(1));
            }
        }

        Iterator<Map.Entry<Object, Integer>> mapIt = m.entrySet().iterator();

        while(mapIt.hasNext()){

            Map.Entry<Object,Integer> me = mapIt.next();
            Integer i = me.getValue();

            if(i.intValue() <= 1){
                mapIt.remove();
            }
        }

        return m;
    }

    public int countPaths(ValueNode vn){

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
