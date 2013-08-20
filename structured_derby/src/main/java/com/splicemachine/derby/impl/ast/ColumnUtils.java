package com.splicemachine.derby.impl.ast;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.compile.Visitable;
import org.apache.derby.impl.sql.compile.*;

import java.util.*;

/**
 * User: pjt
 * Date: 7/29/13
 */
public class ColumnUtils {

    public static <N> Vector<N> collectNodes(Visitable node, Class<N> clazz)
            throws StandardException
    {
        CollectNodesVisitor v = new CollectNodesVisitor(clazz);
        node.accept(v);
        return (Vector<N>)v.getList();
    }

    public static Map<List<Integer>, ResultColumn> rsnChainMap(ResultColumnList rcl)
            throws StandardException
    {
        Map<List<Integer>, ResultColumn> chain = new HashMap<List<Integer>, ResultColumn>();
        Vector<ResultColumn> cols = collectNodes(rcl, ResultColumn.class);

        for (ResultColumn rc: cols){
            List<Integer> top = Arrays.asList(rc.getResultSetNumber(), rc.getVirtualColumnId());
            chain.put(top, rc);
            for (List<Integer> link: rsnChain(rc)){
                chain.put(link, rc);
            }
        }

        return chain;
    }

    public static List<List<Integer>> rsnChain(ResultColumn rc)
            throws StandardException
    {
        List<List<Integer>> chain = new ArrayList<List<Integer>>();

        ValueNode expression = rc.getExpression();
        while (expression != null) {
            if (expression instanceof VirtualColumnNode) {
                ResultColumn sc = ((VirtualColumnNode) expression).getSourceColumn();
                chain.add(Arrays.asList(sc.getResultSetNumber(), sc.getVirtualColumnId()));
                expression = sc.getExpression();
            } else if (expression instanceof ColumnReference) {
                ResultColumn sc = ((ColumnReference) expression).getSource();
                if (sc != null){ // A ColumnReference can be sourceless…
                    chain.add(Arrays.asList(sc.getResultSetNumber(), sc.getVirtualColumnId()));
                    expression = sc.getExpression();
                } else {
                    expression = null;
                }
            } else {
                expression = null;
            }
        }

        return chain;
    }

}
