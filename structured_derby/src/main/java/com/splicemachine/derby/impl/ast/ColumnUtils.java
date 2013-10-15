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

    public static <N> List<N> collectNodes(Visitable node, Class<N> clazz)
            throws StandardException
    {
        CollectNodesVisitor v = new CollectNodesVisitor(clazz);
        node.accept(v);
        return (List<N>)v.getList();
    }

    public static List<ResultSetNode> getChildren(ResultSetNode node)
            throws StandardException
    {
        CollectChildrenVisitor v = new CollectChildrenVisitor();
        node.accept(v);
        return v.getChildren();
    }

    /**
     * For a given ResultColumnList, return a map from
     *  [resultSetNumber, virtualColumnId] => ResultColumn
     * where there is one entry for each ResultColumn in down the chain of reference to
     * its source column on a table. This allows translation from a column reference at
     * any node below into the ResultColumn projected from the passed ResultColumnList.
     */
    public static Map<List<Integer>, ResultColumn> rsnChainMap(ResultColumnList rcl)
            throws StandardException
    {
        Map<List<Integer>, ResultColumn> chain = new HashMap<List<Integer>, ResultColumn>();
        List<ResultColumn> cols = collectNodes(rcl, ResultColumn.class);

        for (ResultColumn rc: cols){
            List<Integer> top = Arrays.asList(rc.getResultSetNumber(), rc.getVirtualColumnId());
            chain.put(top, rc);
            for (List<Integer> link: rsnChain(rc)){
                chain.put(link, rc);
            }
        }

        return chain;
    }

    /**
     * For a given ResultColumn, return a list of integer pairs [resultSetNumber, virtualColumnId]
     * for its source column, and its source's source column, and so on down to the bottom: a source
     * column on a table.
     */
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
