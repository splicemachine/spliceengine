package com.splicemachine.derby.impl.ast;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.impl.sql.compile.*;
import org.apache.hadoop.hbase.util.Pair;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * User: pjt
 * Date: 7/29/13
 */
public class ColumnUtils {

    /**
     * For a given ResultColumnList, return a map from
     * [resultSetNumber, virtualColumnId] => ResultColumn
     * where there is one entry for each ResultColumn down the chain of reference to
     * its source column on a table. This allows translation from a column reference at
     * any node below into the ResultColumn projected from the passed ResultColumnList.
     */
    public static Map<Pair<Integer, Integer>, ResultColumn> rsnChainMap(ResultColumnList rcl)
            throws StandardException {
        Map<Pair<Integer, Integer>, ResultColumn> chain = new HashMap<Pair<Integer, Integer>, ResultColumn>();
        List<ResultColumn> cols = RSUtils.collectNodes(rcl, ResultColumn.class);

        for (ResultColumn rc : cols) {
            Pair<Integer, Integer> top = RSCoordinate(rc);
            chain.put(top, rc);
            for (Pair<Integer, Integer> link : rsnChain(rc)) {
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
    public static List<Pair<Integer, Integer>> rsnChain(ResultColumn rc)
            throws StandardException {
        List<Pair<Integer, Integer>> chain = new ArrayList<Pair<Integer, Integer>>();

        ValueNode expression = rc.getExpression();
        while (expression != null) {
            if (expression instanceof VirtualColumnNode) {
                ResultColumn sc = ((VirtualColumnNode) expression).getSourceColumn();
                chain.add(RSCoordinate(sc));
                expression = sc.getExpression();
            } else if (expression instanceof ColumnReference) {
                ResultColumn sc = ((ColumnReference) expression).getSource();
                if (sc != null) { // A ColumnReference can be sourceless…
                    chain.add(RSCoordinate(sc));
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

    public static Pair<Integer, Integer> RSCoordinate(ResultColumn rc) {
        return Pair.newPair(rc.getResultSetNumber(), rc.getVirtualColumnId());
    }
}
