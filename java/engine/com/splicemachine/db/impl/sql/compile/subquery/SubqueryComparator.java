package com.splicemachine.db.impl.sql.compile.subquery;

import com.splicemachine.db.impl.sql.compile.SubqueryNode;

import java.util.Comparator;

/**
 * Will sort subqueries such that NOT-EXISTS subqueries come before all other types.
 */
public class SubqueryComparator implements Comparator<SubqueryNode> {
    @Override
    public int compare(SubqueryNode s1, SubqueryNode s2) {
        return Integer.compare(s1.isNOT_EXISTS() ? 0 : 1, s2.isNOT_EXISTS() ? 0 : 1);
    }
}