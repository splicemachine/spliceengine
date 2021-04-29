/*
 * Copyright (c) 2012 - 2021 Splice Machine, Inc.
 *
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
 */

package com.splicemachine.db.impl.sql.compile;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.FormatableProperties;
import com.splicemachine.db.iapi.sql.dictionary.ConglomerateDescriptor;
import com.splicemachine.qpt.SQLStatement;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;

public class QueryHinting {
    public static void hint(StatementNode statementNode) {
        try {
            if (!(statementNode instanceof CursorNode))
                return;
            CursorNode cn = (CursorNode) statementNode;
            ResultSetNode rsn = cn.getResultSetNode();

            String stmtId = sqlStatementId(statementNode.getLanguageConnectionContext().getStatementContext().getStatementText());
            switch (stmtId) {
                case "Sp3QPj57":
                case "#yvYkFUJ": // explain
                    hintDB11940(rsn);
                    break;
                case "SmOINPGy":
                case "#Jahtv3X": // explain
                    hintDB11941(rsn);
                    break;
                case "S5#YvaXt":
                case "#qj#jlet": // explain
                    hintDB11942(rsn);
                    break;
                case "SMopSK5W":
                case "#9fzfFLU": //explain
                    hintDB11967(rsn);
                    break;
                case "SQAzSQuw":
                case "#0n9BLjL": //explain
                    hintDB11968(rsn);
                    break;
                case "Sjq3JEny":
                case "#sF7HBge": //explain
                    hintDB11970(rsn);
                    break;
                case "S9wSWJg8":
                case "#uHJq5bA": //explain
                    hintDB11971(rsn);
                    break;
            }
        } catch (Exception e) {
            // ignore, don't hint this query
        }
    }

    private static void hintDB11971(ResultSetNode rsn) throws StandardException {
        if (!(rsn instanceof UnionNode))
            return;

        UnionNode un = (UnionNode) rsn;
        ResultSetNode node = un.getRightResultSet();
        if (!(node instanceof SelectNode)) return;

        SelectNode sn = (SelectNode) node;

        if (!hasTables(sn.fromList, Arrays.asList("TBATCHTERMIN", "TGSCHFAKT", "TVERTRAG"))) return;

        fixedJoinOrder(sn.getFromList());

        for(QueryTreeNode qtn : sn.getFromList()) {
            FromBaseTable fbt = (FromBaseTable) qtn;
            FormatableProperties tableProps = new FormatableProperties();
            switch (fbt.getBaseTableName()) {
                case "TBATCHTERMIN":
                    tableProps.put("index", "XPFBATTERMIN");
                    fbt.setProperties(tableProps);
                    break;
                case "TGSCHFAKT":
                    tableProps.put("joinStrategy", "nestedloop");
                    fbt.setProperties(tableProps);
                    break;
            }
        }
    }

    private static void hintDB11970(ResultSetNode rsn) {
        if (!(rsn instanceof UnionNode))
            return;

        Deque<ResultSetNode> toProcess = new ArrayDeque<>();
        toProcess.add(rsn);
        while(!toProcess.isEmpty()) {
            ResultSetNode top = toProcess.pop();
            if (top instanceof UnionNode) {
                UnionNode un = (UnionNode) top;
                toProcess.add(un.getLeftResultSet());
                toProcess.add(un.getRightResultSet());
            } else if (top instanceof SelectNode) {
                SelectNode sn = (SelectNode) top;

                for(SubqueryNode sqn : sn.getWhereSubquerys()) {
                    if (!(sqn.getResultSet() instanceof SelectNode))
                        continue;

                    for(QueryTreeNode qtn : ((SelectNode)sqn.getResultSet()).getFromList()) {
                        FromBaseTable fbt = (FromBaseTable) qtn;
                        switch (fbt.getBaseTableName()) {
                            case "TTERMINVERW":
                            case "TAUSSTEUERUNG":
                                FormatableProperties tableProps = new FormatableProperties();
                                tableProps.put("joinStrategy", "nestedloop");
                                fbt.setProperties(tableProps);
                            break;
                        }
                    }
                }
            }
        }

    }

    private static void hintDB11968(ResultSetNode rsn) {
        if (!(rsn instanceof SelectNode))
            return;
        SelectNode sn = (SelectNode) rsn;

        if (!hasTables(sn.fromList, Arrays.asList("TVERTRAG"))) return;

        SubqueryList sql = sn.getWhereSubquerys();
        if (sql.size() != 2) return;

        SubqueryNode sqn = sql.elementAt(1);
        if (!(sqn.getResultSet() instanceof SelectNode)) return;

        SelectNode ssn = (SelectNode) sqn.getResultSet();
        if (!hasTables(ssn.fromList, Arrays.asList("TVERSVERTRAG", "TSP_PRODUKT"))) return;

        for(QueryTreeNode qtn : ssn.getFromList()) {
            FromBaseTable fbt = (FromBaseTable) qtn;
            FormatableProperties tableProps = new FormatableProperties();
            tableProps.put("joinStrategy", "nestedloop");
            fbt.setProperties(tableProps);
        }
    }

    private static void hintDB11967(ResultSetNode rsn) throws StandardException {
        if (!(rsn instanceof UnionNode))
            return;
        UnionNode un = (UnionNode) rsn;
        ResultSetNode lrsn = un.getLeftResultSet();
        if (!(lrsn instanceof SelectNode))
            return;
        SelectNode left = (SelectNode) lrsn;
        if (!hasTables(left.fromList, Arrays.asList("TTERMIN", "TSCHADEN", "TREGVEREINB"))) return;

        ResultSetNode rrsn = un.getRightResultSet();
        if (!(rrsn instanceof SelectNode))
            return;
        SelectNode right = (SelectNode) rrsn;
        if (!hasTables(right.fromList, Arrays.asList("TBATCHTERMIN",  "TSCHADEN", "TREGVEREINB"))) return;

        fixedJoinOrder(left.getFromList());
        fixedJoinOrder(right.getFromList());

        for(QueryTreeNode qtn : right.getFromList()) {
            FromBaseTable fbt = (FromBaseTable) qtn;
            if ("TBATCHTERMIN".equals(fbt.getBaseTableName())) {
                FormatableProperties tableProps = new FormatableProperties();
                tableProps.put("index", "XPFBATTERMIN");
                fbt.setProperties(tableProps);
            }
        }
    }

    private static void hintDB11942(ResultSetNode rsn) {
        if (!(rsn instanceof SelectNode))
            return;
        SelectNode sn = (SelectNode) rsn;

        if (!hasTables(sn.fromList, Arrays.asList("TARCHIVINDEX"))) {
            return;
        }

        for(QueryTreeNode qtn : sn.getFromList()) {
            FromBaseTable fbt = (FromBaseTable) qtn;
            FormatableProperties tableProps = new FormatableProperties();
            tableProps.put("index", "XPFARCHIXGF");
            fbt.setProperties(tableProps);
        }
    }

    private static void hintDB11941(ResultSetNode rsn) {
        if (!(rsn instanceof SelectNode))
            return;
        SelectNode sn = (SelectNode) rsn;

        if (!hasTables(sn.fromList, Arrays.asList("TSEPA_MANDAT"))) {
            return;
        }

        for(QueryTreeNode qtn : sn.getFromList()) {
            FromBaseTable fbt = (FromBaseTable) qtn;
            for(ConglomerateDescriptor cd : fbt.getTableDescriptor().getConglomerateDescriptorList()) {
                if (cd.isConstraint()) {
                    FormatableProperties tableProps = new FormatableProperties();
                    tableProps.put("index", cd.getConglomerateName());
                    fbt.setProperties(tableProps);
                    return;
                }
            }
        }
    }

    private static void hintDB11940(ResultSetNode rsn) throws StandardException {
        if (!(rsn instanceof SelectNode))
            return;
        SelectNode sn = (SelectNode) rsn;

        if (!sn.hasDistinct()) return;

        if (!hasTables(sn.fromList, Arrays.asList("TVERTRAG", "TVORSCHREIBUNG", "TBUBASIS"))) {
            return;
        }

        fixedJoinOrder(sn.getFromList());

        for(QueryTreeNode qtn : sn.getFromList()) {
            FromBaseTable fbt = (FromBaseTable) qtn;
            if ("TVERTRAG".equals(fbt.getBaseTableName())) {
                FormatableProperties tableProps = new FormatableProperties();
                tableProps.put("index", "XCLPOLNR");
                fbt.setProperties(tableProps);
            }
            if ("TVORSCHREIBUNG".equals(fbt.getBaseTableName())) {
                FormatableProperties tableProps = new FormatableProperties();
                tableProps.put("joinStrategy", "nestedloop");
                tableProps.put("index", "XPFVSCHRKONTOSALDO");
                fbt.setProperties(tableProps);
            }
        }
    }

    private static String sqlStatementId(String stmt) {
         try {
             return SQLStatement.getSqlStatement(stmt).getId();
         } catch (Exception e) {
             return "X#ERROR#";
         }
    }

    private static boolean hasTables(FromList fromList, List<String> tableNames) {
        if (fromList == null || fromList.isEmpty())
            return false;
        if (fromList.size() != tableNames.size())
            return false;
        Iterator<String> it = tableNames.iterator();
        for (QueryTreeNode qtn : fromList) {
            FromBaseTable fbt = (FromBaseTable) qtn;
            String name = it.next();
            if (!name.equals(fbt.getBaseTableName()))
                return false;
        }
        return true;
    }


    private static void fixedJoinOrder(FromList fromList) throws StandardException {
        FormatableProperties fromProps = new FormatableProperties();
        fromProps.put("joinOrder", "fixed");
        fromList.setProperties(fromProps);
    }

}
