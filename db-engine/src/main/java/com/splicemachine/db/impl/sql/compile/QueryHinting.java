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

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class QueryHinting {
    public static void hint(StatementNode statementNode) {
        try {
            if (!(statementNode instanceof CursorNode))
                return;
            CursorNode cn = (CursorNode) statementNode;
            ResultSetNode rsn = cn.getResultSetNode();
            if (!(rsn instanceof SelectNode))
                return;
            SelectNode sn = (SelectNode) rsn;

            String stmtId = sqlStatementId(statementNode.getLanguageConnectionContext().getStatementContext().getStatementText());
            switch (stmtId) {
                case "Sp3QPj57":
                case "#yvYkFUJ": // explain
                    hintDB11940(sn);
                    break;
                case "SmOINPGy":
                case "#Jahtv3X": // explain
                    hintDB11941(sn);
                    break;
                case "S5#YvaXt":
                case "#qj#jlet": // explain
                    hintDB11942(sn);
                    break;
            }
        } catch (Exception e) {
            // ignore, don't hint this query
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

    private static void hintDB11942(SelectNode sn) {
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

    private static void hintDB11941(SelectNode sn) {
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

    private static void hintDB11940(SelectNode sn) throws StandardException {
        if (!sn.hasDistinct()) return;

        if (!hasTables(sn.fromList, Arrays.asList("TVERTRAG", "TVORSCHREIBUNG", "TBUBASIS"))) {
            return;
        }

        FormatableProperties fromProps = new FormatableProperties();
        fromProps.put("joinOrder", "fixed");
        sn.getFromList().setProperties(fromProps);

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

}
