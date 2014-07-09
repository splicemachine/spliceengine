package com.splicemachine.derby.impl.ast;

import com.splicemachine.hbase.HBaseRegionLoads;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.compile.Visitable;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.impl.sql.compile.*;
import org.apache.hadoop.hbase.HServerLoad;

import java.util.Collection;
import java.util.TreeSet;

/**
 * Created by jyuan on 7/8/14.
 */
public class XPlainTraceVisitor extends AbstractSpliceVisitor  {

    private static TreeSet<String> xplainTables;

    static{
        xplainTables = new TreeSet<String>();

        xplainTables.add("SYSSTATEMENTHISTORY");
        xplainTables.add("SYSOPERATIONHISTORY");
        xplainTables.add("SYSTASKHISTORY");
    }

    @Override
    public Visitable defaultVisit(Visitable node) throws StandardException {

        if (node instanceof FromBaseTable) {
            FromBaseTable table = (FromBaseTable) node;
            TableName tableName = table.getTableName();
            String sName = tableName.getSchemaName();
            String tName = tableName.getTableName();

            LanguageConnectionContext lcc = (LanguageConnectionContext) table.getContextManager().
                    getContext(LanguageConnectionContext.CONTEXT_ID);

            if ((sName == null || sName.compareToIgnoreCase("SYS") == 0) &&
                    xplainTables.contains(tName.toUpperCase())) {

                lcc.getStatementContext().setExplainTablesOrProcedures(true);
            }
            else {
                Collection<HServerLoad.RegionLoad> regionLoads =
                        HBaseRegionLoads
                                .getCachedRegionLoadsForTable(Long.toString(table.getTableDescriptor()
                                        .getHeapConglomerateId()));
                if (regionLoads != null
                        && regionLoads.size() > lcc.getStatementContext().getMaxCardinality()){
                    lcc.getStatementContext().setMaxCardinality(regionLoads.size());
                }
            }

        }
        return node;
    }
}
