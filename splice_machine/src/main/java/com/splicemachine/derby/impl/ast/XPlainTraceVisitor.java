package com.splicemachine.derby.impl.ast;

import com.splicemachine.derby.hbase.DerbyFactory;
import com.splicemachine.derby.hbase.DerbyFactoryDriver;
import com.splicemachine.hbase.HBaseRegionLoads;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.compile.Visitable;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.impl.sql.compile.*;

import java.util.Collection;
import java.util.TreeSet;

/**
 * Created by jyuan on 7/8/14.
 */
public class XPlainTraceVisitor extends AbstractSpliceVisitor  {
	protected static DerbyFactory derbyFactory = DerbyFactoryDriver.derbyFactory;
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

                lcc.getStatementContext().setXPlainTableOrProcedure(true);
            }
            else {
            	derbyFactory.setMaxCardinalityBasedOnRegionLoad(Long.toString(table.getTableDescriptor()
                                        .getHeapConglomerateId()), lcc);
            }

        }
        return node;
    }
}
