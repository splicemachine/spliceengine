package com.splicemachine.db.impl.sql.compile.subquery;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.context.ContextManager;
import com.splicemachine.db.iapi.sql.compile.C_NodeTypes;
import com.splicemachine.db.iapi.sql.compile.NodeFactory;
import com.splicemachine.db.impl.sql.compile.ColumnReference;
import com.splicemachine.db.impl.sql.compile.FromSubquery;
import com.splicemachine.db.impl.sql.compile.ResultColumn;

/**
 * Shared code for creating a ColumnReference to a ResultColumn in a FromSubquery.
 */
public class FromSubqueryColRefFactory {

    /**
     * We have a recently created FromSubquery and we want to create a new ColumnReference to it.
     *
     * EXAMPLE:
     * <pre>
     *     select A.*
     *       from A
     *       join (select 1,b1,sum(b2) from B where b2 > x) foo
     *       where a1 > 1;
     * </pre>
     *
     * Here we are creating a column reference that can be used in the outer query to reference foo.b1 so that we can
     * add predicates to the outer query such as a1 = foo.b1
     *
     * @param outerSelectNestingLevel select node of the outer query.
     * @param newFromSubquery         the FromSubquery we are creating as part of subquery flattening.
     * @param fromSubqueryColumnToRef 0-based index of the FromSubquery column we want to reference.
     */
    public static ColumnReference build(int outerSelectNestingLevel,
                                        FromSubquery newFromSubquery,
                                        int fromSubqueryColumnToRef,
                                        NodeFactory nodeFactory,
                                        ContextManager contextManager) throws StandardException {

        ResultColumn resultColumn = newFromSubquery.getResultColumns().elementAt(fromSubqueryColumnToRef);

        ColumnReference colRef = (ColumnReference) nodeFactory.getNode(C_NodeTypes.COLUMN_REFERENCE,
                resultColumn.getName(),
                newFromSubquery.getTableName(),
                contextManager);

        colRef.setSource(resultColumn);
        colRef.setTableNumber(newFromSubquery.getTableNumber());
        colRef.setColumnNumber(resultColumn.getVirtualColumnId());
        colRef.setNestingLevel(outerSelectNestingLevel);
        colRef.setSourceLevel(outerSelectNestingLevel);

        return colRef;
    }

}
