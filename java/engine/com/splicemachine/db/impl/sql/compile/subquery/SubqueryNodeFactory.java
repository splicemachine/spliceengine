package com.splicemachine.db.impl.sql.compile.subquery;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.context.ContextManager;
import com.splicemachine.db.iapi.sql.compile.C_NodeTypes;
import com.splicemachine.db.iapi.sql.compile.NodeFactory;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;
import com.splicemachine.db.iapi.types.TypeId;
import com.splicemachine.db.impl.sql.compile.*;

/**
 * Higher level NodeFactory API for use in subquery flattening code.
 */
public class SubqueryNodeFactory {

    private NodeFactory nodeFactory;
    private ContextManager contextManager;

    public SubqueryNodeFactory(ContextManager contextManager, NodeFactory nodeFactory) {
        this.contextManager = contextManager;
        this.nodeFactory = nodeFactory;
    }

    /**
     * AndNode
     */
    public AndNode buildAndNode() throws StandardException {
        ValueNode left = buildBooleanTrue();
        ValueNode right = buildBooleanTrue();
        return (AndNode) nodeFactory.getNode(C_NodeTypes.AND_NODE, left, right, contextManager);
    }

    /**
     * IsNullNode
     */
    public IsNullNode buildIsNullNode(ColumnReference columnReference) throws StandardException {
        IsNullNode node = (IsNullNode) nodeFactory.getNode(C_NodeTypes.IS_NULL_NODE, columnReference, contextManager);
        node.bindComparisonOperator();
        return node;
    }

    /**
     * BooleanConstantNode -- TRUE
     */
    public BooleanConstantNode buildBooleanTrue() throws StandardException {
        BooleanConstantNode trueNode = (BooleanConstantNode) nodeFactory.getNode(C_NodeTypes.BOOLEAN_CONSTANT_NODE, Boolean.TRUE, contextManager);
        trueNode.setType(new DataTypeDescriptor(TypeId.BOOLEAN_ID, false));
        return trueNode;
    }

    /**
     * FromSubquery
     */
    public FromSubquery buildFromSubqueryNode(SelectNode outerSelectNode,
                                              SubqueryNode subqueryNode,
                                              ResultSetNode subqueryResultSet,
                                              ResultColumnList newRcl,
                                              String subqueryAlias) throws StandardException {
        FromSubquery fromSubquery = (FromSubquery) nodeFactory.getNode(C_NodeTypes.FROM_SUBQUERY,
                subqueryResultSet,
                subqueryNode.getOrderByList(),
                subqueryNode.getOffset(),
                subqueryNode.getFetchFirst(),
                subqueryNode.hasJDBClimitClause(),
                subqueryAlias,
                newRcl,
                null,
                contextManager);
        fromSubquery.setTableNumber(outerSelectNode.getCompilerContext().getNextTableNumber());
        return fromSubquery;
    }

    /**
     * HalfOuterJoinNode
     */
    public HalfOuterJoinNode buildOuterJoinNode(FromList outerFromList,
                                                FromSubquery fromSubquery,
                                                ValueNode joinClause) throws StandardException {
        /* Currently we only attempt not-exist flattening if the outer table has one table. */
        assert outerFromList.size() == 1 : "expected only one FromList element at this point";
        QueryTreeNode outerTable = outerFromList.getNodes().get(0);

        HalfOuterJoinNode outerJoinNode = (HalfOuterJoinNode) nodeFactory.getNode(
                C_NodeTypes.HALF_OUTER_JOIN_NODE,
                outerTable,                          // left side  - will be outer table(s)
                fromSubquery,                        // right side - will be FromSubquery
                joinClause,                          // join clause
                null,                                // using clause
                Boolean.FALSE,                       // is right join
                null,                                // table props
                contextManager);
        outerJoinNode.setTableNumber(fromSubquery.getCompilerContext().getNextTableNumber());
        outerJoinNode.LOJ_bindResultColumns(true);
        return outerJoinNode;
    }

}