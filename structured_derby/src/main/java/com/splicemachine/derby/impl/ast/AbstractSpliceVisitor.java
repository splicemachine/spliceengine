package com.splicemachine.derby.impl.ast;

import org.apache.derby.iapi.sql.compile.Visitable;
import org.apache.derby.impl.sql.compile.*;

/**
 * User: pjt
 * Date: 7/9/13
 */
public abstract class AbstractSpliceVisitor implements ISpliceVisitor {
    String query;
    int phase;

    @Override
    public void setContext(String query, int phase) {
        this.query = query;
        this.phase = phase;
    }

    @Override
    public boolean isPostOrder() {
        // Default to PostOrder traversal, i.e. bottom-up
        return true;
    }

    @Override
    public boolean stopTraversal() {
        return false;
    }

    @Override
    public boolean skipChildren(Visitable node) {
        return false;
    }

    @Override
    public Visitable visit(CurrentOfNode node) {
        return node;
    }

    @Override
    public Visitable visit(DistinctNode node) {
        return node;
    }

    @Override
    public Visitable visit(FromBaseTable node) {
        return node;
    }

    @Override
    public Visitable visit(FromSubquery node) {
        return node;
    }

    @Override
    public Visitable visit(FromTable node) {
        return node;
    }

    @Override
    public Visitable visit(FromVTI node) {
        return node;
    }

    @Override
    public Visitable visit(GroupByNode node) {
        return node;
    }

    @Override
    public Visitable visit(HalfOuterJoinNode node) {
        return node;
    }

    @Override
    public Visitable visit(HashTableNode node) {
        return node;
    }

    @Override
    public Visitable visit(IndexToBaseRowNode node) {
        return node;
    }

    @Override
    public Visitable visit(IntersectOrExceptNode node) {
        return node;
    }

    @Override
    public Visitable visit(JoinNode node) {
        return node;
    }

    @Override
    public Visitable visit(MaterializeResultSetNode node) {
        return node;
    }

    @Override
    public Visitable visit(NormalizeResultSetNode node) {
        return node;
    }

    @Override
    public Visitable visit(OrderByNode node) {
        return node;
    }

    @Override
    public Visitable visit(ProjectRestrictNode node) {
        return node;
    }

    @Override
    public Visitable visit(RowCountNode node) {
        return node;
    }

    @Override
    public Visitable visit(RowResultSetNode node) {
        return node;
    }

    @Override
    public Visitable visit(ScrollInsensitiveResultSetNode node) {
        return node;
    }

    @Override
    public Visitable visit(SelectNode node) {
        return node;
    }

    @Override
    public Visitable visit(UnionNode node) {
        return node;
    }

    @Override
    public Visitable visit(WindowResultSetNode node) {
        return node;
    }

    @Override
    public Visitable visit(AggregateNode node) {
        return node;
    }

    @Override
    public Visitable visit(AggregateWindowFunctionNode node) {
        return node;
    }

    @Override
    public Visitable visit(AllResultColumn node) {
        return node;
    }

    @Override
    public Visitable visit(AlterTableNode node) {
        return node;
    }

    @Override
    public Visitable visit(AndNode node) {
        return node;
    }

    @Override
    public Visitable visit(BaseColumnNode node) {
        return node;
    }

    @Override
    public Visitable visit(BetweenOperatorNode node) {
        return node;
    }

    @Override
    public Visitable visit(BinaryArithmeticOperatorNode node) {
        return node;
    }

    @Override
    public Visitable visit(BinaryComparisonOperatorNode node) {
        return node;
    }

    @Override
    public Visitable visit(BinaryListOperatorNode node) {
        return node;
    }

    @Override
    public Visitable visit(BinaryOperatorNode node) {
        return node;
    }

    @Override
    public Visitable visit(BinaryRelationalOperatorNode node) {
        return node;
    }

    @Override
    public Visitable visit(BitConstantNode node) {
        return node;
    }

    @Override
    public Visitable visit(BooleanConstantNode node) {
        return node;
    }

    @Override
    public Visitable visit(CallStatementNode node) {
        return node;
    }

    @Override
    public Visitable visit(CastNode node) {
        return node;
    }

    @Override
    public Visitable visit(CharConstantNode node) {
        return node;
    }

    @Override
    public Visitable visit(CoalesceFunctionNode node) {
        return node;
    }

    @Override
    public Visitable visit(ColumnDefinitionNode node) {
        return node;
    }

    @Override
    public Visitable visit(ColumnReference node) {
        return node;
    }

    @Override
    public Visitable visit(ConcatenationOperatorNode node) {
        return node;
    }

    @Override
    public Visitable visit(ConditionalNode node) {
        return node;
    }

    @Override
    public Visitable visit(ConstraintDefinitionNode node) {
        return node;
    }

    @Override
    public Visitable visit(CreateAliasNode node) {
        return node;
    }

    @Override
    public Visitable visit(CreateIndexNode node) {
        return node;
    }

    @Override
    public Visitable visit(CreateRoleNode node) {
        return node;
    }

    @Override
    public Visitable visit(CreateSchemaNode node) {
        return node;
    }

    @Override
    public Visitable visit(CreateSequenceNode node) {
        return node;
    }

    @Override
    public Visitable visit(CreateTableNode node) {
        return node;
    }

    @Override
    public Visitable visit(CreateTriggerNode node) {
        return node;
    }

    @Override
    public Visitable visit(CreateViewNode node) {
        return node;
    }

    @Override
    public Visitable visit(CurrentDatetimeOperatorNode node) {
        return node;
    }

    @Override
    public Visitable visit(CurrentRowLocationNode node) {
        return node;
    }

    @Override
    public Visitable visit(CursorNode node) {
        return node;
    }

    @Override
    public Visitable visit(DB2LengthOperatorNode node) {
        return node;
    }

    @Override
    public Visitable visit(DefaultNode node) {
        return node;
    }

    @Override
    public Visitable visit(DeleteNode node) {
        return node;
    }

    @Override
    public Visitable visit(DropAliasNode node) {
        return node;
    }

    @Override
    public Visitable visit(DropIndexNode node) {
        return node;
    }

    @Override
    public Visitable visit(DropRoleNode node) {
        return node;
    }

    @Override
    public Visitable visit(DropSchemaNode node) {
        return node;
    }

    @Override
    public Visitable visit(DropSequenceNode node) {
        return node;
    }

    @Override
    public Visitable visit(DropTableNode node) {
        return node;
    }

    @Override
    public Visitable visit(DropTriggerNode node) {
        return node;
    }

    @Override
    public Visitable visit(DropViewNode node) {
        return node;
    }

    @Override
    public Visitable visit(ExecSPSNode node) {
        return node;
    }

    @Override
    public Visitable visit(ExtractOperatorNode node) {
        return node;
    }

    @Override
    public Visitable visit(FKConstraintDefinitionNode node) {
        return node;
    }

    @Override
    public Visitable visit(FromList node) {
        return node;
    }

    @Override
    public Visitable visit(GenerationClauseNode node) {
        return node;
    }

    @Override
    public Visitable visit(GetCurrentConnectionNode node) {
        return node;
    }

    @Override
    public Visitable visit(GrantNode node) {
        return node;
    }

    @Override
    public Visitable visit(GrantRoleNode node) {
        return node;
    }

    @Override
    public Visitable visit(GroupByColumn node) {
        return node;
    }

    @Override
    public Visitable visit(GroupByList node) {
        return node;
    }

    @Override
    public Visitable visit(InListOperatorNode node) {
        return node;
    }

    @Override
    public Visitable visit(InsertNode node) {
        return node;
    }

    @Override
    public Visitable visit(IsNode node) {
        return node;
    }

    @Override
    public Visitable visit(IsNullNode node) {
        return node;
    }

    @Override
    public Visitable visit(JavaToSQLValueNode node) {
        return node;
    }

    @Override
    public Visitable visit(LengthOperatorNode node) {
        return node;
    }

    @Override
    public Visitable visit(LikeEscapeOperatorNode node) {
        return node;
    }

    @Override
    public Visitable visit(LockTableNode node) {
        return node;
    }

    @Override
    public Visitable visit(ModifyColumnNode node) {
        return node;
    }

    @Override
    public Visitable visit(NewInvocationNode node) {
        return node;
    }

    @Override
    public Visitable visit(NextSequenceNode node) {
        return node;
    }

    @Override
    public Visitable visit(NonStaticMethodCallNode node) {
        return node;
    }

    @Override
    public Visitable visit(NOPStatementNode node) {
        return node;
    }

    @Override
    public Visitable visit(NotNode node) {
        return node;
    }

    @Override
    public Visitable visit(NumericConstantNode node) {
        return node;
    }

    @Override
    public Visitable visit(OrderByColumn node) {
        return node;
    }

    @Override
    public Visitable visit(OrderByList node) {
        return node;
    }

    @Override
    public Visitable visit(OrderedColumn node) {
        return node;
    }

    @Override
    public Visitable visit(OrderedColumnList node) {
        return node;
    }

    @Override
    public Visitable visit(OrNode node) {
        return node;
    }

    @Override
    public Visitable visit(ParameterNode node) {
        return node;
    }

    @Override
    public Visitable visit(Predicate node) {
        return node;
    }

    @Override
    public Visitable visit(PredicateList node) {
        return node;
    }

    @Override
    public Visitable visit(PrivilegeNode node) {
        return node;
    }

    @Override
    public Visitable visit(QueryTreeNode node) {
        return node;
    }

    @Override
    public Visitable visit(RenameNode node) {
        return node;
    }

    @Override
    public Visitable visit(ResultColumn node) {
        return node;
    }

    @Override
    public Visitable visit(ResultColumnList node) {
        return node;
    }

    @Override
    public Visitable visit(ResultSetNode node) {
        return node;
    }

    @Override
    public Visitable visit(RevokeNode node) {
        return node;
    }

    @Override
    public Visitable visit(RevokeRoleNode node) {
        return node;
    }

    @Override
    public Visitable visit(RowNumberFunctionNode node) {
        return node;
    }

    @Override
    public Visitable visit(SavepointNode node) {
        return node;
    }

    @Override
    public Visitable visit(SetRoleNode node) {
        return node;
    }

    @Override
    public Visitable visit(SetSchemaNode node) {
        return node;
    }

    @Override
    public Visitable visit(SetTransactionIsolationNode node) {
        return node;
    }

    @Override
    public Visitable visit(SimpleStringOperatorNode node) {
        return node;
    }

    @Override
    public Visitable visit(SpecialFunctionNode node) {
        return node;
    }

    @Override
    public Visitable visit(SQLBooleanConstantNode node) {
        return node;
    }

    @Override
    public Visitable visit(SQLToJavaValueNode node) {
        return node;
    }

    @Override
    public Visitable visit(StatementNode node) {
        return node;
    }

    @Override
    public Visitable visit(StaticClassFieldReferenceNode node) {
        return node;
    }

    @Override
    public Visitable visit(StaticMethodCallNode node) {
        return node;
    }

    @Override
    public Visitable visit(SubqueryList node) {
        return node;
    }

    @Override
    public Visitable visit(SubqueryNode node) {
        return node;
    }

    @Override
    public Visitable visit(TableElementList node) {
        return node;
    }

    @Override
    public Visitable visit(TableElementNode node) {
        return node;
    }

    @Override
    public Visitable visit(TableName node) {
        return node;
    }

    @Override
    public Visitable visit(TablePrivilegesNode node) {
        return node;
    }

    @Override
    public Visitable visit(TernaryOperatorNode node) {
        return node;
    }

    @Override
    public Visitable visit(TestConstraintNode node) {
        return node;
    }

    @Override
    public Visitable visit(TimestampOperatorNode node) {
        return node;
    }

    @Override
    public Visitable visit(UnaryArithmeticOperatorNode node) {
        return node;
    }

    @Override
    public Visitable visit(UnaryComparisonOperatorNode node) {
        return node;
    }

    @Override
    public Visitable visit(UnaryDateTimestampOperatorNode node) {
        return node;
    }

    @Override
    public Visitable visit(UnaryLogicalOperatorNode node) {
        return node;
    }

    @Override
    public Visitable visit(UnaryOperatorNode node) {
        return node;
    }

    @Override
    public Visitable visit(UntypedNullConstantNode node) {
        return node;
    }

    @Override
    public Visitable visit(UpdateNode node) {
        return node;
    }

    @Override
    public Visitable visit(UserTypeConstantNode node) {
        return node;
    }

    @Override
    public Visitable visit(ValueNode node) {
        return node;
    }

    @Override
    public Visitable visit(ValueNodeList node) {
        return node;
    }

    @Override
    public Visitable visit(VarbitConstantNode node) {
        return node;
    }

    @Override
    public Visitable visit(VirtualColumnNode node) {
        return node;
    }

    @Override
    public Visitable visit(WindowDefinitionNode node) {
        return node;
    }

    @Override
    public Visitable visit(WindowFunctionNode node) {
        return node;
    }

    @Override
    public Visitable visit(WindowList node) {
        return node;
    }

    @Override
    public Visitable visit(WindowNode node) {
        return node;
    }

    @Override
    public Visitable visit(WindowReferenceNode node) {
        return node;
    }

    @Override
    public Visitable visit(XMLConstantNode node) {
        return node;
    }

}
