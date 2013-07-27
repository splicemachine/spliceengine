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
    public Visitable defaultVisit(Visitable node){
        return node;
    }

    @Override
    public Visitable visit(CurrentOfNode node) {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(DistinctNode node) {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(FromBaseTable node) {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(FromSubquery node) {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(FromTable node) {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(FromVTI node) {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(GroupByNode node) {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(HalfOuterJoinNode node) {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(HashTableNode node) {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(IndexToBaseRowNode node) {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(IntersectOrExceptNode node) {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(JoinNode node) {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(MaterializeResultSetNode node) {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(NormalizeResultSetNode node) {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(OrderByNode node) {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(ProjectRestrictNode node) {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(RowCountNode node) {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(RowResultSetNode node) {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(ScrollInsensitiveResultSetNode node) {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(SelectNode node) {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(UnionNode node) {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(WindowResultSetNode node) {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(AggregateNode node) {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(AggregateWindowFunctionNode node) {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(AllResultColumn node) {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(AlterTableNode node) {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(AndNode node) {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(BaseColumnNode node) {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(BetweenOperatorNode node) {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(BinaryArithmeticOperatorNode node) {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(BinaryComparisonOperatorNode node) {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(BinaryListOperatorNode node) {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(BinaryOperatorNode node) {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(BinaryRelationalOperatorNode node) {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(BitConstantNode node) {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(BooleanConstantNode node) {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(CallStatementNode node) {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(CastNode node) {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(CharConstantNode node) {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(CoalesceFunctionNode node) {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(ColumnDefinitionNode node) {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(ColumnReference node) {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(ConcatenationOperatorNode node) {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(ConditionalNode node) {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(ConstraintDefinitionNode node) {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(CreateAliasNode node) {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(CreateIndexNode node) {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(CreateRoleNode node) {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(CreateSchemaNode node) {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(CreateSequenceNode node) {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(CreateTableNode node) {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(CreateTriggerNode node) {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(CreateViewNode node) {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(CurrentDatetimeOperatorNode node) {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(CurrentRowLocationNode node) {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(CursorNode node) {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(DB2LengthOperatorNode node) {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(DefaultNode node) {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(DeleteNode node) {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(DropAliasNode node) {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(DropIndexNode node) {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(DropRoleNode node) {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(DropSchemaNode node) {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(DropSequenceNode node) {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(DropTableNode node) {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(DropTriggerNode node) {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(DropViewNode node) {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(ExecSPSNode node) {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(ExtractOperatorNode node) {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(FKConstraintDefinitionNode node) {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(FromList node) {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(GenerationClauseNode node) {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(GetCurrentConnectionNode node) {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(GrantNode node) {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(GrantRoleNode node) {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(GroupByColumn node) {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(GroupByList node) {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(InListOperatorNode node) {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(InsertNode node) {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(IsNode node) {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(IsNullNode node) {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(JavaToSQLValueNode node) {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(LengthOperatorNode node) {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(LikeEscapeOperatorNode node) {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(LockTableNode node) {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(ModifyColumnNode node) {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(NewInvocationNode node) {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(NextSequenceNode node) {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(NonStaticMethodCallNode node) {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(NOPStatementNode node) {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(NotNode node) {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(NumericConstantNode node) {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(OrderByColumn node) {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(OrderByList node) {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(OrderedColumn node) {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(OrderedColumnList node) {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(OrNode node) {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(ParameterNode node) {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(Predicate node) {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(PredicateList node) {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(PrivilegeNode node) {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(QueryTreeNode node) {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(RenameNode node) {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(ResultColumn node) {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(ResultColumnList node) {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(ResultSetNode node) {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(RevokeNode node) {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(RevokeRoleNode node) {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(RowNumberFunctionNode node) {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(SavepointNode node) {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(SetRoleNode node) {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(SetSchemaNode node) {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(SetTransactionIsolationNode node) {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(SimpleStringOperatorNode node) {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(SpecialFunctionNode node) {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(SQLBooleanConstantNode node) {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(SQLToJavaValueNode node) {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(StatementNode node) {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(StaticClassFieldReferenceNode node) {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(StaticMethodCallNode node) {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(SubqueryList node) {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(SubqueryNode node) {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(TableElementList node) {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(TableElementNode node) {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(TableName node) {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(TablePrivilegesNode node) {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(TernaryOperatorNode node) {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(TestConstraintNode node) {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(TimestampOperatorNode node) {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(UnaryArithmeticOperatorNode node) {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(UnaryComparisonOperatorNode node) {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(UnaryDateTimestampOperatorNode node) {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(UnaryLogicalOperatorNode node) {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(UnaryOperatorNode node) {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(UntypedNullConstantNode node) {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(UpdateNode node) {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(UserTypeConstantNode node) {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(ValueNode node) {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(ValueNodeList node) {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(VarbitConstantNode node) {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(VirtualColumnNode node) {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(WindowDefinitionNode node) {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(WindowFunctionNode node) {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(WindowList node) {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(WindowNode node) {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(WindowReferenceNode node) {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(XMLConstantNode node) {
        return defaultVisit(node);
    }

}
