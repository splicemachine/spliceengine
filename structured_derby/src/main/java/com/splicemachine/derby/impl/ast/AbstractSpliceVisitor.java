package com.splicemachine.derby.impl.ast;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.compile.Visitable;
import org.apache.derby.impl.sql.compile.*;

/**
 * Default ISpliceVisitor implementation, which provides identity behavior for each of Derby's Visitable nodes.
 * Default behavior is encoded in the defaultVisit, which subclasses can override in addition to individual
 * visit() methods.
 *
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
    public Visitable defaultVisit(Visitable node) throws StandardException {
        return node;
    }

    @Override
    public Visitable visit(CurrentOfNode node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(DistinctNode node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(FromBaseTable node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(FromSubquery node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(FromTable node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(FromVTI node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(GroupByNode node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(HalfOuterJoinNode node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(HashTableNode node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(IndexToBaseRowNode node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(IntersectOrExceptNode node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(JoinNode node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(MaterializeResultSetNode node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(NormalizeResultSetNode node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(OrderByNode node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(ProjectRestrictNode node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(RowCountNode node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(RowResultSetNode node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(ScrollInsensitiveResultSetNode node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(SelectNode node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(UnionNode node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(WindowResultSetNode node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(AggregateNode node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(AggregateWindowFunctionNode node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(AllResultColumn node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(AlterTableNode node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(AndNode node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(BaseColumnNode node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(BetweenOperatorNode node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(BinaryArithmeticOperatorNode node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(BinaryComparisonOperatorNode node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(BinaryListOperatorNode node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(BinaryOperatorNode node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(BinaryRelationalOperatorNode node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(BitConstantNode node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(BooleanConstantNode node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(CallStatementNode node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(CastNode node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(CharConstantNode node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(CoalesceFunctionNode node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(ColumnDefinitionNode node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(ColumnReference node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(ConcatenationOperatorNode node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(ConditionalNode node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(ConstraintDefinitionNode node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(CreateAliasNode node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(CreateIndexNode node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(CreateRoleNode node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(CreateSchemaNode node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(CreateSequenceNode node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(CreateTableNode node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(CreateTriggerNode node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(CreateViewNode node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(CurrentDatetimeOperatorNode node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(CurrentRowLocationNode node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(CursorNode node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(DB2LengthOperatorNode node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(DefaultNode node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(DeleteNode node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(DropAliasNode node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(DropIndexNode node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(DropRoleNode node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(DropSchemaNode node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(DropSequenceNode node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(DropTableNode node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(DropTriggerNode node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(DropViewNode node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(ExecSPSNode node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(ExtractOperatorNode node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(FKConstraintDefinitionNode node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(FromList node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(GenerationClauseNode node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(GetCurrentConnectionNode node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(GrantNode node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(GrantRoleNode node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(GroupByColumn node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(GroupByList node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(InListOperatorNode node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(InsertNode node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(IsNode node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(IsNullNode node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(JavaToSQLValueNode node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(LengthOperatorNode node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(LikeEscapeOperatorNode node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(LockTableNode node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(ModifyColumnNode node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(NewInvocationNode node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(NextSequenceNode node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(NonStaticMethodCallNode node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(NOPStatementNode node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(NotNode node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(NumericConstantNode node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(OrderByColumn node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(OrderByList node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(OrderedColumn node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(OrderedColumnList node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(OrNode node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(ParameterNode node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(Predicate node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(PredicateList node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(PrivilegeNode node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(QueryTreeNode node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(RenameNode node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(ResultColumn node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(ResultColumnList node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(ResultSetNode node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(RevokeNode node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(RevokeRoleNode node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(RowNumberFunctionNode node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(SavepointNode node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(SetRoleNode node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(SetSchemaNode node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(SetTransactionIsolationNode node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(SimpleStringOperatorNode node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(SpecialFunctionNode node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(SQLBooleanConstantNode node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(SQLToJavaValueNode node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(StatementNode node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(StaticClassFieldReferenceNode node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(StaticMethodCallNode node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(SubqueryList node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(SubqueryNode node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(TableElementList node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(TableElementNode node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(TableName node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(TablePrivilegesNode node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(TernaryOperatorNode node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(TestConstraintNode node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(TimestampOperatorNode node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(UnaryArithmeticOperatorNode node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(UnaryComparisonOperatorNode node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(UnaryDateTimestampOperatorNode node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(UnaryLogicalOperatorNode node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(UnaryOperatorNode node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(UntypedNullConstantNode node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(UpdateNode node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(UserTypeConstantNode node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(ValueNode node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(ValueNodeList node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(VarbitConstantNode node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(VirtualColumnNode node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(WindowDefinitionNode node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(WindowFunctionNode node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(WindowList node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(WindowNode node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(WindowReferenceNode node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(XMLConstantNode node) throws StandardException {
        return defaultVisit(node);
    }

}
