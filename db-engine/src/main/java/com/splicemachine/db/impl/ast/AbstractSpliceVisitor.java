/*
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
 * Some parts of this source code are based on Apache Derby, and the following notices apply to
 * Apache Derby:
 *
 * Apache Derby is a subproject of the Apache DB project, and is licensed under
 * the Apache License, Version 2.0 (the "License"); you may not use these files
 * except in compliance with the License. You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * Splice Machine, Inc. has modified the Apache Derby code in this file.
 *
 * All such Splice Machine modifications are Copyright 2012 - 2019 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */

package com.splicemachine.db.impl.ast;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.compile.CompilationPhase;
import com.splicemachine.db.iapi.sql.compile.Visitable;
import com.splicemachine.db.impl.sql.compile.*;
import com.splicemachine.db.iapi.ast.ISpliceVisitor;

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
    CompilationPhase phase;

    @Override
    public void setContext(String query, CompilationPhase phase) {
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
    public Visitable visit(DenseRankFunctionNode node) throws StandardException {
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
    public Visitable visit(ArrayOperatorNode node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(ArrayConstantNode node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(FirstLastValueFunctionNode node) throws StandardException {
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
    public Visitable visit(LeadLagFunctionNode node) throws StandardException {
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
    public Visitable visit(RankFunctionNode node) throws StandardException {
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
    public Visitable visit(SimpleLocaleStringOperatorNode node) throws StandardException {
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
    public Visitable visit(BasicPrivilegesNode node) throws StandardException {
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
    public Visitable visit(TruncateOperatorNode node) throws StandardException {
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
    public Visitable visit(WrappedAggregateFunctionNode node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(XMLConstantNode node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(ExplainNode node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(ExportNode node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(BinaryExportNode node) throws StandardException {
        return defaultVisit(node);
    }

	@Override
	public Visitable visit(OrderByNode node) throws StandardException {
        return defaultVisit(node);
	}

    @Override
    public Visitable visit(BatchOnceNode node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(CreatePinNode node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(DropPinNode node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(SetSessionPropertyNode node) throws StandardException {
        return defaultVisit(node);
    }
    
    @Override
    public Visitable visit(ListValueNode node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(GroupingFunctionNode node) throws StandardException {
        return defaultVisit(node);
    }

    @Override
    public Visitable visit(SelfReferenceNode node) throws StandardException {
        return defaultVisit(node);
    }
}
