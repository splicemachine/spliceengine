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

package com.splicemachine.db.iapi.ast;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.compile.CompilationPhase;
import com.splicemachine.db.iapi.sql.compile.Visitable;
import com.splicemachine.db.impl.sql.compile.*;

/**
 * Visitor interface with methods for each of Derby's Visitable nodes (which make up the QueryTreeNode hierarchy).
 *
 * User: pjt
 * Date: 7/9/13
 */
public interface ISpliceVisitor {
    void setContext(String query, CompilationPhase phase);
    boolean isPostOrder();
    boolean stopTraversal();
    boolean skipChildren(Visitable node);

    Visitable defaultVisit(Visitable node) throws StandardException;

    // ResultSet nodes
    Visitable visit(CurrentOfNode node) throws StandardException;
    Visitable visit(DistinctNode node) throws StandardException;
    Visitable visit(FromBaseTable node) throws StandardException;
    Visitable visit(FromSubquery node) throws StandardException;
    Visitable visit(FromTable node) throws StandardException;
    Visitable visit(FromVTI node) throws StandardException;
    Visitable visit(GroupByNode node) throws StandardException;
    Visitable visit(HalfOuterJoinNode node) throws StandardException;
    Visitable visit(HashTableNode node) throws StandardException;
    Visitable visit(IndexToBaseRowNode node) throws StandardException;
    Visitable visit(IntersectOrExceptNode node) throws StandardException;
    Visitable visit(JoinNode node) throws StandardException;
    Visitable visit(MaterializeResultSetNode node) throws StandardException;
    Visitable visit(NormalizeResultSetNode node) throws StandardException;
    Visitable visit(OrderByNode node) throws StandardException;
    Visitable visit(ProjectRestrictNode node) throws StandardException;
    Visitable visit(RowCountNode node) throws StandardException;
    Visitable visit(RowResultSetNode node) throws StandardException;
    Visitable visit(ScrollInsensitiveResultSetNode node) throws StandardException;
    Visitable visit(SelectNode node) throws StandardException;
    Visitable visit(UnionNode node) throws StandardException;
    Visitable visit(WindowResultSetNode node) throws StandardException;

    // Rest
    Visitable visit(AggregateNode node) throws StandardException;
    Visitable visit(AllResultColumn node) throws StandardException;
    Visitable visit(AlterTableNode node) throws StandardException;
    Visitable visit(AndNode node) throws StandardException;
    Visitable visit(BaseColumnNode node) throws StandardException;
    Visitable visit(BetweenOperatorNode node) throws StandardException;
    Visitable visit(BinaryArithmeticOperatorNode node) throws StandardException;
    Visitable visit(BinaryComparisonOperatorNode node) throws StandardException;
    Visitable visit(BinaryListOperatorNode node) throws StandardException;
    Visitable visit(BinaryOperatorNode node) throws StandardException;
    Visitable visit(BinaryRelationalOperatorNode node) throws StandardException;
    Visitable visit(BitConstantNode node) throws StandardException;
    Visitable visit(BooleanConstantNode node) throws StandardException;
    Visitable visit(CallStatementNode node) throws StandardException;
    Visitable visit(CastNode node) throws StandardException;
    Visitable visit(CharConstantNode node) throws StandardException;
    Visitable visit(CoalesceFunctionNode node) throws StandardException;
    Visitable visit(ColumnDefinitionNode node) throws StandardException;
    Visitable visit(ColumnReference node) throws StandardException;
    Visitable visit(ConcatenationOperatorNode node) throws StandardException;
    Visitable visit(ConditionalNode node) throws StandardException;
    Visitable visit(ConstraintDefinitionNode node) throws StandardException;
    Visitable visit(CreateAliasNode node) throws StandardException;
    Visitable visit(CreateIndexNode node) throws StandardException;
    Visitable visit(CreateRoleNode node) throws StandardException;
    Visitable visit(CreateSchemaNode node) throws StandardException;
    Visitable visit(CreateSequenceNode node) throws StandardException;
    Visitable visit(CreateTableNode node) throws StandardException;
    Visitable visit(CreateTriggerNode node) throws StandardException;
    Visitable visit(CreateViewNode node) throws StandardException;
    Visitable visit(CurrentDatetimeOperatorNode node) throws StandardException;
    Visitable visit(CurrentRowLocationNode node) throws StandardException;
    Visitable visit(CursorNode node) throws StandardException;
    Visitable visit(DB2LengthOperatorNode node) throws StandardException;
    Visitable visit(DefaultNode node) throws StandardException;
    Visitable visit(DeleteNode node) throws StandardException;
    Visitable visit(DenseRankFunctionNode node) throws StandardException;
    Visitable visit(DropAliasNode node) throws StandardException;
    Visitable visit(DropIndexNode node) throws StandardException;
    Visitable visit(DropRoleNode node) throws StandardException;
    Visitable visit(DropSchemaNode node) throws StandardException;
    Visitable visit(DropSequenceNode node) throws StandardException;
    Visitable visit(DropTableNode node) throws StandardException;
    Visitable visit(DropTriggerNode node) throws StandardException;
    Visitable visit(DropViewNode node) throws StandardException;
    Visitable visit(ExecSPSNode node) throws StandardException;
    Visitable visit(ExtractOperatorNode node) throws StandardException;
    Visitable visit(FirstLastValueFunctionNode node) throws StandardException;
    Visitable visit(FKConstraintDefinitionNode node) throws StandardException;
    Visitable visit(FromList node) throws StandardException;
    Visitable visit(GenerationClauseNode node) throws StandardException;
    Visitable visit(GetCurrentConnectionNode node) throws StandardException;
    Visitable visit(GrantNode node) throws StandardException;
    Visitable visit(GrantRoleNode node) throws StandardException;
    Visitable visit(GroupByColumn node) throws StandardException;
    Visitable visit(GroupByList node) throws StandardException;
    Visitable visit(InListOperatorNode node) throws StandardException;
    Visitable visit(InsertNode node) throws StandardException;
    Visitable visit(IsNode node) throws StandardException;
    Visitable visit(IsNullNode node) throws StandardException;
    Visitable visit(JavaToSQLValueNode node) throws StandardException;
    Visitable visit(LeadLagFunctionNode node) throws StandardException;
    Visitable visit(LengthOperatorNode node) throws StandardException;
    Visitable visit(LikeEscapeOperatorNode node) throws StandardException;
    Visitable visit(LockTableNode node) throws StandardException;
    Visitable visit(ModifyColumnNode node) throws StandardException;
    Visitable visit(NewInvocationNode node) throws StandardException;
    Visitable visit(NextSequenceNode node) throws StandardException;
    Visitable visit(NonStaticMethodCallNode node) throws StandardException;
    Visitable visit(NOPStatementNode node) throws StandardException;
    Visitable visit(NotNode node) throws StandardException;
    Visitable visit(NumericConstantNode node) throws StandardException;
    Visitable visit(OrderByColumn node) throws StandardException;
    Visitable visit(OrderByList node) throws StandardException;
    Visitable visit(OrderedColumn node) throws StandardException;
    Visitable visit(OrderedColumnList node) throws StandardException;
    Visitable visit(OrNode node) throws StandardException;
    Visitable visit(ParameterNode node) throws StandardException;
    Visitable visit(Predicate node) throws StandardException;
    Visitable visit(PredicateList node) throws StandardException;
    Visitable visit(PrivilegeNode node) throws StandardException;
    Visitable visit(QueryTreeNode node) throws StandardException;
    Visitable visit(RankFunctionNode node) throws StandardException;
    Visitable visit(RenameNode node) throws StandardException;
    Visitable visit(ResultColumn node) throws StandardException;
    Visitable visit(ResultColumnList node) throws StandardException;
    Visitable visit(ResultSetNode node) throws StandardException;
    Visitable visit(RevokeNode node) throws StandardException;
    Visitable visit(RevokeRoleNode node) throws StandardException;
    Visitable visit(RowNumberFunctionNode node) throws StandardException;
    Visitable visit(SavepointNode node) throws StandardException;
    Visitable visit(SetRoleNode node) throws StandardException;
    Visitable visit(SetSchemaNode node) throws StandardException;
    Visitable visit(SetTransactionIsolationNode node) throws StandardException;
    Visitable visit(SimpleStringOperatorNode node) throws StandardException;
    Visitable visit(SimpleLocaleStringOperatorNode node) throws StandardException;
    Visitable visit(SpecialFunctionNode node) throws StandardException;
    Visitable visit(SQLBooleanConstantNode node) throws StandardException;
    Visitable visit(SQLToJavaValueNode node) throws StandardException;
    Visitable visit(StatementNode node) throws StandardException;
    Visitable visit(StaticClassFieldReferenceNode node) throws StandardException;
    Visitable visit(StaticMethodCallNode node) throws StandardException;
    Visitable visit(SubqueryList node) throws StandardException;
    Visitable visit(SubqueryNode node) throws StandardException;
    Visitable visit(TableElementList node) throws StandardException;
    Visitable visit(TableElementNode node) throws StandardException;
    Visitable visit(TableName node) throws StandardException;
    Visitable visit(BasicPrivilegesNode node) throws StandardException;
    Visitable visit(TernaryOperatorNode node) throws StandardException;
    Visitable visit(TestConstraintNode node) throws StandardException;
    Visitable visit(TimestampOperatorNode node) throws StandardException;
    Visitable visit(TruncateOperatorNode node) throws StandardException;
    Visitable visit(UnaryArithmeticOperatorNode node) throws StandardException;
    Visitable visit(UnaryComparisonOperatorNode node) throws StandardException;
    Visitable visit(UnaryDateTimestampOperatorNode node) throws StandardException;
    Visitable visit(UnaryLogicalOperatorNode node) throws StandardException;
    Visitable visit(UnaryOperatorNode node) throws StandardException;
    Visitable visit(UntypedNullConstantNode node) throws StandardException;
    Visitable visit(UpdateNode node) throws StandardException;
    Visitable visit(UserTypeConstantNode node) throws StandardException;
    Visitable visit(ValueNode node) throws StandardException;
    Visitable visit(ValueNodeList node) throws StandardException;
    Visitable visit(VarbitConstantNode node) throws StandardException;
    Visitable visit(VirtualColumnNode node) throws StandardException;
    Visitable visit(WindowDefinitionNode node) throws StandardException;
    Visitable visit(WindowFunctionNode node) throws StandardException;
    Visitable visit(WindowList node) throws StandardException;
    Visitable visit(WindowNode node) throws StandardException;
    Visitable visit(WindowReferenceNode node) throws StandardException;
    Visitable visit(WrappedAggregateFunctionNode node) throws StandardException;
    Visitable visit(XMLConstantNode node) throws StandardException;
    Visitable visit(ExplainNode node) throws StandardException;
    Visitable visit(ExportNode node) throws StandardException;
    Visitable visit(BinaryExportNode node) throws StandardException;
    Visitable visit(BatchOnceNode node) throws StandardException;
    Visitable visit(CreatePinNode node) throws StandardException;
    Visitable visit(DropPinNode node) throws StandardException;
    Visitable visit(ArrayOperatorNode node) throws StandardException;
    Visitable visit(ArrayConstantNode node) throws StandardException;
    Visitable visit(SetSessionPropertyNode node) throws StandardException;
    Visitable visit(ListValueNode node) throws StandardException;
    Visitable visit(GroupingFunctionNode node) throws StandardException;
    Visitable visit(SelfReferenceNode node) throws StandardException;
    Visitable visit(SignalNode node) throws StandardException;
    Visitable visit(SetNode node) throws StandardException;
    Visitable visit(FullOuterJoinNode node) throws StandardException;

}
