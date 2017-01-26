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
 * All such Splice Machine modifications are Copyright 2012 - 2017 Splice Machine, Inc.,
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
    public void setContext(String query, CompilationPhase phase);
    public boolean isPostOrder();
    public boolean stopTraversal();
    public boolean skipChildren(Visitable node);

    public Visitable defaultVisit(Visitable node) throws StandardException;

    // ResultSet nodes
    public Visitable visit(CurrentOfNode node) throws StandardException;
    public Visitable visit(DistinctNode node) throws StandardException;
    public Visitable visit(FromBaseTable node) throws StandardException;
    public Visitable visit(FromSubquery node) throws StandardException;
    public Visitable visit(FromTable node) throws StandardException;
    public Visitable visit(FromVTI node) throws StandardException;
    public Visitable visit(GroupByNode node) throws StandardException;
    public Visitable visit(HalfOuterJoinNode node) throws StandardException;
    public Visitable visit(HashTableNode node) throws StandardException;
    public Visitable visit(IndexToBaseRowNode node) throws StandardException;
    public Visitable visit(IntersectOrExceptNode node) throws StandardException;
    public Visitable visit(JoinNode node) throws StandardException;
    public Visitable visit(MaterializeResultSetNode node) throws StandardException;
    public Visitable visit(NormalizeResultSetNode node) throws StandardException;
    public Visitable visit(OrderByNode node) throws StandardException;
    public Visitable visit(ProjectRestrictNode node) throws StandardException;
    public Visitable visit(RowCountNode node) throws StandardException;
    public Visitable visit(RowResultSetNode node) throws StandardException;
    public Visitable visit(ScrollInsensitiveResultSetNode node) throws StandardException;
    public Visitable visit(SelectNode node) throws StandardException;
    public Visitable visit(UnionNode node) throws StandardException;
    public Visitable visit(WindowResultSetNode node) throws StandardException;

    // Rest
    public Visitable visit(AggregateNode node) throws StandardException;
    public Visitable visit(AllResultColumn node) throws StandardException;
    public Visitable visit(AlterTableNode node) throws StandardException;
    public Visitable visit(AndNode node) throws StandardException;
    public Visitable visit(BaseColumnNode node) throws StandardException;
    public Visitable visit(BetweenOperatorNode node) throws StandardException;
    public Visitable visit(BinaryArithmeticOperatorNode node) throws StandardException;
    public Visitable visit(BinaryComparisonOperatorNode node) throws StandardException;
    public Visitable visit(BinaryListOperatorNode node) throws StandardException;
    public Visitable visit(BinaryOperatorNode node) throws StandardException;
    public Visitable visit(BinaryRelationalOperatorNode node) throws StandardException;
    public Visitable visit(BitConstantNode node) throws StandardException;
    public Visitable visit(BooleanConstantNode node) throws StandardException;
    public Visitable visit(CallStatementNode node) throws StandardException;
    public Visitable visit(CastNode node) throws StandardException;
    public Visitable visit(CharConstantNode node) throws StandardException;
    public Visitable visit(CoalesceFunctionNode node) throws StandardException;
    public Visitable visit(ColumnDefinitionNode node) throws StandardException;
    public Visitable visit(ColumnReference node) throws StandardException;
    public Visitable visit(ConcatenationOperatorNode node) throws StandardException;
    public Visitable visit(ConditionalNode node) throws StandardException;
    public Visitable visit(ConstraintDefinitionNode node) throws StandardException;
    public Visitable visit(CreateAliasNode node) throws StandardException;
    public Visitable visit(CreateIndexNode node) throws StandardException;
    public Visitable visit(CreateRoleNode node) throws StandardException;
    public Visitable visit(CreateSchemaNode node) throws StandardException;
    public Visitable visit(CreateSequenceNode node) throws StandardException;
    public Visitable visit(CreateTableNode node) throws StandardException;
    public Visitable visit(CreateTriggerNode node) throws StandardException;
    public Visitable visit(CreateViewNode node) throws StandardException;
    public Visitable visit(CurrentDatetimeOperatorNode node) throws StandardException;
    public Visitable visit(CurrentRowLocationNode node) throws StandardException;
    public Visitable visit(CursorNode node) throws StandardException;
    public Visitable visit(DB2LengthOperatorNode node) throws StandardException;
    public Visitable visit(DefaultNode node) throws StandardException;
    public Visitable visit(DeleteNode node) throws StandardException;
    public Visitable visit(DenseRankFunctionNode node) throws StandardException;
    public Visitable visit(DropAliasNode node) throws StandardException;
    public Visitable visit(DropIndexNode node) throws StandardException;
    public Visitable visit(DropRoleNode node) throws StandardException;
    public Visitable visit(DropSchemaNode node) throws StandardException;
    public Visitable visit(DropSequenceNode node) throws StandardException;
    public Visitable visit(DropTableNode node) throws StandardException;
    public Visitable visit(DropTriggerNode node) throws StandardException;
    public Visitable visit(DropViewNode node) throws StandardException;
    public Visitable visit(ExecSPSNode node) throws StandardException;
    public Visitable visit(ExtractOperatorNode node) throws StandardException;
    public Visitable visit(FirstLastValueFunctionNode node) throws StandardException;
    public Visitable visit(FKConstraintDefinitionNode node) throws StandardException;
    public Visitable visit(FromList node) throws StandardException;
    public Visitable visit(GenerationClauseNode node) throws StandardException;
    public Visitable visit(GetCurrentConnectionNode node) throws StandardException;
    public Visitable visit(GrantNode node) throws StandardException;
    public Visitable visit(GrantRoleNode node) throws StandardException;
    public Visitable visit(GroupByColumn node) throws StandardException;
    public Visitable visit(GroupByList node) throws StandardException;
    public Visitable visit(InListOperatorNode node) throws StandardException;
    public Visitable visit(InsertNode node) throws StandardException;
    public Visitable visit(IsNode node) throws StandardException;
    public Visitable visit(IsNullNode node) throws StandardException;
    public Visitable visit(JavaToSQLValueNode node) throws StandardException;
    public Visitable visit(LeadLagFunctionNode node) throws StandardException;
    public Visitable visit(LengthOperatorNode node) throws StandardException;
    public Visitable visit(LikeEscapeOperatorNode node) throws StandardException;
    public Visitable visit(LockTableNode node) throws StandardException;
    public Visitable visit(ModifyColumnNode node) throws StandardException;
    public Visitable visit(NewInvocationNode node) throws StandardException;
    public Visitable visit(NextSequenceNode node) throws StandardException;
    public Visitable visit(NonStaticMethodCallNode node) throws StandardException;
    public Visitable visit(NOPStatementNode node) throws StandardException;
    public Visitable visit(NotNode node) throws StandardException;
    public Visitable visit(NumericConstantNode node) throws StandardException;
    public Visitable visit(OrderByColumn node) throws StandardException;
    public Visitable visit(OrderByList node) throws StandardException;
    public Visitable visit(OrderedColumn node) throws StandardException;
    public Visitable visit(OrderedColumnList node) throws StandardException;
    public Visitable visit(OrNode node) throws StandardException;
    public Visitable visit(ParameterNode node) throws StandardException;
    public Visitable visit(Predicate node) throws StandardException;
    public Visitable visit(PredicateList node) throws StandardException;
    public Visitable visit(PrivilegeNode node) throws StandardException;
    public Visitable visit(QueryTreeNode node) throws StandardException;
    public Visitable visit(RankFunctionNode node) throws StandardException;
    public Visitable visit(RenameNode node) throws StandardException;
    public Visitable visit(ResultColumn node) throws StandardException;
    public Visitable visit(ResultColumnList node) throws StandardException;
    public Visitable visit(ResultSetNode node) throws StandardException;
    public Visitable visit(RevokeNode node) throws StandardException;
    public Visitable visit(RevokeRoleNode node) throws StandardException;
    public Visitable visit(RowNumberFunctionNode node) throws StandardException;
    public Visitable visit(SavepointNode node) throws StandardException;
    public Visitable visit(SetRoleNode node) throws StandardException;
    public Visitable visit(SetSchemaNode node) throws StandardException;
    public Visitable visit(SetTransactionIsolationNode node) throws StandardException;
    public Visitable visit(SimpleStringOperatorNode node) throws StandardException;
    public Visitable visit(SpecialFunctionNode node) throws StandardException;
    public Visitable visit(SQLBooleanConstantNode node) throws StandardException;
    public Visitable visit(SQLToJavaValueNode node) throws StandardException;
    public Visitable visit(StatementNode node) throws StandardException;
    public Visitable visit(StaticClassFieldReferenceNode node) throws StandardException;
    public Visitable visit(StaticMethodCallNode node) throws StandardException;
    public Visitable visit(SubqueryList node) throws StandardException;
    public Visitable visit(SubqueryNode node) throws StandardException;
    public Visitable visit(TableElementList node) throws StandardException;
    public Visitable visit(TableElementNode node) throws StandardException;
    public Visitable visit(TableName node) throws StandardException;
    public Visitable visit(BasicPrivilegesNode node) throws StandardException;
    public Visitable visit(TernaryOperatorNode node) throws StandardException;
    public Visitable visit(TestConstraintNode node) throws StandardException;
    public Visitable visit(TimestampOperatorNode node) throws StandardException;
    public Visitable visit(TruncateOperatorNode node) throws StandardException;
    public Visitable visit(UnaryArithmeticOperatorNode node) throws StandardException;
    public Visitable visit(UnaryComparisonOperatorNode node) throws StandardException;
    public Visitable visit(UnaryDateTimestampOperatorNode node) throws StandardException;
    public Visitable visit(UnaryLogicalOperatorNode node) throws StandardException;
    public Visitable visit(UnaryOperatorNode node) throws StandardException;
    public Visitable visit(UntypedNullConstantNode node) throws StandardException;
    public Visitable visit(UpdateNode node) throws StandardException;
    public Visitable visit(UserTypeConstantNode node) throws StandardException;
    public Visitable visit(ValueNode node) throws StandardException;
    public Visitable visit(ValueNodeList node) throws StandardException;
    public Visitable visit(VarbitConstantNode node) throws StandardException;
    public Visitable visit(VirtualColumnNode node) throws StandardException;
    public Visitable visit(WindowDefinitionNode node) throws StandardException;
    public Visitable visit(WindowFunctionNode node) throws StandardException;
    public Visitable visit(WindowList node) throws StandardException;
    public Visitable visit(WindowNode node) throws StandardException;
    public Visitable visit(WindowReferenceNode node) throws StandardException;
    public Visitable visit(WrappedAggregateFunctionNode node) throws StandardException;
    public Visitable visit(XMLConstantNode node) throws StandardException;
    public Visitable visit(ExplainNode node) throws StandardException;
    public Visitable visit(ExportNode node) throws StandardException;
    public Visitable visit(BatchOnceNode node) throws StandardException;
    public Visitable visit(CreatePinNode node) throws StandardException;
    public Visitable visit(DropPinNode node) throws StandardException;

}
