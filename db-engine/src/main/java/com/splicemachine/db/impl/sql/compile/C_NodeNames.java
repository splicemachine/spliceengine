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

package com.splicemachine.db.impl.sql.compile;

/**
 * This is the set of constants used to identify the classes
 * that are used in NodeFactoryImpl.
 *
 * This class is not shipped. The names are used in
 * NodeFactoryImpl, mapped from int NodeTypes and used in
 * Class.forName calls.
 *
 * WARNING: WHEN ADDING NODE TYPES HERE, YOU MUST ALSO ADD
 * THEM TO tools/jar/DBMSnodes.properties
 *
 */

public interface C_NodeNames
{

	// The names are in alphabetic order.
	//
    // WARNING: WHEN ADDING NODE TYPES HERE, YOU MUST ALSO ADD
    // THEM TO tools/jar/DBMSnodes.properties

	String AGGREGATE_NODE_NAME = "com.splicemachine.db.impl.sql.compile.AggregateNode";
	String ALL_RESULT_COLUMN_NAME = "com.splicemachine.db.impl.sql.compile.AllResultColumn";

	String ALTER_TABLE_NODE_NAME = "com.splicemachine.db.impl.sql.compile.AlterTableNode";

	String AND_NODE_NAME = "com.splicemachine.db.impl.sql.compile.AndNode";

	String BASE_COLUMN_NODE_NAME = "com.splicemachine.db.impl.sql.compile.BaseColumnNode";

	String BETWEEN_OPERATOR_NODE_NAME = "com.splicemachine.db.impl.sql.compile.BetweenOperatorNode";

	String BINARY_ARITHMETIC_OPERATOR_NODE_NAME = "com.splicemachine.db.impl.sql.compile.BinaryArithmeticOperatorNode";

	String BINARY_OPERATOR_NODE_NAME = "com.splicemachine.db.impl.sql.compile.BinaryOperatorNode";

	String BINARY_RELATIONAL_OPERATOR_NODE_NAME = "com.splicemachine.db.impl.sql.compile.BinaryRelationalOperatorNode";

	String BIT_CONSTANT_NODE_NAME = "com.splicemachine.db.impl.sql.compile.BitConstantNode";

	String BOOLEAN_CONSTANT_NODE_NAME = "com.splicemachine.db.impl.sql.compile.BooleanConstantNode";
	
	String LIST_VALUE_NODE_NAME = "com.splicemachine.db.impl.sql.compile.ListValueNode";

	String CALL_STATEMENT_NODE_NAME = "com.splicemachine.db.impl.sql.compile.CallStatementNode";

	String CAST_NODE_NAME = "com.splicemachine.db.impl.sql.compile.CastNode";

	String CHAR_CONSTANT_NODE_NAME = "com.splicemachine.db.impl.sql.compile.CharConstantNode";

	String COALESCE_FUNCTION_NODE_NAME = "com.splicemachine.db.impl.sql.compile.CoalesceFunctionNode";

	String COLUMN_DEFINITION_NODE_NAME = "com.splicemachine.db.impl.sql.compile.ColumnDefinitionNode";

	String COLUMN_REFERENCE_NAME = "com.splicemachine.db.impl.sql.compile.ColumnReference";

	String CONCATENATION_OPERATOR_NODE_NAME = "com.splicemachine.db.impl.sql.compile.ConcatenationOperatorNode";

	String CONDITIONAL_NODE_NAME = "com.splicemachine.db.impl.sql.compile.ConditionalNode";

	String CONSTRAINT_DEFINITION_NODE_NAME = "com.splicemachine.db.impl.sql.compile.ConstraintDefinitionNode";

	String CREATE_ALIAS_NODE_NAME = "com.splicemachine.db.impl.sql.compile.CreateAliasNode";

	String CREATE_ROLE_NODE_NAME =
		"com.splicemachine.db.impl.sql.compile.CreateRoleNode";

	String CREATE_INDEX_NODE_NAME = "com.splicemachine.db.impl.sql.compile.CreateIndexNode";

	String CREATE_SCHEMA_NODE_NAME = "com.splicemachine.db.impl.sql.compile.CreateSchemaNode";

    String CREATE_SEQUENCE_NODE_NAME = "com.splicemachine.db.impl.sql.compile.CreateSequenceNode";

    String CREATE_TABLE_NODE_NAME = "com.splicemachine.db.impl.sql.compile.CreateTableNode";

	String CREATE_TRIGGER_NODE_NAME = "com.splicemachine.db.impl.sql.compile.CreateTriggerNode";

	String CREATE_VIEW_NODE_NAME = "com.splicemachine.db.impl.sql.compile.CreateViewNode";

	String CURRENT_DATETIME_OPERATOR_NODE_NAME = "com.splicemachine.db.impl.sql.compile.CurrentDatetimeOperatorNode";

	String CURRENT_OF_NODE_NAME = "com.splicemachine.db.impl.sql.compile.CurrentOfNode";

	String CURRENT_ROW_LOCATION_NODE_NAME = "com.splicemachine.db.impl.sql.compile.CurrentRowLocationNode";

	String SPECIAL_FUNCTION_NODE_NAME = "com.splicemachine.db.impl.sql.compile.SpecialFunctionNode";

	String CURSOR_NODE_NAME = "com.splicemachine.db.impl.sql.compile.CursorNode";

	String DB2_LENGTH_OPERATOR_NODE_NAME = "com.splicemachine.db.impl.sql.compile.DB2LengthOperatorNode";

	String DML_MOD_STATEMENT_NODE_NAME = "com.splicemachine.db.impl.sql.compile.DMLModStatementNode";

	String DEFAULT_NODE_NAME = "com.splicemachine.db.impl.sql.compile.DefaultNode";

	String DELETE_NODE_NAME = "com.splicemachine.db.impl.sql.compile.DeleteNode";

	String DISTINCT_NODE_NAME = "com.splicemachine.db.impl.sql.compile.DistinctNode";

	String DROP_ALIAS_NODE_NAME = "com.splicemachine.db.impl.sql.compile.DropAliasNode";

	String DROP_INDEX_NODE_NAME = "com.splicemachine.db.impl.sql.compile.DropIndexNode";

	String DROP_ROLE_NODE_NAME =
		"com.splicemachine.db.impl.sql.compile.DropRoleNode";

	String DROP_SCHEMA_NODE_NAME = "com.splicemachine.db.impl.sql.compile.DropSchemaNode";

    String DROP_SEQUENCE_NODE_NAME = "com.splicemachine.db.impl.sql.compile.DropSequenceNode";

    String DROP_TABLE_NODE_NAME = "com.splicemachine.db.impl.sql.compile.DropTableNode";

	String DROP_TRIGGER_NODE_NAME = "com.splicemachine.db.impl.sql.compile.DropTriggerNode";

	String DROP_VIEW_NODE_NAME = "com.splicemachine.db.impl.sql.compile.DropViewNode";

	String EXEC_SPS_NODE_NAME = "com.splicemachine.db.impl.sql.compile.ExecSPSNode";

	String EXTRACT_OPERATOR_NODE_NAME = "com.splicemachine.db.impl.sql.compile.ExtractOperatorNode";

	String ARRAY_OPERATOR_NODE_NAME = "com.splicemachine.db.impl.sql.compile.ArrayOperatorNode";

	String ARRAY_CONSTANT_NODE_NAME = "com.splicemachine.db.impl.sql.compile.ArrayConstantNode";

	String FK_CONSTRAINT_DEFINITION_NODE_NAME = "com.splicemachine.db.impl.sql.compile.FKConstraintDefinitionNode";

	String FROM_BASE_TABLE_NAME = "com.splicemachine.db.impl.sql.compile.FromBaseTable";

	String FROM_LIST_NAME = "com.splicemachine.db.impl.sql.compile.FromList";

	String FROM_SUBQUERY_NAME = "com.splicemachine.db.impl.sql.compile.FromSubquery";

	String FROM_VTI_NAME = "com.splicemachine.db.impl.sql.compile.FromVTI";

	String GENERATION_CLAUSE_NODE_NAME = "com.splicemachine.db.impl.sql.compile.GenerationClauseNode";

	String GET_CURRENT_CONNECTION_NODE_NAME = "com.splicemachine.db.impl.sql.compile.GetCurrentConnectionNode";

	String GRANT_NODE_NAME = "com.splicemachine.db.impl.sql.compile.GrantNode";

    String GRANT_ROLE_NODE_NAME =
		"com.splicemachine.db.impl.sql.compile.GrantRoleNode";
    
	String GROUP_BY_COLUMN_NAME = "com.splicemachine.db.impl.sql.compile.GroupByColumn";

	String GROUP_BY_LIST_NAME = "com.splicemachine.db.impl.sql.compile.GroupByList";

	String GROUP_BY_NODE_NAME = "com.splicemachine.db.impl.sql.compile.GroupByNode";

	String HALF_OUTER_JOIN_NODE_NAME = "com.splicemachine.db.impl.sql.compile.HalfOuterJoinNode";

	String HASH_TABLE_NODE_NAME = "com.splicemachine.db.impl.sql.compile.HashTableNode";

	String IN_LIST_OPERATOR_NODE_NAME = "com.splicemachine.db.impl.sql.compile.InListOperatorNode";

	String INDEX_TO_BASE_ROW_NODE_NAME = "com.splicemachine.db.impl.sql.compile.IndexToBaseRowNode";

	String INSERT_NODE_NAME = "com.splicemachine.db.impl.sql.compile.InsertNode";

	String IS_NODE_NAME = "com.splicemachine.db.impl.sql.compile.IsNode";

	String IS_NULL_NODE_NAME = "com.splicemachine.db.impl.sql.compile.IsNullNode";

	String JAVA_TO_SQL_VALUE_NODE_NAME = "com.splicemachine.db.impl.sql.compile.JavaToSQLValueNode";

	String JOIN_NODE_NAME = "com.splicemachine.db.impl.sql.compile.JoinNode";

	String LENGTH_OPERATOR_NODE_NAME = "com.splicemachine.db.impl.sql.compile.LengthOperatorNode";

	String LIKE_OPERATOR_NODE_NAME = "com.splicemachine.db.impl.sql.compile.LikeEscapeOperatorNode";

	String LOCK_TABLE_NODE_NAME = "com.splicemachine.db.impl.sql.compile.LockTableNode";

	String MATERIALIZE_RESULT_SET_NODE_NAME = "com.splicemachine.db.impl.sql.compile.MaterializeResultSetNode";

	String MODIFY_COLUMN_NODE_NAME = "com.splicemachine.db.impl.sql.compile.ModifyColumnNode";

	String NOP_STATEMENT_NODE_NAME = "com.splicemachine.db.impl.sql.compile.NOPStatementNode";

	String NEW_INVOCATION_NODE_NAME = "com.splicemachine.db.impl.sql.compile.NewInvocationNode";

    String NEXT_SEQUENCE_NODE_NAME = "com.splicemachine.db.impl.sql.compile.NextSequenceNode";

    String NON_STATIC_METHOD_CALL_NODE_NAME = "com.splicemachine.db.impl.sql.compile.NonStaticMethodCallNode";

	String NORMALIZE_RESULT_SET_NODE_NAME = "com.splicemachine.db.impl.sql.compile.NormalizeResultSetNode";

	String NOT_NODE_NAME = "com.splicemachine.db.impl.sql.compile.NotNode";

	String NUMERIC_CONSTANT_NODE_NAME = "com.splicemachine.db.impl.sql.compile.NumericConstantNode";

	String OR_NODE_NAME = "com.splicemachine.db.impl.sql.compile.OrNode";

	String ORDER_BY_COLUMN_NAME = "com.splicemachine.db.impl.sql.compile.OrderByColumn";

	String ORDER_BY_LIST_NAME = "com.splicemachine.db.impl.sql.compile.OrderByList";

	String ORDER_BY_NODE_NAME = "com.splicemachine.db.impl.sql.compile.OrderByNode";

	String PARAMETER_NODE_NAME = "com.splicemachine.db.impl.sql.compile.ParameterNode";

	String PREDICATE_NAME = "com.splicemachine.db.impl.sql.compile.Predicate";

	String PREDICATE_LIST_NAME = "com.splicemachine.db.impl.sql.compile.PredicateList";

	String PRIVILEGE_NAME = "com.splicemachine.db.impl.sql.compile.PrivilegeNode";

	String PROJECT_RESTRICT_NODE_NAME = "com.splicemachine.db.impl.sql.compile.ProjectRestrictNode";

	String RENAME_NODE_NAME = "com.splicemachine.db.impl.sql.compile.RenameNode";

	String RESULT_COLUMN_NAME = "com.splicemachine.db.impl.sql.compile.ResultColumn";

	String RESULT_COLUMN_LIST_NAME = "com.splicemachine.db.impl.sql.compile.ResultColumnList";

	String REVOKE_NODE_NAME = "com.splicemachine.db.impl.sql.compile.RevokeNode";

	String REVOKE_ROLE_NODE_NAME =
		"com.splicemachine.db.impl.sql.compile.RevokeRoleNode";

	String ROW_RESULT_SET_NODE_NAME = "com.splicemachine.db.impl.sql.compile.RowResultSetNode";

	String SQL_BOOLEAN_CONSTANT_NODE_NAME = "com.splicemachine.db.impl.sql.compile.SQLBooleanConstantNode";

	String SQL_TO_JAVA_VALUE_NODE_NAME = "com.splicemachine.db.impl.sql.compile.SQLToJavaValueNode";

	String SCROLL_INSENSITIVE_RESULT_SET_NODE_NAME = "com.splicemachine.db.impl.sql.compile.ScrollInsensitiveResultSetNode";

	String SELECT_NODE_NAME = "com.splicemachine.db.impl.sql.compile.SelectNode";

	String SET_ROLE_NODE_NAME =
		"com.splicemachine.db.impl.sql.compile.SetRoleNode";

	String SET_SCHEMA_NODE_NAME = "com.splicemachine.db.impl.sql.compile.SetSchemaNode";

	String SET_TRANSACTION_ISOLATION_NODE_NAME = "com.splicemachine.db.impl.sql.compile.SetTransactionIsolationNode";

	String SIMPLE_STRING_OPERATOR_NODE_NAME = "com.splicemachine.db.impl.sql.compile.SimpleStringOperatorNode";

	String SIMPLE_LOCALE_STRING_OPERATOR_NODE_NAME = "com.splicemachine.db.impl.sql.compile.SimpleLocaleStringOperatorNode";

	String STATIC_CLASS_FIELD_REFERENCE_NODE_NAME = "com.splicemachine.db.impl.sql.compile.StaticClassFieldReferenceNode";

	String STATIC_METHOD_CALL_NODE_NAME = "com.splicemachine.db.impl.sql.compile.StaticMethodCallNode";

	String SUBQUERY_LIST_NAME = "com.splicemachine.db.impl.sql.compile.SubqueryList";

	String SUBQUERY_NODE_NAME = "com.splicemachine.db.impl.sql.compile.SubqueryNode";

	String TABLE_ELEMENT_LIST_NAME = "com.splicemachine.db.impl.sql.compile.TableElementList";

	String TABLE_ELEMENT_NODE_NAME = "com.splicemachine.db.impl.sql.compile.TableElementNode";

	String TABLE_NAME_NAME = "com.splicemachine.db.impl.sql.compile.TableName";

	String TABLE_PRIVILEGES_NAME = "com.splicemachine.db.impl.sql.compile.BasicPrivilegesNode";

	String TERNARY_OPERATOR_NODE_NAME = "com.splicemachine.db.impl.sql.compile.TernaryOperatorNode";

	String TEST_CONSTRAINT_NODE_NAME = "com.splicemachine.db.impl.sql.compile.TestConstraintNode";

	String TIMESTAMP_OPERATOR_NODE_NAME = "com.splicemachine.db.impl.sql.compile.TimestampOperatorNode";

	String UNARY_ARITHMETIC_OPERATOR_NODE_NAME = "com.splicemachine.db.impl.sql.compile.UnaryArithmeticOperatorNode";

	String UNARY_DATE_TIMESTAMP_OPERATOR_NODE_NAME
    = "com.splicemachine.db.impl.sql.compile.UnaryDateTimestampOperatorNode";

	String UNARY_OPERATOR_NODE_NAME = "com.splicemachine.db.impl.sql.compile.UnaryOperatorNode";

	String UNION_NODE_NAME = "com.splicemachine.db.impl.sql.compile.UnionNode";

	String INTERSECT_OR_EXCEPT_NODE_NAME = "com.splicemachine.db.impl.sql.compile.IntersectOrExceptNode";

	String UNTYPED_NULL_CONSTANT_NODE_NAME = "com.splicemachine.db.impl.sql.compile.UntypedNullConstantNode";

	String UPDATE_NODE_NAME = "com.splicemachine.db.impl.sql.compile.UpdateNode";

	String USERTYPE_CONSTANT_NODE_NAME = "com.splicemachine.db.impl.sql.compile.UserTypeConstantNode";

	String VALUE_NODE_LIST_NAME = "com.splicemachine.db.impl.sql.compile.ValueNodeList";

	String VARBIT_CONSTANT_NODE_NAME = "com.splicemachine.db.impl.sql.compile.VarbitConstantNode";

	String VIRTUAL_COLUMN_NODE_NAME = "com.splicemachine.db.impl.sql.compile.VirtualColumnNode";

	String SAVEPOINT_NODE_NAME = "com.splicemachine.db.impl.sql.compile.SavepointNode";

	String XML_CONSTANT_NODE_NAME = "com.splicemachine.db.impl.sql.compile.XMLConstantNode";
	String WRAPPED_AGGREGATE_FUNCTION_NODE_NAME = "com.splicemachine.db.impl.sql.compile.WrappedAggregateFunctionNode";
	String ROW_NUMBER_FUNCTION_NAME = "com.splicemachine.db.impl.sql.compile.RowNumberFunctionNode";
	String RANK_FUNCTION_NAME = "com.splicemachine.db.impl.sql.compile.RankFunctionNode";
	String DENSE_RANK_FUNCTION_NAME = "com.splicemachine.db.impl.sql.compile.DenseRankFunctionNode";
	String FIRST_LAST_VALUE_FUNCTION_NAME = "com.splicemachine.db.impl.sql.compile.FirstLastValueFunctionNode";
	String LEAD_LAG_FUNCTION_NAME = "com.splicemachine.db.impl.sql.compile.LeadLagFunctionNode";
	String WINDOW_DEFINITION_NAME = "com.splicemachine.db.impl.sql.compile.WindowDefinitionNode";
	String WINDOW_REFERENCE_NAME = "com.splicemachine.db.impl.sql.compile.WindowReferenceNode";
	String WINDOW_RESULTSET_NODE_NAME = "com.splicemachine.db.impl.sql.compile.WindowResultSetNode";

	String ROW_COUNT_NODE_NAME = "com.splicemachine.db.impl.sql.compile.RowCountNode";

    String EXPLAIN_NODE_NAME = "com.splicemachine.db.impl.sql.compile.ExplainNode";

	String EXPORT_NODE_NAME = "com.splicemachine.db.impl.sql.compile.ExportNode";
	
	String BINARY_EXPORT_NODE_NAME = "com.splicemachine.db.impl.sql.compile.BinaryExportNode";

    String TRUNC_NODE_NAME = "com.splicemachine.db.impl.sql.compile.TruncateOperatorNode";

    String BATCH_ONCE_NODE_NAME = "com.splicemachine.db.impl.sql.compile.BatchOnceNode";

	String CREATE_PIN_NODE_NAME = "com.splicemachine.db.impl.sql.compile.CreatePinNode";

	String DROP_PIN_NODE_NAME = "com.splicemachine.db.impl.sql.compile.DropPinNode";

	String SET_SESSION_PROPERTY_NAME = "com.splicemachine.db.impl.sql.compile.SetSessionPropertyNode";

	String GROUPING_FUNCTION_NODE_NAME = "com.splicemachine.db.impl.sql.compile.GroupingFunctionNode";

	String SELF_REFERENCE_NODE_NAME = "com.splicemachine.db.impl.sql.compile.SelfReferenceNode";

	String SIGNAL_NAME = "com.splicemachine.db.impl.sql.compile.SignalNode";

	String SET_NAME = "com.splicemachine.db.impl.sql.compile.SetNode";

	String FULL_OUTER_JOIN_NODE_NAME = "com.splicemachine.db.impl.sql.compile.FullOuterJoinNode";

	// WARNING: WHEN ADDING NODE TYPES HERE, YOU MUST ALSO ADD
    // THEM TO tools/jar/DBMSnodes.properties

}
