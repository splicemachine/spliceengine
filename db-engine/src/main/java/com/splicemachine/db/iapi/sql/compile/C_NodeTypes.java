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

package com.splicemachine.db.iapi.sql.compile;

/**
 * The purpose of this interface is to hold the constant definitions
 * of the different node type identifiers, for use with NodeFactory.
 * The reason this class exists is that it is not shipped with the
 * product, so it saves footprint to have all these constant definitions
 * here instead of in NodeFactory.
 */
public interface C_NodeTypes
{
	/** Node types, for use with getNode methods */
	int TEST_CONSTRAINT_NODE = 1;
	int CURRENT_ROW_LOCATION_NODE = 2;
	int GROUP_BY_LIST = 3;
    int CURRENT_ISOLATION_NODE = 4;
	int IDENTITY_VAL_NODE = 5;
	int CURRENT_SCHEMA_NODE = 6;
	int ORDER_BY_LIST = 7;
	int PREDICATE_LIST = 8;
	int RESULT_COLUMN_LIST = 9;
	// 10 available
	int SUBQUERY_LIST = 11;
	int TABLE_ELEMENT_LIST = 12;
	int UNTYPED_NULL_CONSTANT_NODE = 13;
	int TABLE_ELEMENT_NODE = 14;
	int VALUE_NODE_LIST = 15;
	int ALL_RESULT_COLUMN = 16;
	// 17 is available
	int GET_CURRENT_CONNECTION_NODE = 18;
	int NOP_STATEMENT_NODE = 19;
	int DB2_LENGTH_OPERATOR_NODE = 20;
	int SET_TRANSACTION_ISOLATION_NODE = 21;
	// 22 is available
	int CHAR_LENGTH_OPERATOR_NODE = 23;
	int IS_NOT_NULL_NODE = 24;
	int IS_NULL_NODE = 25;
	int NOT_NODE = 26;
	// 27 is available
	int SQL_TO_JAVA_VALUE_NODE = 28;
	int UNARY_MINUS_OPERATOR_NODE = 29;
	int UNARY_PLUS_OPERATOR_NODE = 30;
	int SQL_BOOLEAN_CONSTANT_NODE = 31;
	int UNARY_DATE_TIMESTAMP_OPERATOR_NODE = 32;
	int TIMESTAMP_OPERATOR_NODE = 33;
	int TABLE_NAME = 34;
	int GROUP_BY_COLUMN = 35;
	int JAVA_TO_SQL_VALUE_NODE = 36;
	int FROM_LIST = 37;
	int BOOLEAN_CONSTANT_NODE = 38;
	int AND_NODE = 39;
	int BINARY_DIVIDE_OPERATOR_NODE = 40;
	int BINARY_EQUALS_OPERATOR_NODE = 41;
	int BINARY_GREATER_EQUALS_OPERATOR_NODE = 42;
	int BINARY_GREATER_THAN_OPERATOR_NODE = 43;
	int BINARY_LESS_EQUALS_OPERATOR_NODE = 44;
	int BINARY_LESS_THAN_OPERATOR_NODE = 45;
	int BINARY_MINUS_OPERATOR_NODE = 46;
	int BINARY_NOT_EQUALS_OPERATOR_NODE = 47;
	int BINARY_PLUS_OPERATOR_NODE = 48;
	int BINARY_TIMES_OPERATOR_NODE = 49;
	int CONCATENATION_OPERATOR_NODE = 50;
	int LIKE_OPERATOR_NODE = 51;
	int OR_NODE = 52;
	int BETWEEN_OPERATOR_NODE = 53;
	int CONDITIONAL_NODE = 54;
	int IN_LIST_OPERATOR_NODE = 55;
	int NOT_BETWEEN_OPERATOR_NODE = 56;
	int NOT_IN_LIST_OPERATOR_NODE = 57;
	int BIT_CONSTANT_NODE = 58;
	int VARBIT_CONSTANT_NODE = 59;
	int CAST_NODE = 60;
	int CHAR_CONSTANT_NODE = 61;
	int COLUMN_REFERENCE = 62;
	int DROP_INDEX_NODE = 63;
	// 64 available;
	int DROP_TRIGGER_NODE = 65;
	// 66 available;
	int DECIMAL_CONSTANT_NODE = 67;
	int DOUBLE_CONSTANT_NODE = 68;
	int FLOAT_CONSTANT_NODE = 69;
	int INT_CONSTANT_NODE = 70;
	int LONGINT_CONSTANT_NODE = 71;
	int LONGVARBIT_CONSTANT_NODE = 72;
	int LONGVARCHAR_CONSTANT_NODE = 73;
	int SMALLINT_CONSTANT_NODE = 74;
	int TINYINT_CONSTANT_NODE = 75;
	int USERTYPE_CONSTANT_NODE = 76;
	int VARCHAR_CONSTANT_NODE = 77;
	int PREDICATE = 78;
	// 79 available
	int RESULT_COLUMN = 80;
	int SET_SCHEMA_NODE = 81;
	int UPDATE_COLUMN = 82;
	int SIMPLE_STRING_OPERATOR_NODE = 83;
	int STATIC_CLASS_FIELD_REFERENCE_NODE = 84;
	int STATIC_METHOD_CALL_NODE = 85;
	int REVOKE_NODE = 86;
	int EXTRACT_OPERATOR_NODE = 87;
	int PARAMETER_NODE = 88;
	int GRANT_NODE = 89;
	int DROP_SCHEMA_NODE = 90;
	int DROP_TABLE_NODE = 91;
	int DROP_VIEW_NODE = 92;
	int SUBQUERY_NODE = 93;
	int BASE_COLUMN_NODE = 94;
	int CALL_STATEMENT_NODE = 95;
	int MODIFY_COLUMN_DEFAULT_NODE = 97;
	int NON_STATIC_METHOD_CALL_NODE = 98;
	int CURRENT_OF_NODE = 99;
	int DEFAULT_NODE = 100;
	int DELETE_NODE = 101;
	int UPDATE_NODE = 102;
	int PRIVILEGE_NODE = 103;
	int ORDER_BY_COLUMN = 104;
	int ROW_RESULT_SET_NODE = 105;
	int TABLE_PRIVILEGES_NODE = 106;
	int VIRTUAL_COLUMN_NODE = 107;
	int CURRENT_DATETIME_OPERATOR_NODE = 108;
	int CURRENT_USER_NODE = 109; // special function CURRENT_USER
	int USER_NODE = 110; // // special function USER
	int IS_NODE = 111;
	int LOCK_TABLE_NODE = 112;
	int DROP_COLUMN_NODE = 113;
	int ALTER_TABLE_NODE = 114;
	int AGGREGATE_NODE = 115;
	int COLUMN_DEFINITION_NODE = 116;
	int SCHEMA_PRIVILEGES_NODE = 117;
	int EXEC_SPS_NODE = 118;
	int FK_CONSTRAINT_DEFINITION_NODE = 119;
	int FROM_VTI = 120;
	int MATERIALIZE_RESULT_SET_NODE = 121;
	int NORMALIZE_RESULT_SET_NODE = 122;
	int SCROLL_INSENSITIVE_RESULT_SET_NODE = 123;
	int DISTINCT_NODE = 124;
	int SESSION_USER_NODE = 125; // // special function SESSION_USER
	int SYSTEM_USER_NODE = 126; // // special function SYSTEM_USER
	int TRIM_OPERATOR_NODE = 127;
	int LEFT_OPERATOR_NODE = 128;
	int SELECT_NODE = 129;
	int CREATE_VIEW_NODE = 130;
	int CONSTRAINT_DEFINITION_NODE = 131;
	// 132 available;
	int NEW_INVOCATION_NODE = 133;
	int CREATE_SCHEMA_NODE = 134;
	int FROM_BASE_TABLE = 135;
	int FROM_SUBQUERY = 136;
	int GROUP_BY_NODE = 137;
	int INSERT_NODE = 138;
	int JOIN_NODE = 139;
	int ORDER_BY_NODE = 140;
	int CREATE_TABLE_NODE = 141;
	int UNION_NODE = 142;
	int CREATE_TRIGGER_NODE = 143;
	int HALF_OUTER_JOIN_NODE = 144;
// UNUSED	static final int CREATE_SPS_NODE = 145;
int CREATE_INDEX_NODE = 146;
	int CURSOR_NODE = 147;
	int HASH_TABLE_NODE = 148;
	int INDEX_TO_BASE_ROW_NODE = 149;
	int CREATE_ALIAS_NODE = 150;
	int PROJECT_RESTRICT_NODE = 151;
	// UNUSED static final int BOOLEAN_TRUE_NODE = 152;
	int RIGHT_OPERATOR_NODE = 153;
	int SUBSTRING_OPERATOR_NODE = 154;
	// UNUSED static final int BOOLEAN_NODE = 155;
	int DROP_ALIAS_NODE = 156;
    int INTERSECT_OR_EXCEPT_NODE = 157;
	int REPLACE_OPERATOR_NODE = 158;
	// 159 - 183 available
	int TIMESTAMP_ADD_FN_NODE = 184;
    int TIMESTAMP_DIFF_FN_NODE = 185;
	int MODIFY_COLUMN_TYPE_NODE = 186;
	int MODIFY_COLUMN_CONSTRAINT_NODE = 187;
    int ABSOLUTE_OPERATOR_NODE = 188;
    int SQRT_OPERATOR_NODE = 189;
    int LOCATE_FUNCTION_NODE = 190;
  //for rename table/column/index
  int RENAME_NODE = 191;

	int COALESCE_FUNCTION_NODE = 192;

	int MODIFY_COLUMN_CONSTRAINT_NOT_NULL_NODE = 193;

	int MOD_OPERATOR_NODE = 194;
    // LOB
	int BLOB_CONSTANT_NODE = 195;
    int CLOB_CONSTANT_NODE = 196;
    //static final int NCLOB_CONSTANT_NODE = 197;
    // for SAVEPOINT sql
	int SAVEPOINT_NODE = 198;

    // XML
	int XML_CONSTANT_NODE = 199;
    int XML_PARSE_OPERATOR_NODE = 200;
    int XML_SERIALIZE_OPERATOR_NODE = 201;
    int XML_EXISTS_OPERATOR_NODE = 202;
    int XML_QUERY_OPERATOR_NODE = 203;

    // Roles
	int CURRENT_ROLE_NODE = 210;
    int CREATE_ROLE_NODE = 211;
    int SET_ROLE_NODE = 212;
    int SET_ROLE_DYNAMIC = 213;
    int DROP_ROLE_NODE = 214;
    int GRANT_ROLE_NODE = 215;
    int REVOKE_ROLE_NODE = 216;

    // generated columns
	int GENERATION_CLAUSE_NODE = 222;

	// OFFSET, FETCH FIRST, TOPn node
	int ROW_COUNT_NODE = 223;

    // sequences
	int CREATE_SEQUENCE_NODE = 224;
    int DROP_SEQUENCE_NODE = 225;
    int NEXT_SEQUENCE_NODE = 231;

	// Windowing
	int WRAPPED_AGGREGATE_FUNCTION_NODE = 226;
	int ROW_NUMBER_FUNCTION_NODE = 227;
	int WINDOW_DEFINITION_NODE = 228;
	int WINDOW_REFERENCE_NODE = 229;
    int WINDOW_RESULTSET_NODE = 230;
    int DENSERANK_FUNCTION_NODE = 232;
    int RANK_FUNCTION_NODE = 233;
    int EXPLAIN_NODE = 234;

    int EXPORT_NODE = 250;
    int TRUNC_NODE = 251;
    int BATCH_ONCE_NODE = 252;
    int FIRST_LAST_VALUE_FUNCTION_NODE = 253;
    int LEAD_LAG_FUNCTION_NODE = 254;
	int CREATE_PIN_NODE = 255;
	int DROP_PIN_NODE = 256;
	int ARRAY_OPERATOR_NODE = 257;
	int ARRAY_CONSTANT_NODE = 258;
	int SET_SESSION_PROPERTY_NODE = 259;
	int CURRENT_SESSION_PROPERTY_NODE = 260;
	int BINARY_EXPORT_NODE = 261;
	int LIST_VALUE_NODE = 262;
	int GROUPING_FUNCTION_NODE = 263;
	int REPEAT_OPERATOR_NODE = 264;
	int SIMPLE_LOCALE_STRING_OPERATOR_NODE = 265;
	int GROUP_USER_NODE = 266;
	int SELF_REFERENCE_NODE = 267;
	int DIGITS_OPERATOR_NODE = 268;

	// Final value in set, keep up to date!
	int FINAL_VALUE = DIGITS_OPERATOR_NODE;

    /**
     * Extensions to this interface can use nodetypes > MAX_NODE_TYPE with out fear of collision
     * with C_NodeTypes
     */
	int MAX_NODE_TYPE = 999;
}
