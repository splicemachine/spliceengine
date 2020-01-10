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
 * All such Splice Machine modifications are Copyright 2012 - 2020 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */

package com.splicemachine.db.iapi.sql.compile;

import com.splicemachine.db.iapi.services.loader.ClassFactory;

import com.splicemachine.db.iapi.services.compiler.MethodBuilder;
import com.splicemachine.db.iapi.services.compiler.LocalField;

import com.splicemachine.db.iapi.types.DataTypeDescriptor;
import com.splicemachine.db.iapi.types.TypeId;

import com.splicemachine.db.iapi.error.StandardException;

import com.splicemachine.db.iapi.reference.Limits;

/**
 * This interface defines methods associated with a TypeId that are used
 * by the compiler.
 */

public interface TypeCompiler
{
	/**
	 * Various fixed numbers related to datatypes.
	 */
	// Need to leave space for '-'
	int LONGINT_MAXWIDTH_AS_CHAR	= 20;

	// Need to leave space for '-'
	int INT_MAXWIDTH_AS_CHAR	= 11;

	// Need to leave space for '-'
	int SMALLINT_MAXWIDTH_AS_CHAR	= 6;

	// Need to leave space for '-'
	int TINYINT_MAXWIDTH_AS_CHAR	= 4;

	// Need to leave space for '-' and decimal point
	int DOUBLE_MAXWIDTH_AS_CHAR		= 54;

	// Need to leave space for '-' and decimal point
	int REAL_MAXWIDTH_AS_CHAR	= 25;

	int DEFAULT_DECIMAL_PRECISION	= Limits.DB2_DEFAULT_DECIMAL_PRECISION;
	int DEFAULT_DECIMAL_SCALE 		= Limits.DB2_DEFAULT_DECIMAL_SCALE;
	int MAX_DECIMAL_PRECISION_SCALE = Limits.DB2_MAX_DECIMAL_PRECISION_SCALE;

	int BOOLEAN_MAXWIDTH_AS_CHAR	= 5;

	String PLUS_OP 		= "+";
	String DIVIDE_OP	= "/";
	String MINUS_OP 	= "-";
	String TIMES_OP 	= "*";
	String SUM_OP 		= "sum";
	String AVG_OP 		= "avg";
	String MOD_OP		= "mod";

	/**
	 * Type resolution methods on binary operators
	 *
	 * @param leftType	The type of the left parameter
	 * @param rightType	The type of the right parameter
	 * @param operator	The name of the operator (e.g. "+").
	 *
	 * @return	The type of the result
	 *
	 * @exception StandardException	Thrown on error
	 */

	DataTypeDescriptor	resolveArithmeticOperation(
							DataTypeDescriptor leftType,
							DataTypeDescriptor rightType,
							String operator
								)
							throws StandardException;

	/**
	 * Determine if this type can be CONVERTed to some other type
	 *
	 * @param otherType	The CompilationType of the other type to compare
	 *					this type to
	 *
	 * @param forDataTypeFunction  true if this is a type function that
	 *   requires more liberal behavior (e.g DOUBLE can convert a char but 
	 *   you cannot cast a CHAR to double.
	 *   
	 * @return	true if the types can be converted, false if conversion
	 *			is not allowed
	 */
	 boolean             convertible(TypeId otherType, 
									 boolean forDataTypeFunction);

	/**
	 * Determine if this type is compatible to some other type
	 * (e.g. COALESCE(thistype, othertype)).
	 *
	 * @param otherType	The CompilationType of the other type to compare
	 *					this type to
	 *
	 * @return	true if the types are compatible, false if not compatible
	 */
	boolean compatible(TypeId otherType);

	/**
	 * Determine if this type can have a value of another type stored into it.
	 * Note that direction is relevant here: the test is that the otherType
	 * is storable into this type.
	 *
	 * @param otherType	The TypeId of the other type to compare this type to
	 * @param cf		A ClassFactory
	 *
	 * @return	true if the other type can be stored in a column of this type.
	 */

	boolean				storable(TypeId otherType, ClassFactory cf);

	/**
	 * Get the name of the interface for this type.  For example, the interface
	 * for a SQLInteger is NumberDataValue.  The full path name of the type
	 * is returned.
	 *
	 * @return	The name of the interface for this type.
	 */
	String interfaceName();

	/**
	 * Get the name of the corresponding Java type.  For numerics and booleans
	 * we will get the corresponding Java primitive type.
	 e
	 * Each SQL type has a corresponding Java type.  When a SQL value is
	 * passed to a Java method, it is translated to its corresponding Java
	 * type.  For example, a SQL Integer will be mapped to a Java int, but
	 * a SQL date will be mapped to a java.sql.Date.
	 *
	 * @return	The name of the corresponding Java primitive type.
	 */
	String	getCorrespondingPrimitiveTypeName();

	/**
	 * Get the method name for getting out the corresponding primitive
	 * Java type from a DataValueDescriptor.
	 *
	 * @return String		The method call name for getting the
	 *						corresponding primitive Java type.
	 */
	String getPrimitiveMethodName();

	/**
	 * Generate the code necessary to produce a SQL null of the appropriate
	 * type. The stack must contain a DataValueFactory and a null or a value
	 * of the correct type (interfaceName()).
	 *
	 * @param mb	The method to put the expression in
	 * @param collationType For character DVDs, this will be used to determine
	 *   what Collator should be associated with the DVD which in turn will 
	 *   decide whether to generate CollatorSQLcharDVDs or SQLcharDVDs.
	 */

	void generateNull(MethodBuilder mb, DataTypeDescriptor dtd, LocalField[] localFields);


	/**
	 * Generate the code necessary to produce a SQL value based on
	 * a value.  The value's type is assumed to match
	 * the type of this TypeId.  For example, a TypeId
	 * for the SQL int type should be given an value that evaluates
	 * to a Java int or Integer.
	 *
	 * If the type of the value is incorrect, the generated code will
	 * not work.
	 * 
	 * The stack must contain data value factory value.
	 * 
	 * @param mb	The method to put the expression in
	 * @param collationType For character DVDs, this will be used to determine
	 *   what Collator should be associated with the DVD which in turn will 
	 *   decide whether to generate CollatorSQLcharDVDs or SQLcharDVDs. For 
	 *   other types of DVDs, this parameter will be ignored.
	 * @param field LocalField
	 */
	void generateDataValue(
			MethodBuilder mb, int collationType, 
			LocalField field);

	/**
	 * Return the maximum width for this data type when cast to a char type.
	 *
	 * @param dts		The associated DataTypeDescriptor for this TypeId.
	 *
	 * @return int			The maximum width for this data type when cast to a char type.
	 */
	int getCastToCharWidth(DataTypeDescriptor dts);

}
