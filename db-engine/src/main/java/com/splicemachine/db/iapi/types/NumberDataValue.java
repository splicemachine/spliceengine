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

package com.splicemachine.db.iapi.types;

import com.splicemachine.db.iapi.error.StandardException;

import java.math.BigDecimal;

import static com.splicemachine.db.iapi.reference.Limits.DB2_MAX_DECIMAL_PRECISION_SCALE;

public interface NumberDataValue extends DataValueDescriptor
{
	/**
	 * The minimum scale when dividing Decimals
	 */
	int MIN_DECIMAL_DIVIDE_SCALE = 4;
	int MAX_DECIMAL_PRECISION_SCALE = DB2_MAX_DECIMAL_PRECISION_SCALE;
	int OLD_MAX_DECIMAL_PRECISION_SCALE = 31;
	int MAX_DECIMAL_PRECISION_WITH_RESERVE_FOR_SCALE = MAX_DECIMAL_PRECISION_SCALE - MIN_DECIMAL_DIVIDE_SCALE;
	int OLD_MAX_DECIMAL_PRECISION_WITH_RESERVE_FOR_SCALE = OLD_MAX_DECIMAL_PRECISION_SCALE - MIN_DECIMAL_DIVIDE_SCALE;

	/**
	 * The SQL + operator.
	 *
	 * @param addend1	One of the addends
	 * @param addend2	The other addend
	 * @param result	The result of the previous call to this method, null
	 *					if not called yet.
	 *
	 * @return	The sum of the two addends
	 *
	 * @exception StandardException		Thrown on error, if result is non-null then its value will be unchanged.
	 */
	NumberDataValue plus(NumberDataValue addend1,
						 NumberDataValue addend2,
						 NumberDataValue result)
							throws StandardException;

	/**
	 * The SQL - operator.
	 *
	 * @param left		The left operand
	 * @param right		The right operand
	 * @param result	The result of the previous call to this method, null
	 *					if not called yet.
	 *
	 * @return	left - right
	 *
	 * @exception StandardException		Thrown on error, if result is non-null then its value will be unchanged.
	 */
	NumberDataValue minus(NumberDataValue left,
						  NumberDataValue right,
						  NumberDataValue result)
							throws StandardException;

	/**
	 * The SQL * operator.
	 *
	 * @param left		The left operand
	 * @param right		The right operand
	 * @param result	The result of the previous call to this method, null
	 *					if not called yet.
	 *
	 * @return	left * right
	 *
	 * @exception StandardException		Thrown on error, if result is non-null then its value will be unchanged.
	 */
	NumberDataValue times(NumberDataValue left,
						  NumberDataValue right,
						  NumberDataValue result)
							throws StandardException;

	/**
	 * The SQL / operator.
	 *
	 * @param dividend		The numerator
	 * @param divisor		The denominator
	 * @param result		The result of the previous call to this method, null
	 *						if not called yet.
	 *
	 * @return	dividend / divisor
	 *
	 * @exception StandardException		Thrown on error, if result is non-null then its value will be unchanged.
	 */
	NumberDataValue divide(NumberDataValue dividend,
						   NumberDataValue divisor,
						   NumberDataValue result)
							throws StandardException;

	/**
	 * The SQL / operator.
	 *
	 * @param dividend		The numerator
	 * @param divisor		The denominator
	 * @param result		The result of the previous call to this method, null
	 *						if not called yet.
	 * @param scale			The scale of the result, for decimal type.  If pass
	 *						in value < 0, can calculate it dynamically.
	 *
	 * @return	dividend / divisor
	 *
	 * @exception StandardException		Thrown on error, if result is non-null then its value will be unchanged.
	 */
	NumberDataValue divide(NumberDataValue dividend,
						   NumberDataValue divisor,
						   NumberDataValue result,
						   int scale)
							throws StandardException;


	/**
	 * The SQL mod operator.
	 *
	 * @param dividend		The numerator
	 * @param divisor		The denominator
	 * @param result		The result of the previous call to this method, null
	 *						if not called yet.
	 *
	 * @return	dividend / divisor
	 *
	 * @exception StandardException		Thrown on error, if result is non-null then its value will be unchanged.
	 */
	NumberDataValue mod(NumberDataValue dividend,
						NumberDataValue divisor,
						NumberDataValue result)
							throws StandardException;

	/**
	 * The SQL unary - operator.  Negates this NumberDataValue.
	 *
	 * @param result	The result of the previous call to this method, null
	 *					if not called yet.
	 *
	 * @return	- operand
	 *
	 * @exception StandardException		Thrown on error, if result is non-null then its value will be unchanged.
	 */
	NumberDataValue minus(NumberDataValue result)
							throws StandardException;

    /**
     * The SQL ABSOLUTE operator.  Absolute value of this NumberDataValue.
     *
     * @param result    The result of the previous call to this method, null
     *                  if not called yet.
     *
     * @exception StandardException     Thrown on error, if result is non-null then its value will be unchanged.
     */
	NumberDataValue absolute(NumberDataValue result)
                            throws StandardException;

    /**
     * The SQL SQRT operator.  Sqrt value of this NumberDataValue.
     *
     * @param result    The result of the previous call to this method, null
     *                  if not call yet.
     * 
     * @exception StandardException     Thrown on error (a negative number), if result is non-null then its value will be unchanged.
     */
	NumberDataValue sqrt(NumberDataValue result)
                            throws StandardException;

	/**
	 * Set the value of this NumberDataValue to the given value.
	   This is only intended to be called when mapping values from
	   the Java space into the SQL space, e.g. parameters and return
	   types from procedures and functions. Each specific type is only
	   expected to handle the explicit type according the JDBC.
	   <UL>
	   <LI> SMALLINT from java.lang.Integer
	   <LI> INTEGER from java.lang.Integer
	   <LI> LONG from java.lang.Long
	   <LI> FLOAT from java.lang.Float
	   <LI> DOUBLE from java.lang.Double
	   <LI> DECIMAL from java.math.BigDecimal
	   </UL>
	 *
	 * @param theValue	An Number containing the value to set this
	 *					NumberDataValue to.  Null means set the value
	 *					to SQL null.
	 *
	 */
	void setValue(Number theValue) throws StandardException;

	/**
		Return the SQL precision of this specific DECIMAL value.
		This does not match the return from BigDecimal.precision()
		added in J2SE 5.0, which represents the precision of the unscaled value.
		If the value does not represent a SQL DECIMAL then
		the return is undefined.
	*/
	int getDecimalValuePrecision();

	/**
		Return the SQL scale of this specific DECIMAL value.
		This does not match the return from BigDecimal.scale()
		since in J2SE 5.0 onwards that can return negative scales.
		If the value does not represent a SQL DECIMAL then
		the return is undefined.
	*/
	int getDecimalValueScale();

	/**
	 * Returns BigDecimal representation of value
	 * @return BigDecimal value
	 * @throws StandardException
	 */
	BigDecimal getBigDecimal() throws StandardException;

	StringDataValue digits(NumberDataValue source, int len, StringDataValue result) throws StandardException;
}









