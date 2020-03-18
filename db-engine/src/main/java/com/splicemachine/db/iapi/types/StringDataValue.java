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

package com.splicemachine.db.iapi.types;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.jdbc.CharacterStreamDescriptor;

import java.sql.Clob;
import java.text.RuleBasedCollator;

public interface StringDataValue extends ConcatableDataValue
{
	// TRIM() types
	int BOTH		= 0;
	int TRAILING	= 1;
	int LEADING		= 2;

	/**
	  For a character string type, the collation derivation should always be
	  "explicit"(not possible in Derby 10.3), "implicit" or "none". We will
	  start by setting it to "implicit" in TypeDescriptorImpl. At runtime, only
	  character string types which are results of aggregate methods dealing
	  with operands with different collation types should have a collation
	  derivation of "none". All the other character string types should have
	  their collation derivation set to "implicit".
	 */
	int COLLATION_DERIVATION_NONE = 0;
	/** @see StringDataValue#COLLATION_DERIVATION_NONE */
	int COLLATION_DERIVATION_IMPLICIT = 1;
	/** @see StringDataValue#COLLATION_DERIVATION_NONE */
	int COLLATION_DERIVATION_EXPLICIT = 2;
	/**
	 * In Derby 10.3, it is possible to have database with one of the following
	 * two configurations
	 * 1)all the character columns will have a collation type of UCS_BASIC.
	 * This is same as what we do in Derby 10.2 release.
	 * 2)all the character string columns belonging to system tables will have
	 * collation type of UCS_BASIC but all the character string columns
	 * belonging to user tables will have collation type of TERRITORY_BASED.
	 *
	 * Data types will start with collation type defaulting to UCS_BASIC in
	 * TypeDescriptorImpl. This collation type of course makes sense for
	 * character string types only. It will be ignored for the rest of the
	 * types. If a character's collation type should be TERRITORY_BASED, then
	 * DTD.setCollationType can be called to change the default of UCS_BASIC.
	 *
	 * The collation types TERRITORY_BASED:PRIMARY through TERRITORY_BASED:IDENTICAL
	 * are variants of the TERRITORY_BASED type, with explicit collation strength.
	 */
	int COLLATION_TYPE_UCS_BASIC = 0;
	/** @see StringDataValue#COLLATION_TYPE_UCS_BASIC */
	int COLLATION_TYPE_TERRITORY_BASED = 1;
	int COLLATION_TYPE_TERRITORY_BASED_PRIMARY = 2;
	int COLLATION_TYPE_TERRITORY_BASED_SECONDARY = 3;
	int COLLATION_TYPE_TERRITORY_BASED_TERTIARY = 4;
	int COLLATION_TYPE_TERRITORY_BASED_IDENTICAL = 5;

	/**
	 * The SQL concatenation '||' operator.
	 *
	 * @param leftOperand	String on the left hand side of '||'
	 * @param rightOperand	String on the right hand side of '||'
	 * @param result	The result of a previous call to this method,
	 *					null if not called yet.
	 *
	 * @return	A ConcatableDataValue containing the result of the '||'
	 *
	 * @exception StandardException		Thrown on error
	 */
	StringDataValue concatenate(
			StringDataValue leftOperand,
			StringDataValue rightOperand,
			StringDataValue result)
		throws StandardException;

	public StringDataValue repeat(
			StringDataValue leftOperand,
			NumberDataValue repeatedTimes,
			StringDataValue result)
		throws StandardException;

	/**
	 * The SQL like() function with out escape clause.
	 *
	 * @param pattern	the pattern to use
	 *
	 * @return	A BooleanDataValue containing the result of the like
	 *
	 * @exception StandardException		Thrown on error
	 */
	BooleanDataValue like(DataValueDescriptor pattern)
							throws StandardException;

	/**
	 * The SQL like() function WITH escape clause.
	 *
	 * @param pattern	the pattern to use
	 * @param escape	the escape character
	 *
	 * @return	A BooleanDataValue containing the result of the like
	 *
	 * @exception StandardException		Thrown on error
	 */
	BooleanDataValue like(DataValueDescriptor pattern,
						  DataValueDescriptor escape)
							throws StandardException;

	/**
	 * right() function.
	 * @param length Number of characters to take.
	 * @param result The result of this method.
	 * @return A StringDataValue containing the result of the right().
	 * @throws StandardException
	 */
	StringDataValue right(
			NumberDataValue length,
			StringDataValue result)
		throws StandardException;
	/**
	 * left() function.
	 * @param length Number of characters to take.
	 * @param result The result of this method.
	 * @return A StringDataValue containing the result of the left().
	 * @throws StandardException
	 */
	StringDataValue left(
			NumberDataValue length,
			StringDataValue result)
		throws StandardException;

	/**
	 * The SQL Ansi trim function.

	 * @param trimType type of trim. Possible values are {@link #LEADING}, {@link #TRAILING}
	 *        or {@link #BOTH}.
	 * @param trimChar  The character to trim from <em>this</em>
	 * @param result The result of a previous call to this method,
	 *					null if not called yet.
	 * @return A StringDataValue containing the result of the trim().
	 * @throws StandardException
	 */
	StringDataValue ansiTrim(
			int trimType,
			StringDataValue trimChar,
			StringDataValue result)
		throws StandardException;

	/**
	 * Convert the string to upper case.
	 *
	 * @param result	The result (reusable - allocate if null).
	 *
	 * @return	The string converted to upper case.
	 *
	 * @exception StandardException		Thrown on error
	 */
	StringDataValue upper(StringDataValue result)
							throws StandardException;

	/**
	 * Covert the string to upper case with specified locale.
	 * @param str The string
	 * @param locale  The locale
	 * @param result the result
	 * @return The string converted to upper case
	 * @throws StandardException Thrown on error
	 */
	StringDataValue upperWithLocale(StringDataValue str, StringDataValue locale, StringDataValue result)
							throws StandardException;

	/**
	 * Covert the string to lower case with specified locale.
	 * @param str The string
	 * @param locale  The locale
	 * @param result the result
	 * @return The string converted to lower case
	 * @throws StandardException Thrown on error
	 */
	StringDataValue lowerWithLocale(StringDataValue str, StringDataValue locale, StringDataValue result)
			throws StandardException;

	/**
	 * Convert the string to lower case.
	 *
	 * @param result	The result (reusable - allocate if null).
	 *
	 * @return	The string converted to lower case.
	 *
	 * @exception StandardException		Thrown on error
	 */
	StringDataValue lower(StringDataValue result)
							throws StandardException;


	/**
	 * Get a char array.  Typically, this is a simple
	 * getter that is cheaper than getString() because
	 * we always need to create a char array when
	 * doing I/O.  Use this instead of getString() where
	 * reasonable.
	 * <p>
	 * <b>WARNING</b>: may return a character array that has spare
	 * characters at the end.  MUST be used in conjunction
	 * with getLength() to be safe.
	 *
	 * @exception StandardException		Thrown on error
	 */
	char[] getCharArray() throws StandardException;

	/**
	 * Gets either SQLChar/SQLVarchar/SQLLongvarchar/SQLClob(base classes) or
	 * CollatorSQLChar/CollatorSQLVarchar/CollatorSQLLongvarch/CollatorSQLClob
	 * (subclasses). Whether this method returns the base class or the subclass
	 * depends on the value of the RuleBasedCollator. If RuleBasedCollator is
	 * null, then the object returned would be baseclass otherwise it would be
	 * subcalss.
	 */
	StringDataValue getValue(RuleBasedCollator collatorForComparison);

    /**
     * Returns the stream header generator for the string data value.
     * <p>
     * The generator writes the correct header into the destination buffer or
     * stream and also keeps track of whether appending an end-of-stream marker
     * is required or not.
     * <p>
     * Note that the generator may fail to generate a header if there is no
     * context at the time the header is asked for, and the mode hasn't been
     * set explicitly.
     * @see #setStreamHeaderFormat
     */
	StreamHeaderGenerator getStreamHeaderGenerator();

    /**
     * Tells the data value descriptor which CLOB stream header format to use.
     *
     * @param usePreTenFiveHdrFormat {@code true} if the database accessed is
     *      prior to version 10.5, {@code false} if the version is 10.5 or
     *      newer, and {@code null} if unknown at this time
     */
	void setStreamHeaderFormat(Boolean usePreTenFiveHdrFormat);

    /**
     * Returns a descriptor for the input stream for this data value.
     * <p>
     * The descriptor contains information about header data, current positions,
     * length, whether the stream should be buffered or not, and if the stream
     * is capable of repositioning itself.
     *
     * @return A descriptor for the stream, which includes a reference to the
     *      stream itself.
     * @throws StandardException if obtaining the descriptor fails, or if the
     *      value isn't represented as a stream.
     */
	CharacterStreamDescriptor getStreamWithDescriptor()
            throws StandardException;


	/**
	 * Stuff a StringDataValue with a Clob.
	 */
	void setValue(Clob value)
		throws StandardException;
}
