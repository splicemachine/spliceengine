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

import com.splicemachine.db.iapi.error.StandardException;

/**
 * The Parser interface is intended to work with Jack-generated parsers (now JavaCC).
 * We will specify "STATIC=false" when building Jack parsers - this specifies
 * that the generated classes will not be static, which will allow there to be
 * more than one parser (this is necessary in a multi-threaded server).
 * Non-static parsers do not have to be re-initialized every time they are
 * used (unlike static parsers, for which one must call ReInit() between calls
 * to the parser).
 *
 */


public interface Parser
{

	/**
	 * Parses the given statement and returns a query tree. The query tree
	 * at this point is a simple syntactic translation of the statement.
	 * No binding will have taken place, and no decisions will have been
	 * made regarding processing strategy.
	 *
	 * @param statementSQLText	The Statement to parse.
	 * @param paramDefaults	Parameter defaults
	 * @return	A new QueryTree representing the syntax of the Statement
	 *
	 * @exception StandardException		Thrown on failure
	 */
	Visitable parseStatement(String statementSQLText,
							 Object[] paramDefaults)
		throws StandardException;


	Visitable parseStatement(String statementSQLText)
		throws StandardException;

	/**
	* Parse an SQL fragment that represents a {@code <search condition>}.
	*
	* @param sqlFragment the SQL fragment to parse
	* @return a parse tree representing the search condition
	* @throws StandardException if the SQL fragment could not be parsed
	*/
	Visitable parseSearchCondition(String sqlFragment)
		throws StandardException;

	/**
	 * Returns the current SQL text string that is being parsed.
	 *
	 * @return	Current SQL text string.
	 *
	 */
	String		getSQLtext();

}
