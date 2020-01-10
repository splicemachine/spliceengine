/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.db.impl.sql.misc;

import com.splicemachine.db.iapi.error.StandardException;

/**
 * Created by yxia on 6/27/18.
 */
public interface CommentStripper {

    /**
     * Parses the given statement text and return a version with the comment stripped
     *
     * @param statementSQLText	The Statement to parse.
     * @return	A new string with the comment stripped
     *
     * @exception StandardException        Thrown on failure
     */
    public String stripStatement(String statementSQLText)
            throws StandardException;

    /**
     * Returns the current SQL text string that is being parsed.
     *
     * @return	Current SQL text string.
     *
     */
    public	String		getSQLtext();

}
