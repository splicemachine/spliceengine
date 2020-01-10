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

package com.splicemachine.test_tools;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @author Scott Fines
 *         Date: 8/28/15
 */
public interface RowCreator{

    /**
     * @return {@code false} if there are no more rows to work with
     */
    boolean advanceRow();

    /**
     * @return the number of rows to work with at one time
     */
    int batchSize();

    /**
     * Set the value for this row into the prepared statement.
     *
     * DO NOT CLOSE the statement! It is managed elsewhere.
     *
     * @param ps the statement to work with
     * @throws SQLException if something goes wrong
     */
    void setRow(PreparedStatement ps) throws SQLException;

    void reset();
}
