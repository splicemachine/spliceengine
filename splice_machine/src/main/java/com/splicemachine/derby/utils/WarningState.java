/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
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

package com.splicemachine.derby.utils;

import com.splicemachine.db.iapi.error.StandardException;

import java.sql.SQLWarning;

/**
 * @author Scott Fines
 *         Date: 9/10/14
 */
public enum WarningState {
    XPLAIN_STATEMENT_ID("SE016");

    private final String sqlState;

    WarningState(String sqlState) {
        this.sqlState = sqlState;
    }

    public String getSqlState(){
        return sqlState;
    }

    public SQLWarning newWarning(Object...args){
        return StandardException.newWarning(getSqlState(),args);
    }
}
