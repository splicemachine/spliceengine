/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
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
