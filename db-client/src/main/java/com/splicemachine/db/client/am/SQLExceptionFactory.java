/*
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
 * Splice Machine, Inc. has modified this file.
 *
 * All Splice Machine modifications are Copyright 2012 - 2020 Splice Machine, Inc.,
 * and are licensed to you under the License; you may not use this file except in
 * compliance with the License.
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 */

package com.splicemachine.db.client.am;

import java.sql.SQLException;
import com.splicemachine.db.shared.common.reference.SQLState;

/**
 * class to create SQLException
 */

public class SQLExceptionFactory {     
     
    public static SQLException notImplemented (String feature) {
        SqlException sqlException = new SqlException (null, 
                new ClientMessageId (SQLState.NOT_IMPLEMENTED), feature);
        return sqlException.getSQLException();
    }
    
    /**
     * creates SQLException initialized with all the params received from the 
     * caller. This method will be overwritten to support jdbc version specific 
     * exception class.
     */
    public SQLException getSQLException (String message, String sqlState, 
            int errCode) {
        return new SQLException (message, sqlState, errCode);           
    }    
}
 
