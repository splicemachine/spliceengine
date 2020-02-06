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

import com.splicemachine.db.shared.common.reference.SQLState;
import com.splicemachine.db.shared.common.error.ExceptionSeverity;

import java.sql.SQLDataException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.SQLInvalidAuthorizationSpecException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.SQLSyntaxErrorException;
import java.sql.SQLTimeoutException;
import java.sql.SQLTransactionRollbackException;


/**
 * SQLException factory class to create jdbc 40 exception classes
 */

public class SQLExceptionFactory40 extends SQLExceptionFactory {
    
    // Important DRDA SQL States, from DRDA v3 spec, Section 8.2
    // We have to consider these as well as the standard SQLState classes
    // when choosing the right exception subclass
    private static final String DRDA_CONVERSATION_TERMINATED    = "58009";    
    private static final String DRDA_COMMAND_NOT_SUPPORTED      = "58014";
    private static final String DRDA_OBJECT_NOT_SUPPORTED       = "58015";
    private static final String DRDA_PARAM_NOT_SUPPORTED        = "58016";
    private static final String DRDA_VALUE_NOT_SUPPORTED        = "58017";
    private static final String DRDA_SQLTYPE_NOT_SUPPORTED      = "56084";
    private static final String DRDA_CONVERSION_NOT_SUPPORTED   = "57017";
    private static final String DRDA_REPLY_MSG_NOT_SUPPORTED    = "58018";
       
    /**
     * creates jdbc4.0 SQLException and its subclass based on sql state
     * 
     * @param message description of the 
     * @param sqlState 
     * @param errCode db error code
     */
    public SQLException getSQLException (String message, String sqlState, 
                                                            int errCode) { 
        SQLException ex = null;
        if (sqlState == null) {
            ex = new SQLException(message, sqlState, errCode); 
        } else if (sqlState.startsWith(SQLState.CONNECTIVITY_PREFIX) ||
            errCode >= ExceptionSeverity.SESSION_SEVERITY) {
            //none of the sqlstate supported by db belongs to
            //TransientConnectionException. DERBY-3075
            ex = new SQLNonTransientConnectionException(message, sqlState, errCode);
        } else if (sqlState.startsWith(SQLState.SQL_DATA_PREFIX)) {
            ex = new SQLDataException(message, sqlState, errCode);
        } else if (sqlState.startsWith(SQLState.INTEGRITY_VIOLATION_PREFIX)) {
            ex = new SQLIntegrityConstraintViolationException(message, sqlState,
                    errCode);
        } else if (sqlState.startsWith(SQLState.AUTHORIZATION_SPEC_PREFIX)) {
            ex = new SQLInvalidAuthorizationSpecException(message, sqlState,
                    errCode);
        } else if (sqlState.startsWith(SQLState.TRANSACTION_PREFIX) ||
            errCode >= ExceptionSeverity.TRANSACTION_SEVERITY ) {
            ex = new SQLTransactionRollbackException(message, sqlState,
                    errCode);
        } else if (sqlState.startsWith(SQLState.LSE_COMPILATION_PREFIX)) {
            ex = new SQLSyntaxErrorException(message, sqlState, errCode);
        } else if (
            sqlState.startsWith (SQLState.UNSUPPORTED_PREFIX)   ||
            sqlState.equals(DRDA_COMMAND_NOT_SUPPORTED)         ||
            sqlState.equals(DRDA_OBJECT_NOT_SUPPORTED)          ||
            sqlState.equals(DRDA_PARAM_NOT_SUPPORTED)           ||
            sqlState.equals(DRDA_VALUE_NOT_SUPPORTED)           ||
            sqlState.equals(DRDA_SQLTYPE_NOT_SUPPORTED)         ||
            sqlState.equals(DRDA_REPLY_MSG_NOT_SUPPORTED)           ) {
            ex = new SQLFeatureNotSupportedException(message, sqlState, 
                    errCode);
        } else if (sqlState.equals(SQLState.LANG_STATEMENT_CANCELLED_OR_TIMED_OUT.substring(0, 5))) {
            ex = new SQLTimeoutException(message, sqlState, errCode);
        } else if (sqlState.equals(SQLState.LANG_CANCELLATION_EXCEPTION)) {
            ex = new SQLTimeoutException(message, sqlState, errCode);
        } else {
            ex = new SQLException(message, sqlState, errCode); 
        }
        return ex;
    }
}
