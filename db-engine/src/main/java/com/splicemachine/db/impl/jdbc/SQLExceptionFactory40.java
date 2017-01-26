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
 * All such Splice Machine modifications are Copyright 2012 - 2017 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */

package com.splicemachine.db.impl.jdbc;

import java.sql.SQLDataException;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.SQLInvalidAuthorizationSpecException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.SQLSyntaxErrorException;
import java.sql.SQLTimeoutException;
import java.sql.SQLTransactionRollbackException;
import java.sql.SQLFeatureNotSupportedException;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.shared.common.reference.SQLState;

/**
 * SQLExceptionFactory40 overwrites getSQLException method
 * to return SQLException or one of its sub class
 */

public class SQLExceptionFactory40 extends SQLExceptionFactory {
    
    /**
     * overwrites super class method to create JDBC4 exceptions      
     * SQLSTATE CLASS (prefix)     Exception
     * 0A                          java.sql.SQLFeatureNotSupportedException
     * 08                          java.sql.SQLNonTransientConnectionException
     * 22                          java.sql.SQLDataException
     * 28                          java.sql.SQLInvalidAuthorizationSpecException
     * 40                          java.sql.SQLTransactionRollbackException
     * 42                          java.sql.SQLSyntaxErrorException
     * 
     * Note the following divergence from JDBC3 behavior: When running
     * a JDBC3 client, we return EmbedSQLException. That exception class
     * overrides Throwable.toString() and strips off the Throwable's class name.
     * In contrast, the following JDBC4 implementation returns
     * subclasses of java.sql.Exception. These subclasses inherit the behavior 
     * of Throwable.toString(). That is, their toString() output includes
     * their class name. This will break code which relies on the
     * stripping behavior of EmbedSQLSxception.toString(). 
     */
    
    public SQLException getSQLException(String message, String messageId,
            SQLException next, int severity, Throwable t, Object[] args) {
        String sqlState = StandardException.getSQLStateFromIdentifier(messageId);

		//
		// Create dummy exception which ferries arguments needed to serialize
		// SQLExceptions across the DRDA network layer.
		//
		t = wrapArgsForTransportAcrossDRDA( message, messageId, next, severity, t, args );

        final SQLException ex;
        if (sqlState.startsWith(SQLState.CONNECTIVITY_PREFIX)) {
            //none of the sqlstate supported by db belongs to
            //TransientConnectionException DERBY-3074
            ex = new SQLNonTransientConnectionException(message, sqlState,
                    severity, t);
        } else if (sqlState.startsWith(SQLState.SQL_DATA_PREFIX)) {
            ex = new SQLDataException(message, sqlState, severity, t);
        } else if (sqlState.startsWith(SQLState.INTEGRITY_VIOLATION_PREFIX)) {
            ex = new SQLIntegrityConstraintViolationException(message, sqlState,
                    severity, t);
        } else if (sqlState.startsWith(SQLState.AUTHORIZATION_SPEC_PREFIX)) {
            ex = new SQLInvalidAuthorizationSpecException(message, sqlState,
                    severity, t);
        }        
        else if (sqlState.startsWith(SQLState.TRANSACTION_PREFIX)) {
            ex = new SQLTransactionRollbackException(message, sqlState,
                    severity, t);
        } else if (sqlState.startsWith(SQLState.LSE_COMPILATION_PREFIX)) {
            ex = new SQLSyntaxErrorException(message, sqlState, severity, t);
        } else if (sqlState.startsWith(SQLState.UNSUPPORTED_PREFIX)) {
            ex = new SQLFeatureNotSupportedException(message, sqlState, severity, t);
        } else if (sqlState.equals(SQLState.LANG_STATEMENT_CANCELLED_OR_TIMED_OUT.substring(0, 5))) {
            ex = new SQLTimeoutException(message, sqlState, severity, t);
        } else {
            ex = new SQLException(message, sqlState, severity, t);
        }
        
        if (next != null) {
            ex.setNextException(next);
        }
        return ex;
    }        

	/**
	 * <p>
	 * The following method helps handle DERBY-1178. The problem is that we may
	 * need to serialize our final SQLException across the DRDA network layer.
	 * That serialization involves some clever encoding of the Derby messageID and
	 * arguments. Unfortunately, once we create one of the
	 * JDBC4-specific subclasses of SQLException, we lose the messageID and
	 * args. This method creates a dummy EmbedSQLException which preserves that
	 * information. We return the dummy exception.
	 * </p>
	 */
	private	SQLException	wrapArgsForTransportAcrossDRDA
	( String message, String messageId, SQLException next, int severity, Throwable t, Object[] args )
	{
        // Generate an EmbedSQLException
        return super.getSQLException(message, messageId,
            (next == null ? null : getArgumentFerry(next)),
            severity, t, args);
	}
	
}
