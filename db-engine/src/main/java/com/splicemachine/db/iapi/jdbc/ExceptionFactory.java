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

package com.splicemachine.db.iapi.jdbc;

import java.sql.SQLException;

/**
 * An exception factory is used to create SQLExceptions of the correct type.
 */
public interface ExceptionFactory {

    /**
     * Unpack a SQL exception, looking for an EmbedSQLException which carries
     * the Derby messageID and args which we will serialize across DRDA so
     * that the client can reconstitute a SQLException with appropriate text.
     * If we are running JDBC 3, then we hope that the passed-in
     * exception is already an EmbedSQLException, which carries all the
     * information we need.
     *
     * @param se the exception to unpack
     * @return the argument ferry for the exception
     */
    SQLException getArgumentFerry(SQLException se);

    /**
     * Construct an SQLException whose message and severity are specified
     * explicitly.
     *
     * @param message the exception message
     * @param messageId the message id
     * @param next the next SQLException
     * @param severity the severity of the exception
     * @param cause the cause of the exception
     * @param args the message arguments
     * @return an SQLException
     */
    SQLException getSQLException(String message, String messageId,
            SQLException next, int severity, Throwable cause, Object[] args);

    /**
     * Construct an SQLException whose message and severity are derived from
     * the message id.
     *
     * @param messageId the message id
     * @param next the next SQLException
     * @param cause the cause of the exception
     * @param args the message arguments
     * @return an SQLException
     */
    SQLException getSQLException(String messageId, SQLException next,
            Throwable cause, Object[] args);

}
