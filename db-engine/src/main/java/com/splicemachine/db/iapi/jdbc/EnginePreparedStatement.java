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

import java.io.InputStream;
import java.io.Reader;
import java.sql.SQLException;
import java.sql.PreparedStatement;

/**
 * Additional methods the embedded engine exposes on its 
 * PreparedStatement object implementations. An internal api only, mainly 
 * for the network server. Allows consistent interaction between embedded 
 * PreparedStatement and Brokered PreparedStatements.
 * (DERBY-1015)
 */
public interface EnginePreparedStatement extends PreparedStatement {
    
    void setBinaryStream(int parameterIndex, InputStream x)
        throws SQLException; 
    
    void setCharacterStream(int parameterIndex, Reader reader)
        throws SQLException;

    /**
     * Get the version of the prepared statement. If this has not been changed,
     * the caller may assume that a recompilation has not taken place, i.e.
     * meta-data are (also) unchanged.
     * @return version counter
     */
    long getVersionCounter() throws SQLException;
}
