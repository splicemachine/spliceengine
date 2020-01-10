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

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Additional methods the embedded engine exposes on its ResultSet object
 * implementations. An internal api only, mainly for the network
 * server
 */
public interface EngineResultSet extends ResultSet {
    
    /**
     * Is this result set from a select for update statement?
     */
    boolean isForUpdate();
    
    /**
     * Is the designated columnIndex a null data value?
     * This is used by EXTDTAInputStream to get the null value without 
     * retrieving the underlying data value.
     * @param columnIndex
     * @return true if the data value at columnIndex for the current row is null 
     * @throws SQLException 
     */
    boolean isNull(int columnIndex) throws SQLException;
    
    /**
     * Return the length of the designated columnIndex data value.
     * Implementation is type dependent.
     * 
     * @param columnIndex  column to access
     * @return length of data value
     * @throws SQLException
     * @see com.splicemachine.db.iapi.types.DataValueDescriptor#getLength()
     */
    int getLength(int columnIndex) throws SQLException;
    
    /**
     * Fetch the holdability of this ResultSet which may be different
     * from the holdability of its Statement.
     * @return HOLD_CURSORS_OVER_COMMIT or CLOSE_CURSORS_AT_COMMIT
     * @throws SQLException Error.
     */
    int getHoldability() throws SQLException;
    
}
