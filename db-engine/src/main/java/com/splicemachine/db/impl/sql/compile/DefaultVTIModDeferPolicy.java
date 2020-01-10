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

package com.splicemachine.db.impl.sql.compile;

import com.splicemachine.db.vti.DeferModification;

/**
 * This class implements the default policy for defering modifications to virtual
 * tables.
 */
class DefaultVTIModDeferPolicy implements DeferModification
{
    private final String targetVTIClassName;
    private final boolean VTIResultSetIsSensitive;

    DefaultVTIModDeferPolicy( String targetVTIClassName,
                              boolean VTIResultSetIsSensitive)
    {
        this.targetVTIClassName = targetVTIClassName;
        this.VTIResultSetIsSensitive = VTIResultSetIsSensitive;
    }

    /**
     * @see com.splicemachine.db.vti.DeferModification#alwaysDefer
     */
    public boolean alwaysDefer( int statementType)
    {
        return false;
    }
          
    /**
     * @see com.splicemachine.db.vti.DeferModification#columnRequiresDefer
     */
    public boolean columnRequiresDefer( int statementType,
                                        String columnName,
                                        boolean inWhereClause)
    {
        switch( statementType)
        {
        case DeferModification.INSERT_STATEMENT:
            return false;

        case DeferModification.UPDATE_STATEMENT:
            return VTIResultSetIsSensitive && inWhereClause;

        case DeferModification.DELETE_STATEMENT:
            return false;
        }
        return false; // Should not get here.
    } // end of columnRequiresDefer

    /**
     * @see com.splicemachine.db.vti.DeferModification#subselectRequiresDefer(int,String,String)
     */
    public boolean subselectRequiresDefer( int statementType,
                                           String schemaName,
                                           String tableName)
    {
        return false;
    } // end of subselectRequiresDefer( statementType, schemaName, tableName)

    /**
     * @see com.splicemachine.db.vti.DeferModification#subselectRequiresDefer(int, String)
     */
    public boolean subselectRequiresDefer( int statementType,
                                           String VTIClassName)
    {
        return targetVTIClassName.equals( VTIClassName);
    } // end of subselectRequiresDefer( statementType, VTIClassName)

    public void modificationNotify( int statementType,
                                    boolean deferred)
    {}
}
