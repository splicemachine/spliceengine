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

package com.splicemachine.db.impl.sql.catalog;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.uuid.UUIDFactory;
import com.splicemachine.db.iapi.sql.dictionary.CatalogRowFactory;
import com.splicemachine.db.iapi.sql.dictionary.PermissionsDescriptor;
import com.splicemachine.db.iapi.sql.execute.ExecIndexRow;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.sql.execute.ExecutionFactory;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.DataValueFactory;
import com.splicemachine.db.iapi.types.SQLVarchar;

abstract class PermissionsCatalogRowFactory extends CatalogRowFactory
{
    PermissionsCatalogRowFactory(UUIDFactory uuidf, ExecutionFactory ef, DataValueFactory dvf)
    {
        super(uuidf,ef,dvf);
    }

    DataValueDescriptor getAuthorizationID( String value)
    {
        return new SQLVarchar(value);
    }

    DataValueDescriptor getNullAuthorizationID()
    {
        return new SQLVarchar();
    }

    /**
     * Extract an internal authorization ID from a row.
     *
     * @param row
     * @param columnPos 1 based
     *
     * @return The internal authorization ID
     */
    String getAuthorizationID( ExecRow row, int columnPos)
        throws StandardException
    {
        return row.getColumn( columnPos).getString();
    }

    /**
     * Build an index key row from a permission descriptor. A key row does not include the RowLocation column.
     *
     * @param indexNumber
     * @param perm a permission descriptor of the appropriate class for this PermissionsCatalogRowFactory class.
     *
     * @exception StandardException standard error policy
     */
    abstract ExecIndexRow buildIndexKeyRow( int indexNumber,
                                                   PermissionsDescriptor perm)
        throws StandardException;

    /**
     * Or a set of permissions in with a row from this catalog table
     *
     * @param row an existing row
     * @param perm a permission descriptor of the appropriate class for this PermissionsCatalogRowFactory class.
     * @param colsChanged An array with one element for each column in row. It is updated to
     *                    indicate which columns in row were changed
     *
     * @return The number of columns that were changed.
     *
     * @exception StandardException standard error policy
     */
    abstract int orPermissions( ExecRow row, PermissionsDescriptor perm, boolean[] colsChanged)
        throws StandardException;

    /**
     * Remove a set of permissions from a row from this catalog table
     *
     * @param row an existing row
     * @param perm a permission descriptor of the appropriate class for this PermissionsCatalogRowFactory class.
     * @param colsChanged An array with one element for each column in row. It is updated to
     *                    indicate which columns in row were changed
     *
     * @return -1 if there are no permissions left in the row, otherwise the number of columns that were changed.
     *
     * @exception StandardException standard error policy
     */
    abstract int removePermissions( ExecRow row, PermissionsDescriptor perm, boolean[] colsChanged)
        throws StandardException;

    /**
     * Set the uuid of the passed permission descriptor to the uuid of the row
     * from the system table. DataDictionary will make this call before calling 
     * the dependency manager to send invalidation messages to the objects 
     * dependent on the permission descriptor's uuid.
     * 
     * @param row The row from the system table for the passed permission descriptor
     * @param perm Permission descriptor
     * @throws StandardException
     */
    abstract void setUUIDOfThePassedDescriptor(ExecRow row, PermissionsDescriptor perm) throws StandardException;
}
