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

package com.splicemachine.db.iapi.sql.dictionary;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.conn.Authorizer;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.sql.Activation;

/**
 * This class describes a role permission required by a statement.
 */

public class StatementRolePermission extends StatementPermission
{
    private String roleName;
    private int privType;

    /**
     * Constructor
     *
     * @param roleName The role name involved in the operation
     * @param privType One of Authorizer.CREATE_ROLE_PRIV, DROP_ROLE_PRIV.
     */
    public StatementRolePermission(String roleName, int privType)
    {
        this.roleName = roleName;
        this.privType = privType;
    }

    /**
     * @see StatementPermission#check
     */
    public void check(LanguageConnectionContext lcc,
                      boolean forGrant,
                      Activation activation
                      ) throws StandardException
    {
        DataDictionary dd = lcc.getDataDictionary();
        TransactionController tc = lcc.getTransactionExecute();

        // For now, only allowed for database owner, and this check
        // is never called for dbo, so always throw.
        switch (privType) {
        case Authorizer.CREATE_ROLE_PRIV:
            throw StandardException.newException
                (SQLState.AUTH_ROLE_DBO_ONLY, "CREATE ROLE");
            // break;
        case Authorizer.DROP_ROLE_PRIV:
            throw StandardException.newException
                (SQLState.AUTH_ROLE_DBO_ONLY, "DROP ROLE");
            // break;
        default:
            if (SanityManager.DEBUG) {
                SanityManager.THROWASSERT
                    ("Unexpected value (" + privType + ") for privType");
            }
            break;
        }
    }

    /**
     * Role level permission is never required as list of privileges required
     * for triggers/constraints/views and hence we don't do any work here, but
     * simply return null
     *
     * @see StatementPermission#check
     */
    public PermissionsDescriptor getPermissionDescriptor(String authid,
                                                         DataDictionary dd)
        throws StandardException
    {
        return null;
    }


    private String getPrivName( )
    {
        switch(privType) {
        case Authorizer.CREATE_ROLE_PRIV:
            return "CREATE_ROLE";
        case Authorizer.DROP_ROLE_PRIV:
            return "DROP_ROLE";
        default:
            return "?";
        }
    }

    public String toString()
    {
        return "StatementRolePermission: " + roleName + " " + getPrivName();
    }
}
