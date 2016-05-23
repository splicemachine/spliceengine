/*

   Derby - Class com.splicemachine.db.iapi.sql.dictionary.StatementRolePermission

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

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
