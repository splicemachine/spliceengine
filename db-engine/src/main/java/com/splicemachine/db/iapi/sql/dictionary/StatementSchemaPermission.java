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

package com.splicemachine.db.iapi.sql.dictionary;

import com.splicemachine.db.catalog.UUID;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.services.context.ContextManager;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.conn.Authorizer;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.depend.DependencyManager;
import com.splicemachine.db.iapi.sql.execute.ExecPreparedStatement;
import com.splicemachine.db.iapi.store.access.TransactionController;

import java.util.ArrayList;
import java.util.List;

/**
 * This class describes a schema permission required by a statement.
 */

public class StatementSchemaPermission extends StatementPermission
{

    protected UUID schemaUUID;
    /**
     * The schema name
     */
    private String schemaName;
    /**
     * Authorization id
     */
    private String aid;
    /**
     * One of Authorizer.CREATE_SCHEMA_PRIV, MODIFY_SCHEMA_PRIV,
     * DROP_SCHEMA_PRIV, etc.
     */
    protected int privType;


    public StatementSchemaPermission(UUID schemaUUID ,   int privType)
    {
        this.schemaUUID = schemaUUID;
        this.privType    = privType;
    }

    public StatementSchemaPermission(String schemaName, String aid, int privType)
    {
        this.schemaName = schemaName;
        this.aid     = aid;
        this.privType    = privType;
    }

    /**
     * @see StatementPermission#check
     */
    public void check( LanguageConnectionContext lcc,
                       boolean forGrant,
                       Activation activation) throws StandardException
    {
        DataDictionary dd =    lcc.getDataDictionary();
        TransactionController tc = lcc.getTransactionExecute();
        String currentUserId = lcc.getCurrentUserId(activation);
        List<String> currentGroupuserlist = lcc.getCurrentGroupUser(activation);

        SchemaDescriptor sd;
        switch ( privType )
        {
            case Authorizer.ACCESS_PRIV:
                /* for access priv, all the permissions set schemaUUID instead of schemaName */
                sd = dd.getSchemaDescriptor(schemaUUID, tc);
                if (sd == null)
                    return;

                // if current user is owner, it can access the schema
                if (currentUserId.equals(sd.getAuthorizationId()) ||
                        currentGroupuserlist != null && currentGroupuserlist.contains(sd.getAuthorizationId()))
                    return;

                if (!hasPermissionOnSchema(lcc, dd, activation, forGrant))
                    throw StandardException.newException(
                            SQLState.LANG_SCHEMA_DOES_NOT_EXIST,
                            sd.getSchemaName());
                break;
            case Authorizer.MODIFY_SCHEMA_PRIV:
                sd = dd.getSchemaDescriptor(schemaName, tc, false);
                if (sd == null)
                    return;

                // if current user is owner, it can modify the schema
                if (currentUserId.equals(sd.getAuthorizationId()) ||
                        currentGroupuserlist != null && currentGroupuserlist.contains(sd.getAuthorizationId()))
                    return;

                schemaUUID = sd.getUUID();
                if (!hasPermissionOnSchema(lcc, dd, activation, forGrant))
                    throw StandardException.newException(
                            SQLState.AUTH_NO_ACCESS_NOT_OWNER,
                            currentUserId,
                            schemaName);

                break;
            case Authorizer.DROP_SCHEMA_PRIV:
                sd = dd.getSchemaDescriptor(schemaName, tc, false);
                // If schema hasn't been created already, no need to check
                // for drop schema, an exception will be thrown if the schema
                // does not exists.
                if (sd == null)
                    return;

                if (!currentUserId.equals(sd.getAuthorizationId()))
                    throw StandardException.newException(
                        SQLState.AUTH_NO_ACCESS_NOT_OWNER,
                        currentUserId,
                        schemaName);
                break;

            case Authorizer.CREATE_SCHEMA_PRIV:
                // Non-DBA Users can only create schemas that match their
                // currentUserId Also allow only DBA to set currentUserId to
                // another user Note that for DBA, check interface wouldn't be
                // called at all
                if ( !schemaName.equals(currentUserId) ||
                         (aid != null && !aid.equals(currentUserId)) )

                    throw StandardException.newException(
                        SQLState.AUTH_NOT_DATABASE_OWNER,
                        currentUserId,
                        schemaName);
                break;

            default:
                if (SanityManager.DEBUG)
                {
                    SanityManager.THROWASSERT(
                            "Unexpected value (" + privType + ") for privType");
                }
                break;
        }
    }

    protected boolean hasPermissionOnSchema(LanguageConnectionContext lcc,
                                        DataDictionary dd,
                                        Activation activation,
                                        boolean forGrant) throws StandardException {
        String currentUserId = lcc.getCurrentUserId(activation);
        ExecPreparedStatement ps = activation.getPreparedStatement();
        List<String> currentGroupuserlist = lcc.getCurrentGroupUser(activation);

        int authorization = oneAuthHasPermissionOnSchema(dd, Authorizer.PUBLIC_AUTHORIZATION_ID, forGrant);

        if(authorization == NONE || authorization == UNAUTHORIZED )
            authorization = oneAuthHasPermissionOnSchema(dd, currentUserId, forGrant);
        if((authorization == NONE || authorization == UNAUTHORIZED) && currentGroupuserlist != null ) {
            for (String currentGroupuser : currentGroupuserlist) {
                authorization = oneAuthHasPermissionOnSchema(dd, currentGroupuser, forGrant);
                if (authorization == AUTHORIZED)
                    break;
            }
        }

        if (authorization == NONE ||
            // for access check, if a role grants the access privilege,
            // we would like treat it as the user has the access privilege
            authorization == UNAUTHORIZED && privType == Authorizer.ACCESS_PRIV) {
            // Since no permission exists for the current user or PUBLIC,
            // check if a permission exists for the current role (if set).
            List<String> currentRoles = lcc.getCurrentRoles(activation);
            List<String> rolesToRemove = new ArrayList<>();
            for (String role : currentRoles) {
                // Check that role is still granted to current user or
                // to PUBLIC: A revoked role which is current for this
                // session, is lazily set to none when it is attempted
                // used.
                RoleGrantDescriptor rd = dd.getRoleGrantDescriptor
                        (role, currentUserId);

                if (rd == null) {
                    rd = dd.getRoleGrantDescriptor(
                            role,
                            Authorizer.PUBLIC_AUTHORIZATION_ID);
                }
                if (rd == null && currentGroupuserlist != null) {
                    for (String currentGroupuser : currentGroupuserlist) {
                        rd = dd.getRoleGrantDescriptor(role, currentGroupuser);
                        if (rd != null)
                            break;
                    }
                }

                if (rd == null) {
                    // We have lost the right to set this role, so we can't
                    // make use of any permission granted to it or its
                    // ancestors.
                    rolesToRemove.add(role);
                } else {
                    // The current role is OK, so we can make use of
                    // any permission granted to it.
                    //
                    // Look at the current role and, if necessary, the
                    // transitive closure of roles granted to current role to
                    // see if permission has been granted to any of the
                    // applicable roles.

                    RoleClosureIterator rci =
                            dd.createRoleClosureIterator
                                    (activation.getTransactionController(),
                                            role, true /* inverse relation*/);

                    String r;

                    while ((authorization != AUTHORIZED) && (r = rci.next()) != null) {
                        authorization = oneAuthHasPermissionOnSchema
                                (dd, r, forGrant);
                    }

                    if (authorization == AUTHORIZED) {
                        // Also add a dependency on the role (qua provider), so
                        // that if role is no longer available to the current
                        // user (e.g. grant is revoked, role is dropped,
                        // another role has been set), we are able to
                        // invalidate the ps or activation (the latter is used
                        // if the current role changes).
                        DependencyManager dm = dd.getDependencyManager();
                        RoleGrantDescriptor rgd =
                                dd.getRoleDefinitionDescriptor(role);
                        ContextManager cm = lcc.getContextManager();

                        dm.addDependency(ps, rgd, cm);
                        dm.addDependency(activation, rgd, cm);
                        break;
                    }
                }
            }
            lcc.removeRoles(activation, rolesToRemove);
        }
        return authorization == AUTHORIZED;
    }

    protected int oneAuthHasPermissionOnSchema(DataDictionary dd, String authorizationId, boolean forGrant)
            throws StandardException
    {
        SchemaPermsDescriptor perms = dd.getSchemaPermissions( schemaUUID, authorizationId);
        if( perms == null)
            return NONE;

        String priv = null;

        switch( privType)
        {
            case Authorizer.SELECT_PRIV:
            case Authorizer.MIN_SELECT_PRIV:
                priv = perms.getSelectPriv();
                break;
            case Authorizer.UPDATE_PRIV:
                priv = perms.getUpdatePriv();
                break;
            case Authorizer.REFERENCES_PRIV:
                priv = perms.getReferencesPriv();
                break;
            case Authorizer.INSERT_PRIV:
                priv = perms.getInsertPriv();
                break;
            case Authorizer.DELETE_PRIV:
                priv = perms.getDeletePriv();
                break;
            case Authorizer.TRIGGER_PRIV:
                priv = perms.getTriggerPriv();
                break;
            case Authorizer.MODIFY_SCHEMA_PRIV:
                priv = perms.getModifyPriv();
                break;
            case Authorizer.ACCESS_PRIV:
                priv = perms.getAccessPriv();
                break;
            default:
                break;
        }

        return "Y".equals(priv) || (!forGrant) && "y".equals( priv) ?  AUTHORIZED : UNAUTHORIZED;
    } // end of hasPermissionOnTable

    /**
     * Schema level permission is never required as list of privileges required
     * for triggers/constraints/views and hence we don't do any work here, but
     * simply return null
     *
     * @see StatementPermission#check
     */
    public PermissionsDescriptor getPermissionDescriptor(String authid, DataDictionary dd)
    throws StandardException
    {
        return null;
    }

    private String getPrivName( )
    {
        switch(privType) {
        case Authorizer.CREATE_SCHEMA_PRIV:
            return "CREATE_SCHEMA";
        case Authorizer.MODIFY_SCHEMA_PRIV:
            return "MODIFY_SCHEMA";
        case Authorizer.DROP_SCHEMA_PRIV:
            return "DROP_SCHEMA";
        case Authorizer.ACCESS_PRIV:
            return "ACCESS_SCHEMA";
        default:
            return "?";
        }
    }

    public UUID getSchemaUUID() {
        return schemaUUID;
    }

    public int getPrivType() {
        return privType;
    }


    public String toString() {
        return "StatementSchemaPermission: " + schemaName + " UUID: " + schemaUUID + " owner:" +
            aid + " privName:" + getPrivName();
    }

    @Override
    public Type getType() {
        return Type.SCHEMA;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public boolean equals( Object obj)
    {
        if( obj == null)
            return false;
        if( getClass().equals( obj.getClass()))
        {
            StatementSchemaPermission other = (StatementSchemaPermission) obj;
            return privType == other.privType && (schemaUUID !=null && other.schemaUUID != null && schemaUUID.equals(other.schemaUUID));
        }
        return false;
    }

    /**
     * Return hash code for this instance
     *
     * @return    Hashcode
     *
     */
    public int hashCode()
    {
        return privType + (schemaUUID!=null?schemaUUID.hashCode():0);
    }
}
