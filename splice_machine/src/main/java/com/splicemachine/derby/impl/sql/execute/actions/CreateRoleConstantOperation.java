/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.derby.impl.sql.execute.actions;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.jdbc.AuthenticationService;
import com.splicemachine.db.iapi.services.property.PropertyUtil;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.conn.Authorizer;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.dictionary.DataDescriptorGenerator;
import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;
import com.splicemachine.db.iapi.sql.dictionary.RoleGrantDescriptor;
import com.splicemachine.db.iapi.sql.execute.ConstantAction;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.db.impl.jdbc.authentication.BasicAuthenticationServiceImpl;
import com.splicemachine.db.shared.common.reference.SQLState;
import com.splicemachine.ddl.DDLMessage;
import com.splicemachine.derby.ddl.DDLUtils;
import com.splicemachine.derby.impl.store.access.SpliceTransactionManager;
import com.splicemachine.protobuf.ProtoUtil;
import org.apache.log4j.Logger;

import com.splicemachine.utils.SpliceLogUtils;

public class CreateRoleConstantOperation extends DDLConstantOperation {
    private static final Logger LOG = Logger.getLogger(CreateRoleConstantOperation.class);

    private String roleName;
    /**
     *  Make the ConstantAction for a CREATE ROLE statement.
     *  When executed, will create a role by the given name.
     *
     *  @param roleName     The name of the role being created
     */
    public CreateRoleConstantOperation(String roleName) {
    	SpliceLogUtils.trace(LOG, "CreateRoleConstantOperation with role name {%s}",roleName);
        this.roleName = roleName;
    }

    /**
     *  This is the guts of the Execution-time logic for CREATE ROLE.
     *
     *  @see ConstantAction#executeConstantAction
     *
     * @exception StandardException     Thrown on failure
     */
    public void executeConstantAction(Activation activation) throws StandardException {
    	SpliceLogUtils.trace(LOG, "executeConstantAction with activation {%s}",activation);
        noGrantCheck();
        LanguageConnectionContext lcc = activation.getLanguageConnectionContext();
        DataDictionary dd = lcc.getDataDictionary();
        TransactionController tc = lcc.getTransactionExecute();
        DataDescriptorGenerator ddg = dd.getDataDescriptorGenerator();
        if (roleName.equals(Authorizer.PUBLIC_AUTHORIZATION_ID)) {
            throw StandardException.newException(SQLState.AUTH_PUBLIC_ILLEGAL_AUTHORIZATION_ID);
        }

        // currentAuthId is currently always the database owner since
        // role definition is a database owner power. This may change
        // in the future since this SQL is more liberal.
        //
        final String currentAuthId = lcc.getCurrentUserId(activation);

        /*
         * We don't need to set the ddl mode here, because we don't need to do 2-phase commit
         * for create role statements
         */
        dd.startWriting(lcc,false);

        //
        // Check if this role already exists. If it does, throw.
        //
        RoleGrantDescriptor rdDef = dd.getRoleDefinitionDescriptor(roleName);

        if (rdDef != null) {
            throw StandardException.
                newException(SQLState.LANG_OBJECT_ALREADY_EXISTS,
                             rdDef.getDescriptorType(), roleName);
        }

        // Check if the proposed role id exists as a user id in
        // a privilege grant or as a built-in user ("best effort"; we
        // can't guarantee against collision if users are externally
        // defined or added later).
        if (knownUser(roleName, currentAuthId, lcc, dd, tc)) {
            throw StandardException.
                newException(SQLState.LANG_OBJECT_ALREADY_EXISTS,
                             "User", roleName);
        }

        DDLMessage.DDLChange change = ProtoUtil.createAddRole(((SpliceTransactionManager) tc).getActiveStateTxn().getTxnId(), roleName);
        tc.prepareDataDictionaryChange(DDLUtils.notifyMetadataChange(change));


        rdDef = ddg.newRoleGrantDescriptor(
            dd.getUUIDFactory().createUUID(),
            roleName,
            currentAuthId,// grantee
            Authorizer.SYSTEM_AUTHORIZATION_ID,// grantor
            true,         // with admin option
            true,
            false);        // is definition

        dd.addDescriptor(rdDef,
                         null,  // parent
                         DataDictionary.SYSROLES_CATALOG_NUM,
                         false, // duplicatesAllowed
                         tc, false);
    }


    // OBJECT SHADOWS

    public String toString()
    {
        // Do not put this under SanityManager.DEBUG - it is needed for
        // error reporting.
        return "CREATE ROLE " + roleName;
    }

    // PRIVATE METHODS

    /**
     * Heuristically, try to determine is a proposed role identifier
     * is already known to Derby as a user name. Method: If BUILTIN
     * authentication is used, check if there is such a user. If
     * external authentication is used, we lose.  If there turns out
     * to be collision, and we can't detect it here, we should block
     * such a user from connecting (FIXME), since there is now a role
     * with that name.
     */
    private boolean knownUser(String roleName,
                              String currentUser,
                              LanguageConnectionContext lcc,
                              DataDictionary dd,
                              TransactionController tc)
            throws StandardException {
    	SpliceLogUtils.trace(LOG, "knownUser called with role %s and currentUser %s",roleName, currentUser);
        //
        AuthenticationService s = lcc.getDatabase().getAuthenticationService();

        if (currentUser.equals(roleName)) {
            return true;
        }

        if (s instanceof BasicAuthenticationServiceImpl) {
            // Derby builtin authentication

            if (PropertyUtil.existsBuiltinUser(tc,roleName)) {
                return true;
            }
        } else {
            // Does LDAP  offer a way to ask if a user exists?
            // User supplied authentication?
            // See DERBY-866. Would be nice to have a dictionary table of users
            // synchronized against external authentication providers.
        }

        // Goto through all grants to see if there is a grant to an
        // authorization identifier which is not a role (hence, it
        // must be a user).
        if (dd.existsGrantToAuthid(roleName, tc)) {
            return true;
        }

        // Go through all schemas to see if any one of them is owned by a authid
        // the same as the proposed roleName.
        return dd.existsSchemaOwnedBy(roleName, tc);

    }
}

