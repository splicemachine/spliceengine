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
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.conn.Authorizer;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.dictionary.DataDescriptorGenerator;
import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;
import com.splicemachine.db.iapi.sql.dictionary.RoleClosureIterator;
import com.splicemachine.db.iapi.sql.dictionary.RoleGrantDescriptor;
import com.splicemachine.db.iapi.sql.execute.ConstantAction;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.db.shared.common.reference.SQLState;
import com.splicemachine.ddl.DDLMessage;
import com.splicemachine.derby.ddl.DDLUtils;
import com.splicemachine.derby.impl.store.access.SpliceTransactionManager;
import com.splicemachine.protobuf.ProtoUtil;
import com.splicemachine.si.impl.driver.SIDriver;

import java.util.Iterator;
import java.util.List;

public class GrantRoleConstantOperation extends DDLConstantOperation {
    private List roleNames;
    private List grantees;
    private boolean isDefaultRole = false;
    private static final boolean withAdminOption = false; // not client.
    /**
     *  Make the ConstantAction for a CREATE ROLE statement.
     *  When executed, will create a role by the given name.
     *
     *  @param roleNames     List of the names of the roles being granted
     *  @param grantees       List of the authorization ids granted to role
     */
    public GrantRoleConstantOperation(List roleNames, List grantees, boolean isDefaultRole) {
        this.roleNames = roleNames;
        this.grantees = grantees;
        this.isDefaultRole = isDefaultRole;
    }

    /**
     *  This is the guts of the Execution-time logic for GRANT role.
     *
     *  @see ConstantAction#executeConstantAction
     *
     * @exception StandardException     Thrown on failure
     */
    public void executeConstantAction(Activation activation) throws StandardException {
        noGrantCheck();
       LanguageConnectionContext lcc = activation.getLanguageConnectionContext();
        DataDictionary dd = lcc.getDataDictionary();
        TransactionController tc = lcc.getTransactionExecute();
        DataDescriptorGenerator ddg = dd.getDataDescriptorGenerator();
        final String grantor = lcc.getCurrentUserId(activation);
        final List<String> groupuserlist = lcc.getCurrentGroupUser(activation);
        String dbo = lcc.getDataDictionary().getAuthorizationDatabaseOwner();

        dd.startWriting(lcc);
        for (Iterator rIter = roleNames.iterator(); rIter.hasNext();) {
            String role = (String)rIter.next();

            if (role.equals(Authorizer.PUBLIC_AUTHORIZATION_ID)) {
                throw StandardException.
                    newException(SQLState.AUTH_PUBLIC_ILLEGAL_AUTHORIZATION_ID);
            }

            for (Iterator gIter = grantees.iterator(); gIter.hasNext();) {
                String grantee = (String)gIter.next();

                // check that role exists
                RoleGrantDescriptor rdDef =
                    dd.getRoleDefinitionDescriptor(role);

                if (rdDef == null) {
                    throw StandardException.
                        newException(SQLState.ROLE_INVALID_SPECIFICATION, role);
                }

                // Check that role is granted to us (or PUBLIC) with
                // WITH ADMIN option so we can grant it. For database
                // owner, a role definition always fulfills this
                // requirement.  If we implement granting with WITH ADMIN
                // option later, we need to look for a grant to us (or
                // PUBLIC) which has WITH ADMIN. The role definition
                // descriptor will not suffice in that case, so we
                // need something like:
                //
                // rdDef = dd.findRoleGrantWithAdminToRoleOrPublic(grantor)
                // if (rdDef != null) {
                //   :
                if (grantor.equals(dbo)
                        || (groupuserlist != null && groupuserlist.contains(dbo))) {
                    // All ok, we are database owner
                    if (SanityManager.DEBUG) {
                        // When there is an LDAP admin group mapped to splice(DB-6636), role may be created by a user different
                        // from grantor but belonging to the admin LDAP group, the below check could be violated
                        // so comment it out
                        //SanityManager.ASSERT(
                        //    rdDef.getGrantee().equals(grantor) || rdDef.getGrantee().equals(dbo),
                        //    "expected database owner in role grant descriptor");
                        SanityManager.ASSERT(
                            rdDef.isWithAdminOption(),
                            "expected role definition to have ADMIN OPTION");
                    }
                } else {
                    throw StandardException.newException
                        (SQLState.AUTH_ROLE_DBO_ONLY, "GRANT role");
                }

                // Has it already been granted?
                RoleGrantDescriptor rgd =
                    dd.getRoleGrantDescriptor(role, grantee);

                if (rgd != null &&
                        withAdminOption && !rgd.isWithAdminOption()) {

                    // NOTE: Never called yet, withAdminOption not yet
                    // implemented.

                    // Remove old descriptor and add a new one with admin
                    // option: cf. SQL 2003, section 12.5, general rule 3
                    rgd.drop(lcc);
                    rgd.setWithAdminOption(true);
                    dd.addDescriptor(rgd,
                                     null,  // parent
                                     DataDictionary.SYSROLES_CATALOG_NUM,
                                     false, // no duplicatesAllowed
                                     tc, false);
                } else if (rgd!= null && (isDefaultRole && !rgd.isDefaultRole() || !isDefaultRole && rgd.isDefaultRole())) {
                    rgd.drop(lcc);
                    rgd.setDefaultRole(isDefaultRole);
                    dd.addDescriptor(rgd,
                            null,  // parent
                            DataDictionary.SYSROLES_CATALOG_NUM,
                            false, // no duplicatesAllowed
                            tc, false);
                    /* we need to invalidate the defaultRole cache as the grantee's defaultRole list has changed;
                       also we need to invalidate the roleGrant cache
                     */
                    DDLMessage.DDLChange ddlChange =
                            ProtoUtil.createGrantRevokeRole(((SpliceTransactionManager) tc).getActiveStateTxn().getTxnId(),
                                    role, grantee, grantor, true);
                    tc.prepareDataDictionaryChange(DDLUtils.notifyMetadataChange(ddlChange));
                } else if (rgd == null) {
                    // Check if the grantee is a role (if not, it is a user)
                    RoleGrantDescriptor granteeDef =
                        dd.getRoleDefinitionDescriptor(grantee);

                    if (granteeDef != null) {
                        checkCircularity(role, grantee, tc, dd);
                    }

                    rgd = ddg.newRoleGrantDescriptor(
                        dd.getUUIDFactory().createUUID(),
                        role,
                        grantee,
                        grantor, // dbo for now
                        withAdminOption,
                        false,
                        isDefaultRole);  // not definition
                    dd.addDescriptor(
                        rgd,
                        null,  // parent
                        DataDictionary.SYSROLES_CATALOG_NUM,
                        false, // no duplicatesAllowed
                        tc, false);

                    /* we need to invalidate the defaultRole cache as the grantee's defaultRole list has changed;
                       also we need to invalidate the roleGrant cache
                     */
                    DDLMessage.DDLChange ddlChange =
                            ProtoUtil.createGrantRevokeRole(((SpliceTransactionManager) tc).getActiveStateTxn().getTxnId(),
                                    role, grantee, grantor, true);
                    tc.prepareDataDictionaryChange(DDLUtils.notifyMetadataChange(ddlChange));

                } // else exists already, no need to add
            }
        }
    }

    /**
     * Check that allowing this grant to go ahead does nto create a
     * circularity in the GRANT role relation graph, cf. Section 12.5,
     * Syntax rule 1 of ISO/IEC 9075-2 2003.
     *
     * @param role The role about to be granted
     * @param grantee The role to which {@code role} is to be granted
     * @throws StandardException normal error policy. Throws
     *                           AUTH_ROLE_GRANT_CIRCULARITY if a
     *                           circularity is detected.
     */
    private void checkCircularity(String role,
                                  String grantee,
                                  TransactionController tc,
                                  DataDictionary dd)
            throws StandardException {

        // The grantee is role, not a user id, so we need to check for
        // circularity. If there exists a grant back to the role being
        // granted now, from one of the roles in the grant closure of
        // grantee, there is a circularity.

        // Via grant closure of grantee
        RoleClosureIterator rci =
            dd.createRoleClosureIterator(tc, grantee, false);

        String r;
        while ((r = rci.next()) != null) {
            if (role.equals(r)) {
                throw StandardException.newException
                    (SQLState.AUTH_ROLE_GRANT_CIRCULARITY,
                     role, grantee);
            }
        }
    }

    public  String  toString() {
        // Do not put this under SanityManager.DEBUG - it is needed for
        // error reporting.
        StringBuilder sb1 = new StringBuilder();
        for (Iterator it = roleNames.iterator(); it.hasNext();) {
            if( sb1.length() > 0) {
                sb1.append( ", ");
            }
            sb1.append( it.next().toString());
        }

        StringBuilder sb2 = new StringBuilder();
        for (Iterator it = grantees.iterator(); it.hasNext();) {
            if( sb2.length() > 0) {
                sb2.append( ", ");
            }
            sb2.append( it.next().toString());
        }
        return ("GRANT " +
                sb1.toString() +
                " TO: " +
                sb2.toString() + (isDefaultRole?" AS DEFAULT":"") +
                "\n");
    }
}

