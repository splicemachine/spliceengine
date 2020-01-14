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
import com.splicemachine.db.iapi.sql.depend.DependencyManager;
import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;
import com.splicemachine.db.iapi.sql.dictionary.RoleClosureIterator;
import com.splicemachine.db.iapi.sql.dictionary.RoleGrantDescriptor;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.db.shared.common.reference.SQLState;
import com.splicemachine.ddl.DDLMessage;
import com.splicemachine.derby.ddl.DDLUtils;
import com.splicemachine.derby.impl.store.access.SpliceTransactionManager;
import com.splicemachine.protobuf.ProtoUtil;
import com.splicemachine.si.impl.driver.SIDriver;

import java.util.Iterator;
import java.util.List;


/**
 *  This class performs actions that are ALWAYS performed for a
 *  REVOKE role statement at execution time.
 *
 */
public class RevokeRoleConstantOperation extends DDLConstantOperation {
    private List roleNames;
    private List grantees;
    private static final boolean withAdminOption = false; // not client.
    /**
     *  Make the ConstantAction for a CREATE ROLE statement.
     *  When executed, will create a role by the given name.
     *
     *  @param roleNames     List of the name of the role names being revoked
     *  @param grantees       List of the authorization ids granted to role
     */
    public RevokeRoleConstantOperation(List roleNames, List grantees) {
        this.roleNames = roleNames;
        this.grantees = grantees;
    }

    // INTERFACE METHODS

    /**
     * This is the guts of the Execution-time logic for REVOKE role.
     *
     * @see com.splicemachine.db.iapi.sql.execute.ConstantAction#executeConstantAction
     */
    public void executeConstantAction(Activation activation) throws StandardException {
        noRevokeCheck();
        LanguageConnectionContext lcc = activation.getLanguageConnectionContext();
        DataDictionary dd = lcc.getDataDictionary();
        TransactionController tc = lcc.getTransactionExecute();
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
                // WITH ADMIN option so we can grant (and hence
                // revoke) it. For database owner, a role definition
                // always fulfills this requirement.  If we implement
                // granting with WITH ADMIN option later, we need to
                // look for a grant to us or to PUBLIC which has WITH
                // ADMIN. The role definition descriptor will not
                // suffice in that case, so we need something like:
                //
                // rd = dd.findRoleGrantWithAdminToRoleOrPublic(grantor)
                // if (rd != null) {
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
                        (SQLState.AUTH_ROLE_DBO_ONLY, "REVOKE role");
                }

                RoleGrantDescriptor rd =
                    dd.getRoleGrantDescriptor(role, grantee);

                if (rd != null && withAdminOption) {
                    // NOTE: Never called yet, withAdminOption not yet
                    // implemented.

                    if (SanityManager.DEBUG) {
                        SanityManager.NOTREACHED();
                    }

                    // revoke only the ADMIN OPTION from grantee
                    //
                    if (rd.isWithAdminOption()) {
                        // Invalidate and remove old descriptor and add a new
                        // one without admin option.
                        //
                        // RoleClosureIterator rci =
                        //     dd.createRoleClosureIterator
                        //     (activation.getTransactionController(),
                        //      role, false);
                        //
                        // String r;
                        // while ((r = rci.next()) != null) {
                        //   rdDef = dd.getRoleDefinitionDescriptor(r);
                        //
                        //   dd.getDependencyManager().invalidateFor
                        //       (rdDef, DependencyManager.REVOKE_ROLE, lcc);
                        // }
                        //
                        // rd.drop(lcc);
                        // rd.setWithAdminOption(false);
                        // dd.addDescriptor(rd,
                        //                  null,  // parent
                        //                  DataDictionary.SYSROLES_CATALOG_NUM,
                        //                  false, // no duplicatesAllowed
                        //                  tc);
                    } else {
                        activation.addWarning
                            (StandardException.newWarning
                             (SQLState.LANG_WITH_ADMIN_OPTION_NOT_REVOKED,
                              role, grantee));
                    }
                } else if (rd != null) {
                    // Normal revoke of role from grantee.
                    //
                    // When a role is revoked, for every role in its grantee
                    // closure, we call the REVOKE_ROLE action. It is used to
                    // invalidate dependent objects (constraints, triggers and
                    // views).  Note that until DERBY-1632 is fixed, we risk
                    // dropping objects not really dependent on this role, but
                    // one some other role just because it inherits from this
                    // one. See also DropRoleConstantAction.
                    RoleClosureIterator rci =
                        dd.createRoleClosureIterator
                        (activation.getTransactionController(),
                         role, false);

                    String r;
                    while ((r = rci.next()) != null) {
                        rdDef = dd.getRoleDefinitionDescriptor(r);

                        dd.getDependencyManager().invalidateFor
                            (rdDef, DependencyManager.REVOKE_ROLE, lcc);
                    }

                    rd.drop(lcc);

                    /* we need to invalidate the defaultRole cache if the grantee's defaultRole list is changed,
                     * also invalidate the roleGrantCache */
                    DDLMessage.DDLChange ddlChange =
                            ProtoUtil.createGrantRevokeRole(((SpliceTransactionManager) tc).getActiveStateTxn().getTxnId(),
                                    role, grantee, grantor, false);
                    tc.prepareDataDictionaryChange(DDLUtils.notifyMetadataChange(ddlChange));

                } else {
                    activation.addWarning
                        (StandardException.newWarning
                         (SQLState.LANG_ROLE_NOT_REVOKED, role, grantee));
                }
            }
        }
    }


    // OBJECT SHADOWS

    public String toString()
    {
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
        return ("REVOKE " +
                sb1.toString() +
                " FROM: " +
                sb2.toString() +
                "\n");
    }
}

