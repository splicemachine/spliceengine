/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.splicemachine.derby.impl.sql.execute.actions;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.depend.DependencyManager;
import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;
import com.splicemachine.db.iapi.sql.dictionary.RoleGrantDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.RoleClosureIterator;
import com.splicemachine.db.shared.common.reference.SQLState;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.ddl.DDLMessage;
import com.splicemachine.derby.ddl.DDLUtils;
import com.splicemachine.derby.impl.store.access.SpliceTransactionManager;
import com.splicemachine.protobuf.ProtoUtil;

/**
 *  This class  describes actions that are ALWAYS performed for a
 *  DROP ROLE Statement at Execution time.
 *
 */

public class DropRoleConstantOperation extends DDLConstantOperation {
    private final String roleName;
    /**
     *  Make the ConstantAction for a DROP ROLE statement.
     *
     *  @param  roleName  role name to be dropped
     *
     */
    public DropRoleConstantOperation(String roleName) {
        this.roleName = roleName;
    }

    public String toString() {
        return "DROP ROLE " + roleName;
    }

    /**
     * This is the guts of the Execution-time logic for DROP ROLE.
     *
     * @see com.splicemachine.db.iapi.sql.execute.ConstantAction#executeConstantAction
     */
    public void executeConstantAction( Activation activation ) throws StandardException {
        LanguageConnectionContext lcc = activation.getLanguageConnectionContext();
        DataDictionary dd = lcc.getDataDictionary();
        TransactionController tc = lcc.getTransactionExecute();
        /*
        ** Inform the data dictionary that we are about to write to it.
        ** There are several calls to data dictionary "get" methods here
        ** that might be done in "read" mode in the data dictionary, but
        ** it seemed safer to do this whole operation in "write" mode.
        **
        ** We tell the data dictionary we're done writing at the end of
        ** the transaction.
        */
        dd.startWriting(lcc);

        RoleGrantDescriptor rdDef = dd.getRoleDefinitionDescriptor(roleName);

        if (rdDef == null) {
            throw StandardException.newException(
                SQLState.ROLE_INVALID_SPECIFICATION, roleName);
        }

        // When a role is dropped, for every role in its grantee closure, we
        // call the REVOKE_ROLE action. It is used to invalidate dependent
        // objects (constraints, triggers and views).  Note that until
        // DERBY-1632 is fixed, we risk dropping objects not really dependent
        // on this role, but one some other role just because it inherits from
        // this one. See also RevokeRoleConstantAction.
        RoleClosureIterator rci =
            dd.createRoleClosureIterator
            (activation.getTransactionController(),
             roleName, false);

        String role;
        while ((role = rci.next()) != null) {
            RoleGrantDescriptor r = dd.getRoleDefinitionDescriptor(role);

            dd.getDependencyManager().invalidateFor
                (r, DependencyManager.REVOKE_ROLE, lcc);
        }

        rdDef.drop(lcc);

        DDLMessage.DDLChange change = ProtoUtil.createDropRole(((SpliceTransactionManager) tc).getActiveStateTxn().getTxnId(), roleName);
        tc.prepareDataDictionaryChange(DDLUtils.notifyMetadataChange(change));

        /*
         * We dropped a role, now drop all dependents:
         * - role grants to this role
         * - grants of this role to other roles or users
         * - privilege grants to this role
         */

        dd.dropRoleGrantsByGrantee(roleName, tc);
        dd.dropRoleGrantsByName(roleName, tc);
        dd.dropAllPermsByGrantee(roleName, tc);
    }
}
