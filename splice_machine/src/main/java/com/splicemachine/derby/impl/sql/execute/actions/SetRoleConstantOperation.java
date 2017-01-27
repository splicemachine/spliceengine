/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
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
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.ParameterValueSet;
import com.splicemachine.db.iapi.sql.StatementType;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.depend.DependencyManager;
import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;
import com.splicemachine.db.iapi.sql.dictionary.RoleGrantDescriptor;
import com.splicemachine.db.iapi.sql.execute.ConstantAction;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.util.IdUtil;
import com.splicemachine.derby.impl.store.access.BaseSpliceTransaction;
import com.splicemachine.derby.impl.store.access.SpliceTransactionManager;
import com.splicemachine.si.impl.txn.ReadOnlyTxn;

public class SetRoleConstantOperation implements ConstantAction {
    protected final String  roleName;
    protected final int     type;
    /**
     * Make the ConstantAction for a SET ROLE statement.
     *
     *  @param roleName Name of role.
     *  @param type     type of set role (literal role name or ?)
     */
    public SetRoleConstantOperation(String roleName, int type) {
        this.roleName = roleName;
        this.type = type;
    }
    
    public String toString() {
        return "SET ROLE " +
            ((type == StatementType.SET_ROLE_DYNAMIC && roleName == null) ?
             "?" : roleName);
    }

    /**
     *  This is the guts of the Execution-time logic for SET ROLE.
     *
     *  @see ConstantAction#executeConstantAction
     *
     * @exception StandardException     Thrown on failure
     */
    public void executeConstantAction( Activation activation ) throws StandardException {
        LanguageConnectionContext lcc = activation.getLanguageConnectionContext();
        DataDictionary dd = lcc.getDataDictionary();
        String thisRoleName = roleName;


        TransactionController tc = lcc.getTransactionExecute();

        // SQL 2003, section 18.3, General rule 1:
        BaseSpliceTransaction txn = ((SpliceTransactionManager) tc).getRawTransaction();
        if (!(txn.getTxnInformation() instanceof ReadOnlyTxn)) {
            throw StandardException.newException
                (SQLState.INVALID_TRANSACTION_STATE_ACTIVE_CONNECTION);
        }

        if (type == StatementType.SET_ROLE_DYNAMIC) {
            ParameterValueSet pvs = activation.getParameterValueSet();
            DataValueDescriptor dvs = pvs.getParameter(0);
            // SQL 2003, section 18.3, GR2: trim whitespace first, and
            // interpret as identifier, then we convert it to case normal form
            // here.
            String roleId = dvs.getString();

            if (roleId == null) {
                throw StandardException.newException(SQLState.ID_PARSE_ERROR);
            }

            thisRoleName = IdUtil.parseRoleId(roleId);
        }

        RoleGrantDescriptor rdDef = null;

        try {
            String oldRole = lcc.getCurrentRoleId(activation);

            if (oldRole != null && !oldRole.equals(thisRoleName)) {
                rdDef = dd.getRoleDefinitionDescriptor(oldRole);

                if (rdDef != null) {
                    dd.getDependencyManager().invalidateFor(
                        rdDef,
                        DependencyManager.RECHECK_PRIVILEGES,
                        lcc);
                } // else: old role else no longer exists, so ignore.
            }

            if (thisRoleName != null) {
                rdDef = dd.getRoleDefinitionDescriptor(thisRoleName);

                // SQL 2003, section 18.3, General rule 4:
                if (rdDef == null) {
                    throw StandardException.newException
                        (SQLState.ROLE_INVALID_SPECIFICATION, thisRoleName);
                }

                if (!lcc.roleIsSettable(activation, thisRoleName)) {
                    throw StandardException.newException
                              (SQLState. ROLE_INVALID_SPECIFICATION_NOT_GRANTED,
                               thisRoleName);
                }
            }
        } finally {
            // reading above changes idle state, so reestablish it
            lcc.userCommit();
        }

        lcc.setCurrentRole(activation, rdDef != null ? thisRoleName : null);
    }
}
