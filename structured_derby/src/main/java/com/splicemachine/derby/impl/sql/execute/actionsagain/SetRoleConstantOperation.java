package com.splicemachine.derby.impl.sql.execute.actionsagain;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.ParameterValueSet;
import org.apache.derby.iapi.sql.StatementType;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.depend.DependencyManager;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.RoleGrantDescriptor;
import org.apache.derby.iapi.sql.execute.ConstantAction;
import org.apache.derby.iapi.store.access.TransactionController;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.util.IdUtil;

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

        final String currentAuthId = lcc.getCurrentUserId(activation);
        final String dbo = lcc.getDataDictionary().getAuthorizationDatabaseOwner();

        TransactionController tc = lcc.getTransactionExecute();

        // SQL 2003, section 18.3, General rule 1:
        if (!tc.isIdle()) {
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
