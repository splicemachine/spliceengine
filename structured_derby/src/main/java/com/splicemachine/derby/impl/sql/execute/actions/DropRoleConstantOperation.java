package com.splicemachine.derby.impl.sql.execute.actions;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.depend.DependencyManager;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.RoleGrantDescriptor;
import org.apache.derby.iapi.sql.dictionary.RoleClosureIterator;
import org.apache.derby.shared.common.reference.SQLState;
import org.apache.derby.iapi.store.access.TransactionController;

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
     * @see org.apache.derby.iapi.sql.execute.ConstantAction#executeConstantAction
     */
    public void executeTransactionalConstantAction( Activation activation ) throws StandardException {
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
