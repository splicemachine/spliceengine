package com.splicemachine.derby.impl.sql.execute.actions;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.execute.ConstantAction;
import com.splicemachine.db.impl.sql.execute.PrivilegeInfo;
import java.util.List;

public class GrantRevokeConstantOperation implements ConstantAction {
	private boolean grant;
	private PrivilegeInfo privileges;
	private List grantees;
	public GrantRevokeConstantOperation( boolean grant, PrivilegeInfo privileges, List grantees) {
		this.grant = grant;
		this.privileges = privileges;
		this.grantees = grantees;
	}

	public	String	toString() {
		return grant ? "GRANT" : "REVOKE";
	}


	/**
	 *	This is the guts of the Execution-time logic for GRANT/REVOKE
	 *
	 *	See ConstantAction#executeConstantAction
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public void executeConstantAction( Activation activation ) throws StandardException {
		privileges.executeGrantRevoke( activation, grant, grantees);
	}
}

