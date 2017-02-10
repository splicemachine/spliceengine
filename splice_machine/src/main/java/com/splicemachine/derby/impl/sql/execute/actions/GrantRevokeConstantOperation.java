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
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.dictionary.*;
import com.splicemachine.db.iapi.sql.execute.ConstantAction;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.db.impl.sql.execute.GenericPrivilegeInfo;
import com.splicemachine.db.impl.sql.execute.PrivilegeInfo;
import com.splicemachine.ddl.DDLMessage;
import com.splicemachine.derby.ddl.DDLUtils;
import com.splicemachine.derby.impl.store.access.SpliceTransactionManager;
import com.splicemachine.protobuf.ProtoUtil;

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
        LanguageConnectionContext lcc = activation.getLanguageConnectionContext();
        TransactionController tc = lcc.getTransactionExecute();

		List <PermissionsDescriptor> permissionsDescriptors = privileges.executeGrantRevoke( activation, grant, grantees);
		for (PermissionsDescriptor permissionsDescriptor : permissionsDescriptors) {
            DDLMessage.DDLChange ddlChange = createDDLChange(((SpliceTransactionManager) tc).getActiveStateTxn().getTxnId(), permissionsDescriptor);
            tc.prepareDataDictionaryChange(DDLUtils.notifyMetadataChange(ddlChange));
        }
	}

    private DDLMessage.DDLChange  createDDLChange(long txnId, PermissionsDescriptor permissionsDescriptor) {
        if (permissionsDescriptor instanceof SchemaPermsDescriptor) {
            SchemaPermsDescriptor schemaPermsDescriptor = (SchemaPermsDescriptor) permissionsDescriptor;
            return ProtoUtil.createRevokeSchemaPrivilege(txnId, schemaPermsDescriptor);
        }
        else if (permissionsDescriptor instanceof TablePermsDescriptor) {
            TablePermsDescriptor tablePermsDescriptor = (TablePermsDescriptor) permissionsDescriptor;
            return ProtoUtil.createRevokeTablePrivilege(txnId, tablePermsDescriptor);
        }
        else if (permissionsDescriptor instanceof ColPermsDescriptor) {
            ColPermsDescriptor colPermsDescriptor = (ColPermsDescriptor) permissionsDescriptor;
            return ProtoUtil.createRevokeColumnPrivilege(txnId, colPermsDescriptor);
        }
        else if (permissionsDescriptor instanceof RoutinePermsDescriptor) {
            RoutinePermsDescriptor routinePermsDescriptor = (RoutinePermsDescriptor)permissionsDescriptor;
            return ProtoUtil.createRevokeRoutinePrivilege(txnId, routinePermsDescriptor);
        }
        else if (permissionsDescriptor instanceof PermDescriptor) {
            PermDescriptor permDescriptor = (PermDescriptor)permissionsDescriptor;
            boolean restrict = ((GenericPrivilegeInfo)privileges).isRestrict();
            return ProtoUtil.createRevokeGenericPrivilege(txnId, permDescriptor, restrict);
        }

        throw new RuntimeException("Unsupported permission descriptor type");
    }
}

