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
 * All such Splice Machine modifications are Copyright 2012 - 2017 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */

package com.splicemachine.db.impl.sql.compile;

import com.splicemachine.db.iapi.sql.execute.ConstantAction;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.error.StandardException;

import java.util.Iterator;
import java.util.List;

/**
 * This class represents a GRANT role statement.
 */
public class GrantRoleNode extends DDLStatementNode
{
    private List roles;
    private List grantees;

    /**
     * Initialize a GrantRoleNode.
     *
     * @param roles list of strings containing role name to be granted
     * @param grantees list of strings containing grantee names
     */
    public void init(Object roles,
					 Object grantees)
        throws StandardException
    {
        initAndCheck(null);
        this.roles = (List) roles;
        this.grantees = (List) grantees;
    }


    /**
     * Create the Constant information that will drive the guts of Execution.
     *
     * @exception StandardException Standard error policy.
     */
    public ConstantAction makeConstantAction() throws StandardException
    {
        return getGenericConstantActionFactory().
            getGrantRoleConstantAction( roles, grantees);
    }


    /**
     * Convert this object to a String.  See comments in QueryTreeNode.java
     * for how this should be done for tree printing.
     *
     * @return  This object as a String
     */

    public String toString()
    {
        if (SanityManager.DEBUG) {
                StringBuffer sb1 = new StringBuffer();
                for( Iterator it = roles.iterator(); it.hasNext();) {
					if( sb1.length() > 0) {
						sb1.append( ", ");
					}
					sb1.append( it.next().toString());
				}

                StringBuffer sb2 = new StringBuffer();
                for( Iterator it = grantees.iterator(); it.hasNext();) {
					if( sb2.length() > 0) {
						sb2.append( ", ");
					}
					sb2.append( it.next().toString());
				}
                return (super.toString() +
                        sb1.toString() +
                        " TO: " +
                        sb2.toString() +
                        "\n");
		} else {
			return "";
		}
    } // end of toString

    public String statementToString()
    {
        return "GRANT role";
    }
}
