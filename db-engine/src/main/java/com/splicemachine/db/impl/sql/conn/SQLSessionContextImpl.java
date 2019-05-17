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
 * All such Splice Machine modifications are Copyright 2012 - 2019 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */

package com.splicemachine.db.impl.sql.conn;

import com.splicemachine.db.iapi.sql.conn.SQLSessionContext;
import com.splicemachine.db.iapi.sql.dictionary.SchemaDescriptor;

import java.util.ArrayList;
import java.util.List;

public class SQLSessionContextImpl implements SQLSessionContext {

    private String currentUser;
    private ArrayList<String> currentRoles = new ArrayList<>();
    private SchemaDescriptor currentDefaultSchema;
    private List<String> groupuserlist;

    public SQLSessionContextImpl (SchemaDescriptor sd, String currentUser, List<String> roles, List<String> groupuserlist) {
        if (roles != null)
            currentRoles.addAll(roles);
        currentDefaultSchema = sd;
        this.currentUser = currentUser;
        this.groupuserlist = groupuserlist;
    }

    public void setRole(String role) {

        if (role == null) {
            currentRoles.clear();
        } else {
            // check if it already exists
            for (String aRole : currentRoles) {
                if (aRole.equals(role))
                    return;
            }
            currentRoles.add(role);
        }
    }

    public void removeRole(String roleName) {
        int i;
        for (i=0; i< currentRoles.size(); i++) {
            String aRole = currentRoles.get(i);
            if (aRole.equals(roleName)) {
                break;
            }
        }
        if (i < currentRoles.size())
            currentRoles.remove(i);
        return;
    }

    public List<String> getRoles() {
        return currentRoles;
    }

    public void setRoles(List<String> roles) {
        currentRoles.clear();
        if (roles != null)
            currentRoles.addAll(roles);
    }

    public void setUser(String user) {
        currentUser = user;
    }

    public String getCurrentUser() {
        return currentUser;
    }

    public void setDefaultSchema(SchemaDescriptor sd) {
        currentDefaultSchema = sd;
    }

    public SchemaDescriptor getDefaultSchema() {
        return currentDefaultSchema;
    }

    public List<String> getCurrentGroupUser() { return groupuserlist; }

    public void setCurrentGroupUser(List<String> groupUsers) {
        groupuserlist = groupUsers;
    }
}
