/*
 * ddlUtils is a subproject of the Apache DB project, and is licensed under
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
 * Splice Machine, Inc. has modified this file.
 *
 * All Splice Machine modifications are Copyright 2012 - 2016 Splice Machine, Inc.,
 * and are licensed to you under the License; you may not use this file except in
 * compliance with the License.
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package org.apache.ddlutils.platform.splicemachine;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.ddlutils.Platform;
import org.apache.ddlutils.model.Database;
import org.apache.ddlutils.model.Permission;
import org.apache.ddlutils.model.Role;
import org.apache.ddlutils.model.User;
import org.apache.ddlutils.platform.derby.DerbyModelReader;

/**
 * Reads a database model from a Splice Machine database.
 */
public class SpliceModelReader extends DerbyModelReader {
    /**
     * Creates a new model reader for Splice Machine databases.
     *
     * @param platform The platform that this model reader belongs to
     */
    public SpliceModelReader(Platform platform) {
        super(platform);
    }

    private final static String USER_QUERY = "select USERNAME from SYS.SYSUSERS";
    protected void addUsers(Database db) throws SQLException {
        // TODO: JC - User pwd?
        try (Statement st = _connection.createStatement()) {
            try (ResultSet rs = st.executeQuery(USER_QUERY)) {
                while (rs.next()) {
                    db.addUser(new User(rs.getString("USERNAME"), "".toCharArray()));
                }
            }
        }
    }

    private final static String ROLE_QUERY = "select ROLEID, GRANTEE, GRANTOR from SYS.SYSROLES";
    protected void addRoles(Database db) throws SQLException {
        // we're expecting all users to have been found and added to model
        try (Statement st = _connection.createStatement()) {
            try (ResultSet rs = st.executeQuery(ROLE_QUERY)) {
                while (rs.next()) {
                    Role role = db.findRole( rs.getString("ROLEID"));
                    if (role == null) {
                        role = new Role( rs.getString("ROLEID"));
                        db.addRole(role);
                    }
                    if (! User.ADMIN_USERNAME.equals(rs.getString("GRANTEE").toUpperCase())) {
                        role.addGrantPair(rs.getString("GRANTEE"), rs.getString("GRANTOR"));
                    }
                }
            }
        }
    }

    private final static String PERM_QUERY = "select a.tablepermsid, a.GRANTEE, a.GRANTOR, a.SELECTPRIV, a.DELETEPRIV, " +
        "a.INSERTPRIV, a.UPDATEPRIV, a.REFERENCESPRIV, a.TRIGGERPRIV, c.schemaname, b.tablename from sys.SYSTABLEPERMS a, " +
        "sys.SYSTABLES b, sys.sysschemas c where a.tableID = b.tableID and b.schemaid = c.schemaid";
    protected void addPermissions(Database db) throws SQLException {
        // we're expecting all users and roles to have been found and added to model
        try (Statement st = _connection.createStatement()) {
            try (ResultSet rs = st.executeQuery(PERM_QUERY)) {
                while (rs.next()) {
                    Permission permission = db.findPermission(rs.getString("TABLEPERMSID"));
                    if (permission == null) {
                        permission = new Permission(rs.getString("TABLEPERMSID"), rs.getString("SCHEMANAME")+
                            "."+rs.getString("TABLENAME"), Permission.ResourceType.TABLE);
                        db.addPermission(permission);
                    }
                    if (! User.ADMIN_USERNAME.equals(rs.getString("GRANTEE").toUpperCase())) {
                        permission.addGrantPair(rs.getString("GRANTEE"), rs.getString("GRANTOR"));
                    }

                    for (Permission.Action action : Permission.Action.allActions()) {
                        if (rs.getObject(action.toString()).equals("y")) {
                            permission.addAction(action);
                        }
                    }
                }
            }
        }
    }
}
