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

package com.splicemachine.db.impl.tools.dblook;

import java.sql.Connection;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.splicemachine.db.tools.dblook;

public class DB_Roles {

    /**
     * Generate role definition statements and role grant statements. Note that
     * privileges granted to roles are handled by DB_GrantRevoke, similar to
     * privileges granted to users.
     *
     * @param conn Connection to use
     */
    public static void doRoles(Connection conn)
        throws SQLException {

        // First generate role definition statements
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery
            ("SELECT ROLEID, GRANTEE, GRANTOR, " +
             "WITHADMINOPTION FROM SYS.SYSROLES WHERE ISDEF = 'Y'");
        generateRoleDefinitions(rs);
        rs.close();

        // Generate role grant statements
        rs = stmt.executeQuery
            ("SELECT ROLEID, GRANTEE, GRANTOR, WITHADMINOPTION" +
             " FROM SYS.SYSROLES WHERE ISDEF = 'N'");
        generateRoleGrants(rs);

        rs.close();
        stmt.close();
        return;

    }

    /**
     * Generate role definition statements
     *
     * @param rs Result set holding required information
     */
    private static void generateRoleDefinitions(ResultSet rs)
        throws SQLException
    {
        boolean firstTime = true;
        while (rs.next()) {

            if (firstTime) {
                Logs.reportString
                    ("----------------------------------------------");
                Logs.reportMessage( "DBLOOK_Role_definitions_header");
                Logs.reportString
                    ("----------------------------------------------\n");
            }

            String roleName = dblook.addQuotes
                (dblook.expandDoubleQuotes(rs.getString(1)));
            // String grantee = dblook.addQuotes
            //     (dblook.expandDoubleQuotes(rs.getString(2))); // always DBO
            // String grantor = dblook.addQuotes
            //   (dblook.expandDoubleQuotes(rs.getString(3))); // always _SYSTEM
            // boolean isWithAdminOption = rs.getString
            //   (4).equals("Y") ? true : false; // always true for a definition

            Logs.writeToNewDDL(roleDefinitionStatement(rs, roleName));
            Logs.writeStmtEndToNewDDL();
            Logs.writeNewlineToNewDDL();
            firstTime = false;
        }
    }

    /**
     * Generate a role definition statement for the current row
     *
     * @param rs        @{code ResultSet} holding role definition information
     * @param roleName  The role defined, already quoted
     */
    private static String roleDefinitionStatement(ResultSet rs, String roleName)
        throws SQLException
    {
        StringBuffer createStmt = new StringBuffer("CREATE ROLE ");

        createStmt.append(roleName);
        return createStmt.toString();
    }

    private static void generateRoleGrants(ResultSet rs)
        throws SQLException
    {
        boolean firstTime = true;
        while (rs.next()) {

            if (firstTime) {
                firstTime = false;

                Logs.reportString
                    ("----------------------------------------------");
                Logs.reportMessage( "DBLOOK_Role_grants_header");
                Logs.reportString
                    ("----------------------------------------------\n");
            }

            String roleName = dblook.addQuotes
                (dblook.expandDoubleQuotes(rs.getString(1)));
            String grantee = dblook.addQuotes
                (dblook.expandDoubleQuotes(rs.getString(2)));
            String grantor = dblook.addQuotes
                (dblook.expandDoubleQuotes(rs.getString(3))); // always DBO
            boolean isWithAdminOption =
                rs.getString(4).equals("Y") ? true : false;

            Logs.writeToNewDDL
                (roleGrantStatement(rs, roleName, grantee, isWithAdminOption));
            Logs.writeStmtEndToNewDDL();
            Logs.writeNewlineToNewDDL();
        }
    }

    /**
     * Generate role grant statement for the current row
     *
     * @param rs        @{ResultSet} holding role grant information
     * @param roleName  The role granted, already quoted
     * @param grantee   The authorization id to whom the role is granted (a role
     *                  or a user), already quoted
     * @param isWithAdminOption @{code true} if ADMIN OPTION was used for the
     *         grant
     */
    private static String roleGrantStatement(ResultSet rs,
                                             String roleName,
                                             String grantee,
                                             boolean isWithAdminOption)
        throws SQLException
    {
        StringBuffer createStmt = new StringBuffer("GRANT ");

        createStmt.append(roleName);
        createStmt.append(" TO ");
        createStmt.append(grantee);

        if (isWithAdminOption) {
            createStmt.append(" WITH ADMIN OPTION");
        }

        return createStmt.toString();
    }

}
