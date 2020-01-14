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
 * All such Splice Machine modifications are Copyright 2012 - 2020 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */

package com.splicemachine.db.impl.tools.dblook;

import java.sql.Connection;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.splicemachine.db.tools.dblook;

public class DB_View {

	/* ************************************************
	 * Generate the DDL for all views in a given
	 * database.
	 * @param conn Connection to the source database.
	 * @return The DDL for the views has been written
	 *  to output via Logs.java.
	 ****/

	public static void doViews(Connection conn)
		throws SQLException {

		Statement stmt = conn.createStatement();
		ResultSet rs = stmt.executeQuery("SELECT V.VIEWDEFINITION, " +
			"T.TABLENAME, T.SCHEMAID, V.COMPILATIONSCHEMAID FROM SYS.SYSVIEWS V, " +
			"SYS.SYSTABLES T WHERE T.TABLEID = V.TABLEID");

		boolean firstTime = true;
		while (rs.next()) {

			String viewSchema = dblook.lookupSchemaId(rs.getString(3));
			if (dblook.isIgnorableSchema(viewSchema))
				continue;

			if (!dblook.stringContainsTargetTable(rs.getString(1)))
				continue;

			if (firstTime) {
				Logs.reportString("----------------------------------------------");
				Logs.reportMessage("DBLOOK_ViewsHeader");
				Logs.reportString("----------------------------------------------\n");
			}

			// We are using the exact text that was entered by the user,
			// which means the view name that is given might not include
			// the schema in which the view was created.  So, we change
			// our schema to be the one in which the view was created
			// before we execute the create statement.
			Logs.writeToNewDDL("SET SCHEMA ");
			Logs.writeToNewDDL(dblook.lookupSchemaId(rs.getString(4)));
			Logs.writeStmtEndToNewDDL();

			// Now, go ahead and create the view.
			Logs.writeToNewDDL(dblook.removeNewlines(rs.getString(1)));
			Logs.writeStmtEndToNewDDL();
			Logs.writeNewlineToNewDDL();
			firstTime = false;

		}

		// Set schema back to default ("SPLICE").
		if (!firstTime) {
			Logs.reportMessage("DBLOOK_DefaultSchema");
			Logs.writeToNewDDL("SET SCHEMA \"SPLICE\"");
			Logs.writeStmtEndToNewDDL();
			Logs.writeNewlineToNewDDL();
		}

		rs.close();
		stmt.close();

	}

}
