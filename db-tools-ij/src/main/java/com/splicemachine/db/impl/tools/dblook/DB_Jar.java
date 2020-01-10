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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import com.splicemachine.db.tools.dblook;

public class DB_Jar {

	/* ************************************************
	 * Generate the DDL for all jars in a given
	 * database.
	 * @param dbName Name of the database (for locating the jar).
	 * @param conn Connection to the source database.
     * @param at10_9 Dictionary is at 10.9 or higher
	 * @return The DDL for the jars has been written
	 *  to output via Logs.java.
	 ****/

	public static void doJars(
        String dbName, Connection conn, boolean at10_9)
		throws SQLException
	{

		String separator = System.getProperty("file.separator");
		Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(
            "SELECT FILENAME, SCHEMAID, " +
            "GENERATIONID, FILEID FROM SYS.SYSFILES");

		boolean firstTime = true;
		while (rs.next()) {

            StringBuilder loadJarString = new StringBuilder();

            String jarName    = rs.getString(1);
            String schemaId   = rs.getString(2);
            String genID      = rs.getString(3);
            String UUIDstring = rs.getString(4);

            String schemaNameSQL = dblook.lookupSchemaId(schemaId);

            if (dblook.isIgnorableSchema(schemaNameSQL))
                continue;

            doHeader(firstTime);

            if (at10_9) {
                String schemaNameCNF =
                    dblook.unExpandDoubleQuotes(
                        dblook.stripQuotes(dblook.lookupSchemaId(schemaId)));

                StringBuilder jarFullName = new StringBuilder();
                jarFullName.append(UUIDstring);
                jarFullName.append(".jar.G");
                jarFullName.append(genID);

                StringBuilder oldJarPath = new StringBuilder();
                oldJarPath.append(dbName);
                oldJarPath.append(separator);
                oldJarPath.append("jar");
                oldJarPath.append(separator);
                oldJarPath.append(jarFullName.toString());

                // Copy jar file to DBJARS directory.
                String absJarDir = null;
                try {

                    // Create the DBJARS directory.
                    File jarDir = new File(System.getProperty("user.dir") +
                                           separator + "DBJARS");
                    absJarDir = jarDir.getAbsolutePath();
                    jarDir.mkdirs();

                    doCopy(oldJarPath.toString(), absJarDir + separator + jarFullName);
                } catch (Exception e) {
                    Logs.debug("DBLOOK_FailedToLoadJar",
                               absJarDir + separator + jarFullName.toString());
                    Logs.debug(e);
                    firstTime = false;
                    continue;
                }

                // Now, add the DDL to read the jar from DBJARS.
                loadJarString.append("CALL SQLJ.INSTALL_JAR('file:");
                loadJarString.append(absJarDir);
                loadJarString.append(separator);
                loadJarString.append(jarFullName);
                loadJarString.append("', '");
                loadJarString.append(
                    dblook.addQuotes(
                        dblook.expandDoubleQuotes(schemaNameCNF)));

                loadJarString.append(".");

                loadJarString.append(
                    dblook.addQuotes(
                        dblook.expandDoubleQuotes(jarName)));

            } else {
                jarName = dblook.addQuotes(
                    dblook.expandDoubleQuotes(jarName));

                String schemaWithoutQuotes = dblook.stripQuotes(schemaNameSQL);
                StringBuilder jarFullName = new StringBuilder(separator);
                jarFullName.append(dblook.stripQuotes(jarName));
                jarFullName.append(".jar.G");
                jarFullName.append(genID);

                StringBuilder oldJarPath = new StringBuilder();
                oldJarPath.append(dbName);
                oldJarPath.append(separator);
                oldJarPath.append("jar");
                oldJarPath.append(separator);
                oldJarPath.append(schemaWithoutQuotes);
                oldJarPath.append(jarFullName);

                // Copy jar file to DBJARS directory.
                String absJarDir = null;
                try {

                    // Create the DBJARS directory.
                    File jarDir = new File(
                        System.getProperty("user.dir") +
                        separator + "DBJARS" + separator + schemaWithoutQuotes);
                    absJarDir = jarDir.getAbsolutePath();
                    jarDir.mkdirs();

                    doCopy(oldJarPath.toString(), absJarDir + jarFullName);
                } catch (Exception e) {
                    Logs.debug("DBLOOK_FailedToLoadJar",
                               absJarDir + jarFullName.toString());
                    Logs.debug(e);
                    firstTime = false;
                    continue;
                }

                // Now, add the DDL to read the jar from DBJARS.
                loadJarString.append("CALL SQLJ.INSTALL_JAR('file:");
                loadJarString.append(absJarDir);
                loadJarString.append(jarFullName);
                loadJarString.append("', '");
                loadJarString.append(schemaNameSQL);
                loadJarString.append(".");
                loadJarString.append(jarName);
            }
            
            loadJarString.append("', 0)");

            Logs.writeToNewDDL(loadJarString.toString());
            Logs.writeStmtEndToNewDDL();
            Logs.writeNewlineToNewDDL();
            firstTime = false;
		}

		stmt.close();
		rs.close();

	}

    private static void  doHeader(boolean firstTime) {
        if (firstTime) {
            Logs.reportString(
                "----------------------------------------------");
            Logs.reportMessage("DBLOOK_JarsHeader");
            Logs.reportMessage("DBLOOK_Jar_Note");
            Logs.reportString(
                "----------------------------------------------\n");
        }
    }

    private static void doCopy(
        String oldJarFileName,
        String newJarFileName) throws IOException {

        FileInputStream oldJarFile = new FileInputStream(oldJarFileName);
        FileOutputStream newJarFile = new FileOutputStream(newJarFileName);
        int st = 0;
        while (true) {
            if (oldJarFile.available() == 0)
                break;
            byte[] bAr = new byte[oldJarFile.available()];
            oldJarFile.read(bAr);
            newJarFile.write(bAr);
        }

        oldJarFile.close();
        newJarFile.close();
    }
}
