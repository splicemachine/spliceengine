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
package com.splicemachine.dbTesting.functionTests.tests.tools;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import com.splicemachine.dbTesting.junit.BaseJDBCTestCase;


/**
 * This is base class for some of the import tests, provides 
 * methods to perform import/export using different procedures.
 */

public abstract class ImportExportBaseTest extends BaseJDBCTestCase {


    public ImportExportBaseTest(String name) {
        super(name);
    }


    /**
     * Perform export using SYSCS_UTIL.SYSCS_EXPORT_TABLE procedure.
     */
    protected void doExportTable(String schemaName, 
                                 String tableName, 
                                 String fileName, 
                                 String colDel , 
                                 String charDel, 
                                 String codeset) throws SQLException 
    {
        String expsql = 
            "call SYSCS_UTIL.SYSCS_EXPORT_TABLE (? , ? , ? , ?, ? , ?)";
        PreparedStatement ps = prepareStatement(expsql);
        ps.setString(1, schemaName);
        ps.setString(2, tableName);
        ps.setString(3, fileName);
        ps.setString(4, colDel);
        ps.setString(5, charDel);
        ps.setString(6, codeset);
        ps.execute();
        ps.close();
    }

    

    /**
     * Perform export using SYSCS_UTIL.SYSCS_EXPORT_QUERY procedure.
     */
    protected void doExportQuery(String query,
                               String fileName,
                               String colDel , 
                               String charDel, 
                               String codeset) 
        throws SQLException 
    {
        String expsql = 
            "call SYSCS_UTIL.SYSCS_EXPORT_QUERY(? , ? , ? , ?, ?)";
        PreparedStatement ps = prepareStatement(expsql);
        ps.setString(1, query);
        ps.setString(2, fileName);
        ps.setString(3, colDel);
        ps.setString(4, charDel);
        ps.setString(5, codeset);
        ps.execute();
        ps.close();
    }

    /**
     * Perform import using SYSCS_UTIL.SYSCS_IMPORT_TABLE procedure.
     */
    protected void doImportTable(String schemaName,
                               String tableName, 
                               String fileName, 
                               String colDel, 
                               String charDel , 
                               String codeset, 
                               int replace) throws SQLException 
    {
        String impsql = 
            "call SYSCS_UTIL.SYSCS_IMPORT_TABLE (?, ?, ?, ?, ?, ?, ?)";
        PreparedStatement ps = prepareStatement(impsql);
        ps.setString(1 , schemaName);
        ps.setString(2, tableName);
        ps.setString(3, fileName);
        ps.setString(4 , colDel);
        ps.setString(5 , charDel);
        ps.setString(6 , codeset);
        ps.setInt(7, replace);
        ps.execute();
        ps.close();
    }


    /**
     *  Perform import using SYSCS_UTIL.SYSCS_IMPORT_DATA procedure.
     */
    protected void doImportData(String schemaName,
                                String tableName, 
                                String insertCols,
                                String colIndexes, 
                                String fileName,
                                String colDel, 
                                String charDel , 
                                String codeset, 
                                int replace) throws SQLException 
    {
        String impsql = 
            "call SYSCS_UTIL.SYSCS_IMPORT_DATA(?, ?, ?, ?, ?, ?, ?, ?, ?)";
        PreparedStatement ps = prepareStatement(impsql);
        ps.setString(1, schemaName);
        ps.setString(2, tableName);
        ps.setString(3, insertCols);
        ps.setString(4, colIndexes);
        ps.setString(5, fileName);
        ps.setString(6 , colDel);
        ps.setString(7 , charDel);
        ps.setString(8 , codeset);
        ps.setInt(9, replace);
        ps.execute();
        ps.close();
    }


    /**
     * Perform export using 
     * SYSCS_UTIL.SYSCS_EXPORT_TABLE_LOBS_TO_EXTFILE procedure.
     */
    protected void doExportTableLobsToExtFile(String schemaName, 
                                              String tableName, 
                                              String fileName, 
                                              String colDel , 
                                              String charDel, 
                                              String codeset, 
                                              String lobsFileName) 
        throws SQLException 
    {
        String expsql = 
            "call SYSCS_UTIL.SYSCS_EXPORT_TABLE_LOBS_TO_EXTFILE" +  
            "(? , ? , ? , ?, ?, ?, ?)";
        PreparedStatement ps = prepareStatement(expsql);
        ps.setString(1, schemaName);
        ps.setString(2, tableName);
        ps.setString(3, fileName);
        ps.setString(4, colDel);
        ps.setString(5, charDel);
        ps.setString(6, codeset);
        ps.setString(7, lobsFileName);
        ps.execute();
        ps.close();
    }

    

    /**
     * Perform export using 
     * SYSCS_UTIL.SYSCS_EXPORT_QUERY_LOBS_TO_EXTFILE procedure.
     */
    protected void doExportQueryLobsToExtFile(String query,
                                              String fileName,
                                              String colDel , 
                                              String charDel, 
                                              String codeset, 
                                              String lobsFileName) 
        throws SQLException 
    {
        String expsql = 
            "call SYSCS_UTIL.SYSCS_EXPORT_QUERY_LOBS_TO_EXTFILE" + 
            "(? , ? , ? , ?, ?, ?)";
        PreparedStatement ps = prepareStatement(expsql);
        ps.setString(1, query);
        ps.setString(2, fileName);
        ps.setString(3, colDel);
        ps.setString(4, charDel);
        ps.setString(5, codeset);
        ps.setString(6, lobsFileName);
        ps.execute();
        ps.close();
    }

    /**
     * Perform import using 
     * SYSCS_UTIL.SYSCS_IMPORT_TABLE_LOBS_FROM_EXTFILE procedure.
     */
    protected void doImportTableLobsFromExtFile(String schemaName,
                                              String tableName, 
                                              String fileName, 
                                              String colDel, 
                                              String charDel , 
                                              String codeset, 
                                              int replace) 
        throws SQLException 
    {
        String impsql = 
            "call SYSCS_UTIL.SYSCS_IMPORT_TABLE_LOBS_FROM_EXTFILE" +
            "(?, ?, ?, ?, ?, ?, ?)";
        PreparedStatement ps = prepareStatement(impsql);
        ps.setString(1 , schemaName);
        ps.setString(2, tableName);
        ps.setString(3, fileName);
        ps.setString(4 , colDel);
        ps.setString(5 , charDel);
        ps.setString(6 , codeset);
        ps.setInt(7, replace);
        ps.execute();
        ps.close();
    }


    /**
     *  Perform import using 
     *  SYSCS_UTIL.SYSCS_IMPORT_DATA_LOBS_FROM_EXTFILE procedure.
     */
    protected void doImportDataLobsFromExtFile(String schemaName,
                                               String tableName, 
                                               String insertCols,
                                               String colIndexes, 
                                               String fileName,
                                               String colDel, 
                                               String charDel , 
                                               String codeset, 
                                               int replace) 
        throws SQLException 
    {
        String impsql = 
            "call SYSCS_UTIL.SYSCS_IMPORT_DATA_LOBS_FROM_EXTFILE" + 
            "(?, ?, ?, ?, ?, ?, ?, ?, ?)";
        PreparedStatement ps = prepareStatement(impsql);
        ps.setString(1, schemaName);
        ps.setString(2, tableName);
        ps.setString(3, insertCols);
        ps.setString(4, colIndexes);
        ps.setString(5, fileName);
        ps.setString(6 , colDel);
        ps.setString(7 , charDel);
        ps.setString(8 , codeset);
        ps.setInt(9, replace);
        ps.execute();
        ps.close();
    }
}
