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
package com.splicemachine.dbTesting.junit;

import java.io.File;
import java.security.AccessController;
import java.sql.SQLException;

import javax.sql.DataSource;

import junit.framework.Test;

/**
 * Shutdown and drop the database identified by the logical
 * name passed in when creating this decorator.
 *
 */
class DropDatabaseSetup extends BaseTestSetup {

    final String logicalDBName;
    DropDatabaseSetup(Test test, String logicalDBName) {
        super(test);
        this.logicalDBName = logicalDBName;
     }
    
    /**
     * Shutdown the database and then remove all of its files.
     */
    protected void tearDown() throws Exception {
        
        TestConfiguration config = TestConfiguration.getCurrent();
        
        // Ensure the database is booted
        // since that is what shutdownDatabase() requires.
        boolean shutdown;
        try {
            config.openConnection(logicalDBName).close();
            shutdown = true;
        } catch (SQLException e) {
            String  sqlState = e.getSQLState();
            // If the database cannot be booted due
            // to some restrictions such as authentication
            // or encrypted (ie here we don't know the 
            // correct authentication tokens, then it's
            // ok since we just want it shutdown anyway!
            if ( "XJ040".equals( sqlState ) || "08004".equals( sqlState ) || "4251I".equals( sqlState ) )
            {
                shutdown = false;
            }
            else
            {
                throw e;
            }
        }
        if (shutdown)
        {
            DataSource ds = JDBCDataSource.getDataSourceLogical(logicalDBName);
            JDBCDataSource.shutdownDatabase(ds);
        }

        removeDatabase();
    }

    void removeDatabase()
    {
        TestConfiguration config = TestConfiguration.getCurrent();
        String dbName = config.getPhysicalDatabaseName(logicalDBName);
        dbName = dbName.replace('/', File.separatorChar);
        String dsh = BaseTestCase.getSystemProperty("derby.system.home");
        if (dsh == null) {
            fail("not implemented");
        } else {
            dbName = dsh + File.separator + dbName;
        }
        removeDirectory(dbName);
    }


    static void removeDirectory(String path)
    {
        final File dir = new File(path);
        removeDirectory(dir);
    }
    
    static void removeDirectory(final File dir) {
        AccessController.doPrivileged(new java.security.PrivilegedAction() {

            public Object run() {
                removeDir(dir);
                return null;
            }
        });
        
    }

    private static void removeDir(File dir) {
        
        // Check if anything to do!
        // Database may not have been created.
        if (!dir.exists())
            return;

        BaseJDBCTestCase.assertDirectoryDeleted(dir);
    }

    /**
     * Remove all the files in the list
     * @param list the list of files that will be deleted
     **/
    static void removeFiles(String[] list) {
        for (int i = 0; i < list.length; i++) {
             try {
                 File dfile = new File(list[i]);
                 assertTrue(list[i], dfile.delete());
             } catch (IllegalArgumentException e) {
                 fail("open file error");
             }
        }
    }
}
