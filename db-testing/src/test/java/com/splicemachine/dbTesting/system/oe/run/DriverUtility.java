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
package com.splicemachine.dbTesting.system.oe.run;

import java.sql.SQLException;
import java.sql.DriverManager;
import java.sql.Connection;
import com.splicemachine.dbTesting.system.oe.client.Load;
import com.splicemachine.dbTesting.system.oe.load.SimpleInsert;
import com.splicemachine.dbTesting.system.oe.util.HandleCheckError;
import com.splicemachine.dbTesting.system.oe.util.OEChecks;

/**
 * Driver to do the load phase for the Order Entry benchmark.
 *
 * This class takes in following arguments currently:
 * Usage: java com.splicemachine.dbTesting.system.oe.run.DriverUtility options
 * Options:
 * <OL>
 * <LI>-scale warehouse scaling factor. Takes a short value. If not specified 
 * defaults to 1
 * <LI>-doChecks check consistency of data, takes a boolean value. If not specified, defaults to true
 * <LI>-driver jdbc driver class to use
 * <LI>-dbUrl  database connection url 
 * <LI>-help prints usage
 * </OL>
 *
 * To load database with scale of 2
 * and to not do any checks, the command to run the test is as follows:
 * <BR>
 * java com.splicemachine.dbTesting.system.oe.run.DriverUtility -driver com.splicemachine.db.jdbc.ClientDriver -dbUrl 'jdbc:splice://localhost:1527/db' -scale 2 -doChecks false
 * <BR>
 */
public class DriverUtility {

    /**
     * Database connection
     */
    private Connection conn = null;
    /**
     * Warehouse scale factor
     */
    private static short scale = 1;

    /**
     * Database connection url
     */
    private static String dbUrl = "jdbc:splice:wombat;create=true";

    /**
     * JDBC Driver class
     */
    private static String driver = "com.splicemachine.db.jdbc.EmbeddedDriver";

    /**
     * flag to indicate if we should perform consistency, cardinality checks
     * after the load
     */
    private static boolean doChecks = true;

    /**
     * Create a test case with the given name.
     */
    public DriverUtility() {

        try {
            Class.forName(driver);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    /**
     * Run OE load
     * @param args supply arguments for benchmark.
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        parseArgs(args);
        DriverUtility oe = new DriverUtility();
        oe.populate();
        if ( doChecks )
            oe.allChecks();
        oe.cleanup();
    }

    /**
     * @return the connection
     * @throws SQLException
     */
    public Connection getConnection() throws SQLException {
        if ( conn == null)
        {
            System.out.println("dbUrl="+dbUrl);
            conn = DriverManager.getConnection(dbUrl);
        }
        return conn;
    }

    /**
     * Populate the OE database.
     * Assumption is that the schema is already loaded
     * in the database.
     */
    public void populate() throws Exception {
        // Use simple insert statements to insert data.
        // currently only this form of load is present, once we have
        // different implementations, the loading mechanism will need
        // to be configurable taking an option from the command line
        // arguments.
        Load loader = new SimpleInsert();
        loader.setupLoad(getConnection(), scale);
        long start = System.currentTimeMillis();
        loader.populateAllTables();
        long stop = System.currentTimeMillis();
        System.out.println("Time to load (ms)=" + (stop - start));
    }

    /**
     * Do the necessary checks to see if database is in consistent state
     */
    public void allChecks() throws Exception {
        OEChecks checks = new OEChecks();
        checks.initialize(new HandleCheckError(), getConnection(), scale);
        long start = System.currentTimeMillis();
        checks.checkAllRowCounts();
        long stop = System.currentTimeMillis();
        System.out.println("Time to do checks (ms)=" + (stop - start));
    }

    /**
     * parse arguments.
     * @param args arguments to parse
     */
    private static void parseArgs(String[] args) {
        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            if (arg.equals("-scale")) {
                scale = Short.parseShort(args[++i]);
            } else if (arg.equals("-driver")) {
                driver = args[++i];
            } else if (arg.equals("-dbUrl")) {
                dbUrl = args[++i];
            } else if (arg.equals("-doChecks")) {
                doChecks = (args[++i].equals("false")? false:true);
            } else if (arg.equals("-help")) {
                printUsage();
                System.exit(0);
            } else {
                System.err.println("Invalid option: " + args[i]);
                System.exit(1);
            }
        }
    }

    /**
     * prints the usage
     */
    private static void printUsage() {
        System.out.println("Usage: java com.splicemachine.dbTesting.system.oe." +
                "run.DriverUtility options");
        System.out.println();
        System.out.println("Options:");
        System.out.println("  -scale warehouse scaling factor. " +
                "Takes a short value. If not specified defaults to 1");
        System.out.println("  -doChecks  should consistency checks be run" +
                " on the database. Takes a boolean value");
        System.out.println("  -driver  the class of the jdbc driver");
        System.out.println("  -dbUrl  the database connection url");
        System.out.println("  -help prints usage");
        System.out.println();
    }

    /**
     * cleanup resources. 
     * @throws SQLException
     */
    public void cleanup() throws SQLException {
        if (conn != null)
            conn.close();
    }

}
