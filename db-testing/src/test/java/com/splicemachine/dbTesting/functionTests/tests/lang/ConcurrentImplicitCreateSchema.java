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

package com.splicemachine.dbTesting.functionTests.tests.lang;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import com.splicemachine.db.tools.ij;

/**
 * Test for several threads creating tables in the same originally
 * non-existing schema.  This will cause an implicit creation of the
 * schema.  The test was created for the fix of JIRA issue DERBY-230
 * where an error occurred if two threads try to create the schema in
 * parallel.
 * 
 */

public class ConcurrentImplicitCreateSchema
{
    
    /**
     * Runnable that will create and drop table.
     */
    class CreateTable implements Runnable 
    {
        /** Id of the thread running this instance */
        private int myId;
        /** Which schema to use */
        private int schemaId;
        /** Database connection for this thread */
        private Connection conn = null;
        /** Test object. Used to inform about failures. */
        private ConcurrentImplicitCreateSchema test;

        CreateTable(int id, int schemaId, 
                    ConcurrentImplicitCreateSchema test) 
            throws SQLException, IllegalAccessException, 
                   ClassNotFoundException, InstantiationException
        {
            this.myId = id;
            this.schemaId = schemaId;
            this.test = test; 

            // Open a connection for this thread
            conn = ij.startJBMS();
            conn.setAutoCommit(false);
        }
     
        public void run()        
        {
            try {
                Statement s = conn.createStatement();
                s.execute("create table testschema" + schemaId + ".testtab" 
                          + myId + "(num int, addr varchar(40))");
                s.execute("drop table testschema" + schemaId + ".testtab" 
                          + myId);
            } catch (SQLException e) {
                test.failed(e);
            } 
  
            // We should close the transaction regardless of outcome 
            // Otherwise, other threads may wait on transactional 
            // locks until this transaction times out.
            try {
                conn.commit();
                conn.close();
            } catch (SQLException e) {
                test.failed(e);
            }
            //            System.out.println("Thread " + myId + " completed.");
        }
    }
    
    /** 
     * Outcome of test.  
     * Will be set to false if any failure is detected.
     */
    boolean passed = true;

    public static void main(String[] args)
    {
        new ConcurrentImplicitCreateSchema().go(args);
    }

    void go(String[] args)
    {
        System.out.println("Test ConcurrentImplicitCreateSchema starting");

        try {
            // Load the JDBC Driver class
            // use the ij utility to read the property file and
            // make the initial connection.
            ij.getPropertyArg(args);
            Connection conn = ij.startJBMS();

            conn.setAutoCommit(true);

            // Drop the schemas we will be using in case they exist.
            // This will ensure that they are implicitly created by this test
            Statement s = conn.createStatement();

            // Number of schemas to use.  An equal number of threads
            // will operate on each schema.
            final int NSCHEMAS = 1;

            for (int i=0; i < NSCHEMAS; ++i) {
                try {
                    s.execute("drop schema testschema" + i + " restrict");
                } catch (SQLException e) {
                    if (e.getSQLState().equals("42Y07")) {
                        // IGNORE. Schema did not exist. That is our target.
                    } else {
                        throw e;
                    }
                }
            }

            // Number of threads to run.  
            final int NTHREADS = 100;  

            // Create threads
            Thread[] threads = new Thread[NTHREADS];
            for (int i=0; i<NTHREADS; ++i) {
                threads[i] 
                    = new Thread(new CreateTable(i, i%NSCHEMAS, this));
            }
            
            // Start threads
            for (int i=0; i<NTHREADS; ++i) {
                threads[i].start();           
            }         
            
            // Wait for threads to complete
            for (int i=0; i<NTHREADS; ++i) {
                threads[i].join();           
            }
  
            conn.close();
            System.out.println("Closed connection");
        } catch (Throwable e) {
            System.out.println("exception thrown:");
            failed(e);
        }

        System.out.print("Test ConcurrentImplicitCreateSchema ");
        if (passed) {
            System.out.println("PASSED");
        } else {
            System.out.println("FAILED");
	}
    }

    void failed(Throwable e) 
    {
        if (e instanceof SQLException) {
            printSQLError((SQLException) e);
        } else {
            e.printStackTrace();
        }
        passed = false;
    }

    void printSQLError(SQLException e)
    {
        while (e != null)
        {
            System.out.println(e.toString());
            e.printStackTrace();
            e = e.getNextException();
        }
    }
}
