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
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;

import com.splicemachine.db.tools.ij;

/*
	This is from a bug found by a beta customer.
 */
public class simpleThread extends Thread {

        private static Connection _connection = null;
        private static boolean _inUse = false;
        private static Object _lock = new Object();

        private long _wait = 0;
        private long _myCount = 0;
        private static int _count = 0;
        private synchronized static int getCount() { return(_count++); }
        private String _query;

        public simpleThread( String query, long waitTime) {
                _wait = waitTime;
                _myCount = getCount();
                _query = query;
                start();
        }

        public void run() {
				int rows = 0;
				boolean caught = false;
                try {
                        Thread.currentThread().sleep(_wait);
                        Connection conn = GetConnection();
                        Statement stmt = conn.createStatement();
                        String query = _query;
                        ResultSet rs = stmt.executeQuery( query );
                        ResultSetMetaData rsmd = rs.getMetaData();
                        //int cols = rsmd.getColumnCount();
                        while(rs.next()) {
							rows++;
                                //System.out.print(_myCount + ":");
                                //for( int x=0;x<cols;x++) {
                                 //       String s = rs.getString(x+1);
                                  //      if( x > 0) System.out.print(",");
                                   //     System.out.print(s);
                                //}
                                //System.out.println();
                        }
                        stmt.close();
                        ReturnConnection(conn);
                } catch (Exception ex) {
					// we expect some threads to get exceptions
					caught = true;
                }
				if (rows == 3 || caught)
				{
					//System.out.println("This thread's okay!");
			    }
				else
				{
					System.out.println("FAIL: thread "+_myCount+" only got "+rows+" rows and caught was "+caught);
		        }
        }


        public simpleThread(String argv[]) throws Exception {
            
			ij.getPropertyArg(argv);
			_connection = ij.startJBMS();

			Connection conn = GetConnection();

            Statement stmt = conn.createStatement();

            stmt.execute("create table people(name varchar(255), address varchar(255), phone varchar(64))");
            stmt.execute("insert into people VALUES ('mike', 'mikes address', '123-456-7890')");
            stmt.execute("insert into people VALUES ('adam', 'adams address', '123-456-1234')");
            stmt.execute("insert into people VALUES ('steve', 'steves address', '123-456-4321')");
            stmt.close();

            ReturnConnection(conn);

            String query = "SELECT * from people ORDER by name";

            Thread[] threads = {
                new simpleThread(query,0),
                new simpleThread(query,10000),
                new simpleThread(query,10100),
                new simpleThread(query,20000),
            };

            for (int i = 0; i < threads.length; i++) {
                threads[i].join();
            }

            _connection.close();
            _connection = null;
        }

        public static Connection GetConnection() {
                synchronized(_lock) {
                        _inUse = true;
                }
                return _connection;
        }
        public static void ReturnConnection(Connection c) {
                synchronized(_lock) {
                        _inUse = false;
                        _lock.notifyAll();
                }
        }
}
