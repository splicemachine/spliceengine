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
package com.splicemachine.dbTesting.functionTests.tests.lang;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

import com.splicemachine.db.impl.tools.ij.utilMain;
import com.splicemachine.db.tools.ij;


public class wisconsin {


	public static void main(String[] args) throws Throwable{
		ij.getPropertyArg(args); 
        Connection conn = ij.startJBMS();
        
        conn.setAutoCommit(false);
        conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
        
        createTables(conn, true);
        
        BufferedInputStream inStream;
        String resource = "com/splicemachine/dbTesting/functionTests/tests/" +
                "lang/wisc_setup.sql";  
        // set input stream
        URL sql = getTestResource(resource);
        InputStream sqlIn = openTestResource(sql);
        if (sqlIn == null ) {
            throw new Exception("SQL Resource missing:" +
                    resource);
        }

        inStream = new BufferedInputStream(sqlIn, 
                utilMain.BUFFEREDFILESIZE);

		ij.runScript(conn, inStream, "US-ASCII",
			     System.out, (String) null );
		conn.commit();
	}
	
	public static void createTables(Connection conn, boolean compress)
			throws SQLException {
                createTables(conn, compress, 10000);
        }
	public static void createTables(Connection conn, boolean compress, int numRows)
			throws SQLException {

		Statement stmt = conn.createStatement();
		
		stmt.execute("create table TENKTUP1 ( unique1 int not null, " +
											 "unique2 int not null, " +
											 "two int, " +
											 "four int, " +
											 "ten int, " +
											 "twenty int, " +
											 "onePercent int, " +
											 "tenPercent int, " +
											 "twentyPercent int, " +
											 "fiftyPercent int, " +
											 "unique3 int, " +
											 "evenOnePercent int, " +
											 "oddOnePercent int, " +
											 "stringu1 char(52) not null, " +
											 "stringu2 char(52) not null, " +
											 "string4 char(52) )");
		//--insert numRows rows into TENKTUP1
		WISCInsert wi = new WISCInsert();
		wi.doWISCInsert(numRows, "TENKTUP1", conn);
		
		stmt.execute("create unique index TK1UNIQUE1 on TENKTUP1(unique1)");
		stmt.execute("create unique index TK1UNIQUE2 on TENKTUP1(unique2)");
		stmt.execute("create index TK1TWO on TENKTUP1(two)");
		stmt.execute("create index TK1FOUR on TENKTUP1(four)");
		stmt.execute("create index TK1TEN on TENKTUP1(ten)");
		stmt.execute("create index TK1TWENTY on TENKTUP1(twenty)");
		stmt.execute("create index TK1ONEPERCENT on TENKTUP1(onePercent)");
		stmt.execute("create index TK1TWENTYPERCENT on TENKTUP1(twentyPercent)");
		stmt.execute("create index TK1EVENONEPERCENT on TENKTUP1(evenOnePercent)");
		stmt.execute("create index TK1ODDONEPERCENT on TENKTUP1(oddOnePercent)");
		stmt.execute("create unique index TK1STRINGU1 on TENKTUP1(stringu1)");
		stmt.execute("create unique index TK1STRINGU2 on TENKTUP1(stringu2)");
		stmt.execute("create index TK1STRING4 on TENKTUP1(string4)");
		
		stmt.execute("create table TENKTUP2 (unique1 int not null, " +
											"unique2 int not null, " +
											"two int, " +
											"four int, " +
											"ten int, " +
											"twenty int, " +
											"onePercent int, " +
											"tenPercent int, " +
											"twentyPercent int, " +
											"fiftyPercent int, " +
											"unique3 int, " +
											"evenOnePercent int, " +
											"oddOnePercent int, " +
											"stringu1 char(52), " +
											"stringu2 char(52), " +
											"string4 char(52) )");
		//-- insert numRows rows into TENKTUP2
		wi = new WISCInsert();
		wi.doWISCInsert(numRows, "TENKTUP2", conn);
		
		stmt.execute("create unique index TK2UNIQUE1 on TENKTUP2(unique1)");
		stmt.execute("create unique index TK2UNIQUE2 on TENKTUP2(unique2)");
		
		stmt.execute("create table ONEKTUP ( unique1 int not null, " +
											"unique2 int not null, " +
											"two int, " +
											"four int, " +
											"ten int, " +
											"twenty int, " +
											"onePercent int, " +
											"tenPercent int, " +
											"twentyPercent int, " +
											"fiftyPercent int, " +
											"unique3 int, " +
											"evenOnePercent int, " +
											"oddOnePercent int, " +
											"stringu1 char(52), " +
											"stringu2 char(52), " +
											"string4 char(52) )");
		
		//-- insert 1000 rows into ONEKTUP
		wi = new WISCInsert();
		wi.doWISCInsert(1000, "ONEKTUP", conn);
		
		stmt.execute("create unique index ONEKUNIQUE1 on ONEKTUP(unique1)");
		stmt.execute("create unique index ONEKUNIQUE2 on ONEKTUP(unique2)");

		stmt.execute("create table BPRIME (	 unique1 int, " +
										  	"unique2 int, " +
											"two int, " +
											"four int, " +
											"ten int, " +
											"twenty int, " +
											"onePercent int, " +
											"tenPercent int, " +
											"twentyPercent int, " +
											"fiftyPercent int, " +
											"unique3 int, " +
											"evenOnePercent int, " +
											"oddOnePercent int, " +
											"stringu1 char(52), " +
											"stringu2 char(52), " +
											"string4 char(52))");

		stmt.execute("insert into BPRIME select * from TENKTUP2 where TENKTUP2.unique2 < 1000");

		conn.commit();

		if (!compress) {
			return;
		}

		PreparedStatement ps2 = conn.prepareStatement
			("call SYSCS_UTIL.SYSCS_COMPRESS_TABLE(?, ?, ?)");
		ps2.setString(1, "SPLICE");
		ps2.setString(2, "BPRIME");
		ps2.setInt(3, 0);
		ps2.executeUpdate();
		conn.commit();

		ps2.setString(1, "SPLICE");
		ps2.setString(2, "TENKTUP1");
		ps2.setInt(3, 0);
		ps2.executeUpdate();
		conn.commit();
		
		ps2.setString(1, "SPLICE");
		ps2.setString(2, "TENKTUP2");
		ps2.setInt(3, 0);
		ps2.executeUpdate();
		conn.commit();

		ps2.setString(1, "SPLICE");
		ps2.setString(2, "ONEKTUP");
		ps2.setInt(3, 0);
		ps2.executeUpdate();
		conn.commit();
	}
	
	
    /**
     * Open the URL for a a test resource, e.g. a policy
     * file or a SQL script.
     * @param url URL obtained from getTestResource
     * @return An open stream
    */
    protected static InputStream openTestResource(final URL url)
        throws PrivilegedActionException
    {
        return (InputStream)AccessController.doPrivileged
        (new java.security.PrivilegedExceptionAction(){

            public Object run() throws IOException{
            return url.openStream();

            }

        }
         );     
    }
    
    /**
     * Obtain the URL for a test resource, e.g. a policy
     * file or a SQL script.
     * @param name Resource name, typically - org.apache.derbyTesing.something
     * @return URL to the resource, null if it does not exist.
     */
    protected static URL getTestResource(final String name)
    {

    return (URL)AccessController.doPrivileged
        (new java.security.PrivilegedAction(){

            public Object run(){
            return this.getClass().getClassLoader().
                getResource(name);

            }

        }
         );
    }  
}
