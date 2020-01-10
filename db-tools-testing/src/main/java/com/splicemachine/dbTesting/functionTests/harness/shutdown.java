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

package com.splicemachine.dbTesting.functionTests.harness;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.*;
import java.io.*;


/*
 **
 ** shutdown
 **
 **	force a shutdown after a test complete to guarantee shutdown
 **	which doesn't always seem to happen with useprocess=false
 **
 */
public class shutdown
{
 
	static String shutdownurl;
	static String driver = "com.splicemachine.db.jdbc.EmbeddedDriver";
	static String systemHome;

	public static void main(String[] args) throws SQLException,
		InterruptedException, Exception 
    {
		systemHome = args[0];
		shutdownurl = args[1];
		try
		{
		    doit();
		}
		catch(Exception e)
		{
		    System.out.println("Exception in shutdown: " + e);
		}
	}

	public static void doit() throws SQLException,
		InterruptedException, Exception 
	{
		Connection conn = null;
		boolean finished = false;	
		Date d = new Date();

        Properties sp = System.getProperties();
        if (systemHome == null)
        {
		    systemHome = sp.getProperty("user.dir") + File.separatorChar +
			"testCSHome";
        	sp.put("derby.system.home", systemHome);
        	System.setProperties(sp);
        }
		boolean useprocess = true;
		String up = sp.getProperty("useprocess");
		if (up != null && up.equals("false"))
			useprocess = false;		

        PrintStream stdout = System.out;
    	PrintStream stderr = System.err;

		Class.forName(driver).newInstance();

		try 
		{
			conn = DriverManager.getConnection(shutdownurl);
		} 
		catch (SQLException  se) 
		{
		    if (se.getSQLState().equals("08006"))
		    {
		        // It was already shutdown
		        //System.out.println("Shutdown with: " + shutdownurl);
		    }
		    else 
			{
				System.out.println("shutdown failed for " + shutdownurl);
				System.exit(1);
	        }
		}
    }
}
