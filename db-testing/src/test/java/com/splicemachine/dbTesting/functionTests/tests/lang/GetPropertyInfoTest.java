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

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Properties;


public class GetPropertyInfoTest
{
	static String protocol = "jdbc:splice:";
	static String url = "EncryptedDB;create=true;dataEncryption=true";
	static String driver = "com.splicemachine.db.jdbc.EmbeddedDriver";

	public static void main(String[] args) throws SQLException,
		InterruptedException, Exception 
    {
		boolean		passed = true;

		// adjust URL to compensate for other encryption providers
		String provider = System.getProperty("testEncryptionProvider");
		if (provider != null)
		{
		    url = "EncryptedDB;create=true;dataEncryption=true;encryptionProvider=" + provider;
		}

		System.out.println("Test GetPropertyInfoTest starting");
		try
		{
			Properties info = new Properties();
			Class.forName(driver).newInstance();
			Driver cDriver = DriverManager.getDriver(protocol);
			boolean canConnect = false;

			// Test getPropertyInfo by passing attributes in the
			// url.
			for(int i = 0; i < 2; i++)
			{
				// In order to check for inadequate properties, we omit the
				// bootPassword and call getPropertyInfo. (bootPassword is
				// required because dataEncryption is true)
				DriverPropertyInfo[] attributes = cDriver.getPropertyInfo(protocol+url,info);
				
				// zero length means a connection attempt can be made
				if (attributes.length == 0)
				{
					canConnect = true;
					break;
				}

				for (int j = 0; j < attributes.length; j++)
				{
					System.out.print(attributes[j].name + " - value: " + attributes[j].value);
					// Also check on the other PropertyInfo fields
					String[] choices = attributes[j].choices;
					System.out.print(" - description: " 
						+ attributes[j].description +
						" - required " + attributes[j].required);
					if (choices != null)
					{
						for (int k = 0; k < choices.length; k++)
						{
							System.out.print("     - choices [" + k + "] : " + choices[k]);
						}
						System.out.print("\n");
					}
					else
						System.out.print(" - choices null \n");
				}

				// Now set bootPassword and call getPropertyInfo again.  
				// This time attribute length should be zero, sice we pass all
				// minimum required properties. 
				url = url + ";bootPassword=db2everyplace";
			}

			if(canConnect == false)
			{
				System.out.println("More attributes are required to connect to the database");
				passed = false;
			}
			else
			{			
				Connection conn = DriverManager.getConnection(protocol + url, info);
				conn.close();
			}
		
			canConnect = false;

			// Test getPropertyInfo by passing attributes in the
			// Properties array.
			info.put("create", "true");
			info.put("dataEncryption", "true");
			info.put("bootPassword", "db2everyplace");
			// Use alternate encryption provider if necessary.
			if (provider != null)
			{ 
			    info.put("encryptionProvider", provider);
			}

			for(int i = 0; i < 2; i++)
			{
				// In order to check for inadequate properties, we omit the
				// database name and call getPropertyInfo. 
				DriverPropertyInfo[] attributes = cDriver.getPropertyInfo(protocol,info);
			
				// zero length means a connection attempt can be made
				if (attributes.length == 0)
				{
					canConnect = true;
					break;
				}

				for (int j = 0; j < attributes.length; j++)
				{
					System.out.print(attributes[j].name + " - value: " + attributes[j].value);
					// Also check on the other PropertyInfo fields
					String[] choices = attributes[j].choices;
					System.out.print(" - description: " 
						+ attributes[j].description +
						" - required " + attributes[j].required);
					if (choices != null)
					{
						for (int k = 0; k < choices.length; k++)
						{
							System.out.print("     - choices [" + k + "] : " + choices[k]);
						}
						System.out.print("\n");
					}
					else
						System.out.print(" - choices null \n");
				}

				// Now set database name and call getPropertyInfo again.  
				// This time attribute length should be zero, sice we pass all
				// minimum required properties. 
				info.put("databaseName", "EncryptedDB1");
			}

			if(canConnect == false)
			{
				System.out.println("More attributes are required to connect to the database");
				passed = false;
			}
			else
			{			
				Connection conn1 = DriverManager.getConnection(protocol, info);
				conn1.close();
			}

		}
		catch(SQLException sqle)
		{
			passed = false;
			do {
				System.out.println(sqle.getSQLState() + ":" + sqle.getMessage());
				sqle = sqle.getNextException();
			} while (sqle != null);
		}
		catch (Throwable e) 
		{
			System.out.println("FAIL -- unexpected exception caught in main():\n");
			System.out.println(e.getMessage());
			e.printStackTrace();
			passed = false;
		}

		if(passed)
			System.out.println("Test GetPropertyInfoTest finished");
		else
			System.out.println("Test GetPropertyInfoTest failed");
	}
}

	









