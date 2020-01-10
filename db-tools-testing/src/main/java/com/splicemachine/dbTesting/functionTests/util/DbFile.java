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

package com.splicemachine.dbTesting.functionTests.util;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.lang.StringBuffer;
import java.net.URL;

/**
  Utility class for testing files stored in the database.
  */ 
public class DbFile
{
	/**
	  Read the current generation of a file stored in the
	  database we are connected to and return a 1 line string
	  representation of the file.

	  Sample usage
	  values com.splicemachine.dbTesting.functionTests.util.DbFile::readAsString('S1','J1');
	  @exception Exception Oops.
	  */
/*	
CANT USE JarAccess - not a public API (actually it's gone!)
public static String
	readAsString(String schemaName, String sqlName)
		 throws Exception
	{
		InputStream is = JarAccess.getAsStream(schemaName,
											sqlName,
 											FileResource.CURRENT_GENERATION_ID);
		return stringFromFile(is);
	}
*/
	/**
	  Create a string that contains a representation of the content of
	  a file for testing.
	  @exception Exception Oops.
	  */
	public static String
	stringFromFile(InputStream is)
		 throws Exception
	{
		InputStreamReader isr = new InputStreamReader(is);
		BufferedReader br =
			new BufferedReader(isr);
		StringBuilder sb = new StringBuilder();
		String l;
		while((l = br.readLine()) != null) {
			sb.append(l);
			sb.append("<CR>");
		}
		is.close();
		return sb.toString();
	}

	/**
	  Get the URL for a resource.

	  @param packageName the name of the resource package
	  @param name the name of the resourse.
	  */
	public static URL
	getResourceURL(String packageName, String name)
	{
		String resourceName =
			"/"+
			packageName.replace('.','/')+
			"/"+
			name;
		//
		//We need a class to get our URL. Since we give a
		//fully qualified name for the URL, any class will
		//do.
		Class c = resourceName.getClass();
		return c.getResource(resourceName);
	}

	/**
	  Get an InputStream for reading a resource.

	  @param packageName the name of the resource package
	  @param name the name of the resourse.
	  @exception Exception Oops.
	  */
	public static InputStream
	getResourceAsStream(String packageName, String name)
	{
		String resourceName =
			"/"+
			packageName.replace('.','/')+
			"/"+
			name;
		//
		//We need a class to get our URL. Since we give a
		//fully qualified name for the URL, any class will
		//do.
		Class c = resourceName.getClass();
		return c.getResourceAsStream(resourceName);
	}

	public	static	boolean	deleteFile( String outputFileName )
		 throws Exception
	{
		File f = new File( outputFileName );

		return f.delete();
	}

	public static String mkFileFromResource
	(String packageName, String resourceName)
		 throws Exception
	{
		return mkFileFromResource( packageName, resourceName, resourceName );
	}

	public static String mkFileFromResource
	( String packageName, String resourceName, String outputFileName )
		 throws Exception
	{
		File f = new File( outputFileName );
		InputStream is = getResourceAsStream(packageName,resourceName);
		BufferedInputStream bis = new BufferedInputStream(is);
		OutputStream os = new FileOutputStream(f);
		byte[]buf=new byte[4096];
		int readThisTime = 0;
		while((readThisTime = bis.read(buf)) != -1)
			os.write(buf,0,readThisTime);
		os.close();
		return f.getAbsolutePath();
	}
}
 
