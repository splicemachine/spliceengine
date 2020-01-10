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

package com.splicemachine.db.tools;

import com.splicemachine.db.iapi.services.info.ProductVersionHolder;
import com.splicemachine.db.impl.tools.sysinfo.Main;

/**
	
   This class displays system information to system out.
	 
	To run from the command-line, enter the following:
	<p>
	<code>java com.splicemachine.db.tools.sysinfo</code>
	<p>
	<p>
	Also available on this class are methods which allow you to determine
	the version of the code for the system without actually booting a database.
	Please note that this is the Derby version of the .jar files, not of your databases.
	<p>
	The numbering scheme for released Derby products is <b><code>m1.m2.m3 </code></b>
	where <b><code>m1</code></b> is the major release version, <b><code>m2</code></b> is the minor release version,
	and <b><code>m3</code></b> is the maintenance level. Versions of the product with the same
	major and minor version numbers are considered feature compatible. 
	<p>Valid major and minor versions are always greater than zero. Valid maintenance
	versions are greater than or equal to zero.


*/
public class sysinfo {

  static public void main(String[] args) {
    Main.main(args);
  }

  private sysinfo() { // no instances allowed
  }

	/**
		The genus name for the Apache Derby code. Use this to determine the version of the
		Apache Derby embedded code in db.jar.
	*/
	public static final String DBMS="DBMS";

	/**
	 *	The genus name for the tools code. Use this to determine the version of 
		code in derbytools.jar
	 */
	public static final String TOOLS="tools";

	/**
	 *	The genus name for the network server code. Use this to determine the version of 
		code in derbynet.jar
	 */
	public static final String NET="net";

	/**
	 *	The genus name for the client code. Use this to determine the version of 
		code in derbyclient.jar
	 */
	public static final String CLIENT="dnc";


	/**
		gets the major version of the Apache Derby embedded code.
		@return	the major version. Returns -1 if not found.
	 */
  static public int getMajorVersion()
  {
    return getMajorVersion(DBMS);
  }


	/**
		gets the major version of the specified code library. 
		@param genus	which library to get the version of. Valid inputs include
			DBMS, TOOLS, NET, CLIENT
		@return the major version. Return -1 if the information is not found. 
    */		
  static public int getMajorVersion(String genus)
  {
        ProductVersionHolder pvh = ProductVersionHolder.getProductVersionHolderFromMyEnv(genus);
        if (pvh == null)
        {
            return -1;
        }

        return pvh.getMajorVersion();
  }


	/**
		gets the minor version of the Apache Derby embedded code.
		@return	the minor version. Returns -1 if not found.
	 */
  static public int getMinorVersion()
  {
    return getMinorVersion(DBMS);
  }

	/**
		gets the minor version of the specified code library. 
		@param genus	which library to get the version of. Valid inputs include
			DBMS, TOOLS, NET, CLIENT.
		@return the minor version. Return -1 if the information is not found. 
    */	
  static public int getMinorVersion(String genus)
  {
        ProductVersionHolder pvh = ProductVersionHolder.getProductVersionHolderFromMyEnv(genus);
        if (pvh == null)
        {
            return -1;
        }

        return pvh.getMinorVersion();
  }

	/**
		gets the build number for the Apache Derby embedded library
		@return the build number, or -1 if the information is not found.
	*/
  static public String getBuildNumber()
  {
    return getBuildNumber("DBMS");
  }

	/**
		gets the build number for the specified library
		@param genus which library to get the build number for. Valid inputs are
			DBMS, TOOLS, NET, CLIENT.
		@return the build number, or ???? if the information is not found.
	*/
  static public String getBuildNumber(String genus)
  {
        ProductVersionHolder pvh = ProductVersionHolder.getProductVersionHolderFromMyEnv(genus);
        if (pvh == null)
        {
            return "????";
        }

        return pvh.getBuildNumber();
  }


	/**
		gets the product name for the Apache Derby embedded library
		@return the name
	*/
  static public String getProductName()
  {
    return getProductName("DBMS");
  }

	/**
		gets the external name for the specified code library.
		@param genus which library to get the name for
		@return the name.
	*/

  static public String getProductName(String genus)
  {
        ProductVersionHolder pvh = ProductVersionHolder.getProductVersionHolderFromMyEnv(genus);
        if (pvh == null)
        {
            return Main.getTextMessage ("SIF01.K");
        }

        return pvh.getProductName();
  }

  /**
	Return the version information string for the specified library including alpha or beta indicators.
  */
  static public String getVersionString() {
	return getVersionString(DBMS);
  }

  /**
	Return the version information string for the Apache Derby embedded library including alpha or beta indicators.
  */
  static public String getVersionString(String genus) {

        ProductVersionHolder pvh = ProductVersionHolder.getProductVersionHolderFromMyEnv(genus);
        if (pvh == null)
        {
            return Main.getTextMessage ("SIF01.K");
        }
		
		return pvh.getVersionBuildString(false);
  }

  public static void getInfo (java.io.PrintWriter out) {
    Main.getMainInfo(out, false);
  }
}
