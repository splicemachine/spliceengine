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

import com.splicemachine.db.iapi.tools.i18n.LocalizedInput;
import com.splicemachine.db.iapi.tools.i18n.LocalizedOutput;
import com.splicemachine.db.iapi.tools.i18n.LocalizedResource;

import com.splicemachine.db.impl.tools.ij.Main;
import com.splicemachine.db.impl.tools.ij.utilMain;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.sql.Connection;

/**
	
	ij is Derby's interactive JDBC scripting tool.
	It is a simple utility for running scripts against a Derby database.
	You can also use it interactively to run ad hoc queries.
	ij provides several commands for ease in accessing a variety of JDBC features.
	<P>

	To run from the command line enter the following:
	<p>
	java [options] com.splicemachine.db.tools.ij [arguments]
	<P>
	ij is can also be used with any database server that supports a JDBC driver.
*/
@SuppressFBWarnings(value = "NM_CLASS_NAMING_CONVENTION",justification = "DB-9772")
public class ij {

  /**
  	@exception IOException thrown if cannot access input or output files.
   */
  static public void main(String[] args) throws IOException {
      Main.main(args);
  }
  
  /**
   * Run a SQL script from an InputStream and write
   * the resulting output to the provided PrintStream.
   * SQL commands are separated by a semi-colon ';' character.
   * 
   * @param conn Connection to be used as the script's default connection. 
   * @param sqlIn InputStream for the script.
   * @param inputEncoding Encoding of the script.
   * @param sqlOut OutputStream for the script's output
   * @param outputEncoding Output encoding to use.
   * @return Number of SQLExceptions thrown during the execution, -1 if not known.
   * @throws UnsupportedEncodingException
   */
  public static int runScript(
		  Connection conn,
		  InputStream sqlIn,
		  String inputEncoding,
		  OutputStream sqlOut,
		  String outputEncoding)
		  throws UnsupportedEncodingException
  {
	  return ij.runScript(conn, sqlIn, inputEncoding, sqlOut, outputEncoding,false);
  }

    /**
    * Run a SQL script from an InputStream and write
    * the resulting output to the provided PrintStream.
    * SQL commands are separated by a semi-colon ';' character.
    *
    * @param conn Connection to be used as the script's default connection.
    * @param sqlIn InputStream for the script.
    * @param inputEncoding Encoding of the script.
    * @param sqlOut OutputStream for the script's output
    * @param outputEncoding Output encoding to use.
    * @param loadSystemProperties Whether to use the system properties.
    * @return Number of SQLExceptions thrown during the execution, -1 if not known.
    * @throws UnsupportedEncodingException
    */
    public static int runScript(
		  Connection conn,
		  InputStream sqlIn,
		  String inputEncoding,
		  OutputStream sqlOut,
		  String outputEncoding,
          boolean loadSystemProperties)
		  throws UnsupportedEncodingException
    {
        LocalizedOutput lo =
          outputEncoding == null ?
                  LocalizedResource.getInstance().
                    getNewOutput(sqlOut)
                 :
                  LocalizedResource.getInstance().
                    getNewEncodedOutput(sqlOut, outputEncoding);

        Main ijE = new Main(false);

        LocalizedInput li = LocalizedResource.getInstance().
                getNewEncodedInput(sqlIn, inputEncoding);

        utilMain um = ijE.getutilMain(1, lo, loadSystemProperties);

        return um.goScript(conn, li);
    }

  private ij() { // no instances allowed
  }
  
  public static String getArg(String param, String[] args)
  {
	  return com.splicemachine.db.impl.tools.ij.util.getArg(param, args);
  }

  public static void getPropertyArg(String[] args) throws IOException
  {
	  com.splicemachine.db.impl.tools.ij.util.getPropertyArg(args);
  }

  public static java.sql.Connection startJBMS()
	  throws java.sql.SQLException, IllegalAccessException, ClassNotFoundException, InstantiationException
  {			
		return com.splicemachine.db.impl.tools.ij.util.startJBMS();
  }
}
