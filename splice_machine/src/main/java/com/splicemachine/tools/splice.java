/*

   Derby - Class com.splicemachine.db.tools.ij

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

package com.splicemachine.tools;

import com.splicemachine.db.iapi.tools.i18n.LocalizedInput;
import com.splicemachine.db.iapi.tools.i18n.LocalizedOutput;
import com.splicemachine.db.iapi.tools.i18n.LocalizedResource;

import com.splicemachine.db.impl.tools.ij.Main;
import com.splicemachine.db.impl.tools.ij.utilMain;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.sql.Connection;

/**
	
	splice is Derby's interactive JDBC scripting tool.
	It is a simple utility for running scripts against a Derby database.
	You can also use it interactively to run ad hoc queries.
	ij provides several commands for ease in accessing a variety of JDBC features.
	<P>

	To run from the command line enter the following:
	<p>
	java [options] com.splicemachine.db.tools.splice [arguments]
	<P>
	splice can also be used with any database server that supports a JDBC driver.
*/
public class splice {

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
	  return splice.runScript(conn, sqlIn, inputEncoding, sqlOut, outputEncoding,false);
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

  private splice() { // no instances allowed
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
