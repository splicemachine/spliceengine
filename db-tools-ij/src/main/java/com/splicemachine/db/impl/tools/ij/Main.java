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

package com.splicemachine.db.impl.tools.ij;

import com.splicemachine.db.tools.JDBCDisplayUtil;
import com.splicemachine.db.iapi.tools.i18n.LocalizedResource;
import com.splicemachine.db.iapi.tools.i18n.LocalizedInput;
import com.splicemachine.db.iapi.tools.i18n.LocalizedOutput;

import java.io.FileInputStream;
import java.io.BufferedInputStream;
import java.io.FileOutputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.IOException;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;

/**
 * This is the controller for ij. It uses two parsers:
 * one to grab the next statement, and another to
 * see if it is an ij command, and if so execute it.
 * If it is not an ij command, it is treated as a JSQL
 * statement and executed against the current connection.
 * ijParser controls the current connection, and so contains
 * all of the state information for executing JSQL statements.
 * <p>
 * This was written to facilitate a test harness for language
 * functionality tests.
 *
 *
 */
public class Main {
	private utilMain utilInstance;

	/**
	 * ij can be used directly on a shell command line through
	 * its main program.
	 * @param args allows 1 file name to be specified, from which
	 *    input will be read; if not specified, stdin is used.
	 */
	public static void main(String[] args)
		throws IOException
	{
		mainCore(args, new Main(true));
	}

	public static void mainCore(String[] args, Main main)
		throws IOException
	{
		LocalizedInput in = null;
		InputStream in1 = null;
		Main me;
		String file;
		String inputResourceName;

		LocalizedResource langUtil = LocalizedResource.getInstance();
		LocalizedOutput out = langUtil.getNewOutput(System.out);

                // Validate arguments, check for --help.
		if (util.invalidArgs(args)) {
			util.Usage(out);
      		return;
		}

		// load the property file if specified
		util.getPropertyArg(args);

		// readjust output to db.ui.locale and db.ui.codeset if
                // they were loaded from a property file.
		langUtil.init();
		out = langUtil.getNewOutput(System.out);
                main.initAppUI();

		file = util.getFileArg(args);
		inputResourceName = util.getInputResourceNameArg(args);
		if (inputResourceName != null) {
			in = langUtil.getNewInput(util.getResourceAsStream(inputResourceName));
			if (in == null) {
				out.println(langUtil.getTextMessage("IJ_IjErroResoNo",inputResourceName));
				return;
			}
		} else if (file == null) {
			in = langUtil.getNewInput(System.in);
                        out.flush();
    	        } else {
                    try {
                    	final String inFile1 = file;
                    	in1 = (FileInputStream) AccessController.doPrivileged(new PrivilegedExceptionAction() {
            				public Object run() throws FileNotFoundException {
        						return new FileInputStream(inFile1);
            				}
            			});
                        if (in1 != null) {
                            in1 = new BufferedInputStream(in1, utilMain.BUFFEREDFILESIZE);
                            in = langUtil.getNewInput(in1);
                        }
                    } catch (PrivilegedActionException e) {
                        if (Boolean.getBoolean("ij.searchClassPath")) {
                            in = langUtil.getNewInput(util.getResourceAsStream(file));
                        }
                        if (in == null) {
                        out.println(langUtil.getTextMessage("IJ_IjErroFileNo",file));
            		  return;
                        }
                    }
                }

		final String outFile = util.getSystemProperty("ij.outfile");
		if (outFile != null && !outFile.isEmpty()) {
			LocalizedOutput oldOut = out;
			FileOutputStream fos = (FileOutputStream) AccessController.doPrivileged(new PrivilegedAction() {
				public Object run() {
					FileOutputStream out = null;
					try {
						out = new FileOutputStream(outFile);
					} catch (FileNotFoundException e) {
						out = null;
					}
					return out;
				}
			});
			out = langUtil.getNewOutput(fos);

			if (out == null)
			   oldOut.println(langUtil.getTextMessage("IJ_IjErroUnabTo",outFile));

		}

		// the old property name is deprecated...
		String maxDisplayWidth = util.getSystemProperty("maximumDisplayWidth");
		if (maxDisplayWidth==null)
			maxDisplayWidth = util.getSystemProperty("ij.maximumDisplayWidth");
		if (maxDisplayWidth != null && !maxDisplayWidth.isEmpty()) {
			try {
				int maxWidth = Integer.parseInt(maxDisplayWidth);
				JDBCDisplayUtil.setMaxDisplayWidth(maxWidth);
			}
			catch (NumberFormatException nfe) {
				out.println(langUtil.getTextMessage("IJ_IjErroMaxiVa", maxDisplayWidth));
			}
		}

		/* Use the main parameter to get to
		 * a new Main that we can use.
		 * (We can't do the work in Main(out)
		 * until after we do all of the work above
		 * us in this method.
		 */
		me = main.getMain(out);

		/* Let the processing begin! */
		me.go(in, out);
		if(in != null) {
			in.close();
		}
		if(out != null) {
			out.close();
		}
	}

	/**
	 * Get the right Main (according to
	 * the JDBC version.
	 *
	 * @return	The right main (according to the JDBC version).
	 */
	public Main getMain(LocalizedOutput out)
	{
		return new Main(out);
	}

	/**
	 * Get the right utilMain (according to
	 * the JDBC version.
	 *
	 * @return	The right utilMain (according to the JDBC version).
	 */
	public utilMain getutilMain(int numConnections, LocalizedOutput out)
	{
		return new utilMain(numConnections, out);
	}

    /**
	 * Get the right utilMain (according to
	 * the JDBC version. This overload allows the choice of whether
     * the system properties will be used or not.
	 *
	 * @return	The right utilMain (according to the JDBC version).
	 */
    public utilMain getutilMain(int numConnections, LocalizedOutput out, boolean loadSystemProperties)
	{
		return new utilMain(numConnections, out, loadSystemProperties);
	}

	/**
		Give a shortcut to go on the utilInstance so
		we don't expose utilMain.
	 */
	private void go(LocalizedInput in, LocalizedOutput out )
	{
		LocalizedInput[] inA = { in } ;
		utilInstance.go(inA, out);
	}

	/**
	 * create an ij tool waiting to be given input and output streams.
	 */
	public Main() {
		this(null);
	}

	public Main(LocalizedOutput out) {
		if (out == null) {
	        out = LocalizedResource.getInstance().getNewOutput(System.out);
		}
		utilInstance = getutilMain(1, out);
		utilInstance.initFromEnvironment();
	}

	/**
	 * This constructor is only used so that we
	 * can get to the right Main based on the
	 * JDBC version.  We don't do any work in
	 * this constructor and we only use this
	 * object to get to the right Main via
	 * getMain().
	 */
	public Main(boolean trash)
	{
	}
  private void initAppUI(){
    //To fix a problem in the AppUI implementation, a reference to the AppUI class is
    //maintained by this tool.  Without this reference, it is possible for the
    //AppUI class to be garbage collected and the initialization values lost.
    //langUtilClass = LocalizedResource.class;

		// adjust the application in accordance with db.ui.locale and db.ui.codeset
	LocalizedResource.getInstance();
  }

}
