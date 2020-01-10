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

/***
 * SysInfoLog
 * Purpose: For a Suite or Test run, write out the
 * sysinfo to the suite or test output file
 *
 ***/

import java.io.*;
import java.util.Vector;

public class SysInfoLog
{

    public SysInfoLog()
    {
    }

    // Write out sysinfo for a suite or test
    public void exec(String jvmName, String javaCmd, String classpath,
        String framework, PrintWriter pw, boolean useprocess)
        throws Exception
	{
        if ( useprocess == true )
        {
            // Create a process to run sysinfo
    		Process pr = null;
			jvm javavm = null; // to quiet the compiler
    		try
    		{
                // Create the command line
                //System.out.println("jvmName: " + jvmName);
                if ( (jvmName == null) || (jvmName.isEmpty()) )
                    jvmName = "jdk13";
                else if (jvmName.startsWith("jdk13"))
                    jvmName = "jdk13";

				javavm = jvm.getJvm(jvmName);
                if (javaCmd != null)
                    javavm.setJavaCmd(javaCmd);
				
                if (javavm == null) System.out.println("WHOA, javavm is NULL");
                if (javavm == null) pw.println("WHOA, javavm is NULL");

                if ( (classpath != null) && (!classpath.isEmpty()) )
                {
                    javavm.setClasspath(classpath);
                }

				Vector v = javavm.getCommandLine();
                v.addElement("com.splicemachine.db.tools.sysinfo");
                // Now convert the vector into a string array
                String[] sCmd = new String[v.size()];
                for (int i = 0; i < v.size(); i++)
                {
                    sCmd[i] = (String)v.elementAt(i);
                    //System.out.println(sCmd[i]);
                }
                
                pr = Runtime.getRuntime().exec(sCmd);

                // We need the process inputstream to capture into the output file
                BackgroundStreamDrainer stdout =
                    new BackgroundStreamDrainer(pr.getInputStream(), null);
                BackgroundStreamDrainer stderr =
                    new BackgroundStreamDrainer(pr.getErrorStream(), null);

                pr.waitFor();
                String result = HandleResult.handleResult(pr.exitValue(),
                    stdout.getData(), stderr.getData(), pw);
                pw.flush();

                if ( (framework != null) && (!framework.isEmpty()) )
                {
                    pw.println("Framework: " + framework);
                }

                pr.destroy();
                pr = null;
            }
            catch(Throwable t)
            {
                if (javavm == null) System.out.println("WHOA, javavm is NULL");
                if (javavm == null) pw.println("WHOA, javavm is NULL");
                System.out.println("Process exception: " + t);
                pw.println("Process exception: " + t);
                t.printStackTrace(pw);
                if (pr != null)
                {
                    pr.destroy();
                    pr = null;
                }
            }
        }
        else
        {
            // For platforms where process exec fails or hangs
            // useprocess=false and attempt to get some info
            /*
            pw.println(com.splicemachine.db.impl.tools.sysinfo.Main.javaSep);
            com.splicemachine.db.impl.tools.sysinfo.Main.reportCloudscape(pw);
            pw.println(com.splicemachine.db.impl.tools.sysinfo.Main.jbmsSep);
            com.splicemachine.db.impl.tools.sysinfo.Main.reportDerby(pw);
            pw.println(com.splicemachine.db.impl.tools.sysinfo.Main.licSep);
            com.splicemachine.db.impl.tools.sysinfo.Main.printLicenseFile(pw);
            */
        }
    }
}

