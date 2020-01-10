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
import java.io.*;


/**
 *
 * <Put Class Comments Here>
 */
public class JarUtil {

    /** 
     * Unjar a file into the specified directory.  This runs in a separate
     * process.  Note, your test needs security permissions to read user.dir
     * and to start a process for this to work.
     * 
     * @param jarpath - Path to jar file
     *
     * @param outputdir - The directory to unjar to.  If this is null,
     *    we user user.dir (the current directory)
     *
     */
    public static void unjar(String jarpath, String outputdir)
        throws ClassNotFoundException, IOException, InterruptedException
    {                
        if ( outputdir == null ) {
            outputdir = System.getProperty("user.dir");
        }
        File jarFile = new File((new File(outputdir, jarpath)).getCanonicalPath());

        // Now unjar the file
        String jarCmd = "jar xf " + jarFile.getPath();
        // Now execute the jar command
        Process pr = null;
        try
        {
            //System.out.println("Use process to execute: " + jarCmd);
            pr = Runtime.getRuntime().exec(jarCmd);

            pr.waitFor();
            //System.out.println("Process done.");
            pr.destroy();
        }
        finally {
            if (pr != null)
            {
                pr.destroy();
                pr = null;
            }
        }
    }
}
