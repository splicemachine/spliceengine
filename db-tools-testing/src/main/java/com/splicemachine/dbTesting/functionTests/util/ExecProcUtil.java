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
 * All such Splice Machine modifications are Copyright 2012 - 2017 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */
package com.splicemachine.dbTesting.functionTests.util;

import com.splicemachine.dbTesting.functionTests.harness.ProcessStreamResult;
import com.splicemachine.dbTesting.functionTests.harness.TimedProcess;
import java.util.Vector;
import java.io.BufferedOutputStream;
/**
 * Utility class to hold helper methods to exec new processes
 */
public class ExecProcUtil {
    
    /**
     * For each new exec process done, set 
     * timeout for ProcessStreamResult after which the thread that 
     * handles the streams for the process exits.  Timeout is in minutes. 
     * Note: timeout handling will only come into effect when 
     * ProcessStreamResult#Wait() is called
     */
    private static String timeoutMinutes = "2";
    
    /**
     * timeout in seconds for the processes spawned.
     */
    private static int timeoutSecondsForProcess = 180;
    
    /**
     * Execute the given command and dump the results to standard out
     *
     * @param args  command and arguments
     * @param vCmd  java command line arguments.
     * @param bos  buffered stream (System.out) to dump results to.
     * @exception Exception
     */
    public static void execCmdDumpResults(String[] args, Vector vCmd,
            BufferedOutputStream bos) throws Exception {
        // We need the process inputstream and errorstream
        ProcessStreamResult prout = null;
        ProcessStreamResult prerr = null;

        StringBuffer sb = new StringBuffer();

        for (int i = 0; i < args.length; i++) {
            sb.append(args[i] + " ");
        }
        System.out.println(sb.toString());
        int totalSize = vCmd.size() + args.length;
        String serverCmd[] = new String[totalSize];

        int i = 0;
        for (i = 0; i < vCmd.size(); i++)
            serverCmd[i] = (String) vCmd.elementAt(i);

        for (int j = 0; i < totalSize; i++)
            serverCmd[i] = args[j++];

        System.out.flush();
        bos.flush();

        // Start a process to run the command
        Process pr = Runtime.getRuntime().exec(serverCmd);

        // TimedProcess, kill process if process doesnt finish in a certain 
        // amount of time
        TimedProcess tp = new TimedProcess(pr);
        prout = new ProcessStreamResult(pr.getInputStream(), bos,
                timeoutMinutes);
        prerr = new ProcessStreamResult(pr.getErrorStream(), bos,
                timeoutMinutes);

        // wait until all the results have been processed
        boolean outTimedOut = prout.Wait();
        boolean errTimedOut = prerr.Wait();
        
        // wait for this process to terminate, upto a wait period
        // of 'timeoutSecondsForProcess'
        // if process has already been terminated, this call will 
        // return immediately.
        tp.waitFor(timeoutSecondsForProcess);
        pr = null;
        
        if (outTimedOut || errTimedOut)
            System.out.println(" Reading from process streams timed out.. ");

        System.out.flush();
    }
    
}
