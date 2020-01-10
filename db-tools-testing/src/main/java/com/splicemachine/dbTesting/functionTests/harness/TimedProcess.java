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

/**
 * This class is a wrapper of Process to provide a waitFor() method
 * that forcibly terminates the process if it does not
 * complete within the specified time.
 *
 * 
 */
public class TimedProcess
{

  private Process process;

  public TimedProcess(Process process)
  {
    this.process = process;
  }

  public int waitFor(int sec)
  {
    int exitValue = -1;

    // Create a thread to wait for the process to die
    WaitForProcess t = new WaitForProcess(process);
    t.start();
    
    // Give the process sec seconds to terminate
    try
    {
      t.join(sec * 1000);

      // Otherwise, interrupt the thread...
      if (t.isAlive())
      {
        t.interrupt();
        
        System.err.println("Server Process did not complete in time. Destroying...");
        // ...and destroy the process with gusto
        process.destroy();
      }
      else
      {
        // process shut down, so it is right to get the exit value from it
        exitValue = t.getProcessExitValue();
      }
    }
    catch (InterruptedException e)
    {
      e.printStackTrace();
    }
  
    return exitValue;
  }
} // public class TimedProcess


class WaitForProcess
  extends Thread
{
  private Process process;
  private int processExitValue;
  
  public WaitForProcess(Process process)
  {
    this.process = process;
  }

  public int getProcessExitValue()
  {
    return processExitValue;
  }

  public void run()
  {
    // Our whole goal in life here is to waitFor() the process.
    // However, we're actually going to catch the InterruptedException for it!
    try
    {
      processExitValue = process.waitFor();
    }
    catch (InterruptedException e)
    {
      // Don't do anything here; the thread will die of natural causes
    }
  }
} // class WaitForProcess

