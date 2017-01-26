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

package com.splicemachine.dbTesting.system.nstest.utils;

import java.util.Date;

import com.splicemachine.dbTesting.system.nstest.NsTest;

/**
 * MemCheck - a background thread that prints current memory usage
 */
public class MemCheck extends Thread {

	int delay = 200000;

	public boolean stopNow = false;

	public MemCheck() {
	}

	public MemCheck(int num) {
		delay = num;
	}

	/*
	 * Implementation of run() method to check memory
	 * 
	 */
	public void run() {
		while (stopNow == false) {
			try {
				showmem();
				sleep(delay);
                
				// first check if there are still active tester threads, so 
				// we do not make backups on an unchanged db every 10 mins for
				// the remainder of MAX_ITERATIONS.
				if (NsTest.numActiveTestThreads() != 0 && NsTest.numActiveTestThreads() > 1)
				{
					continue;
				}
				else
				{
					System.out.println("no more test threads, finishing memcheck thread also");
					showmem();
					stopNow=true;
				}
			} catch (java.lang.InterruptedException ie) {
				System.out.println("memcheck: unexpected error in sleep");
			}
		}
	}

	/*
	 * Print the current memory status
	 */
	public static void showmem() {
		Runtime rt = null;
		Date d = null;
		rt = Runtime.getRuntime();
		d = new Date();
		System.out.println("total memory: " + rt.totalMemory() + " free: "
				+ rt.freeMemory() + " " + d.toString());

	}

	public static void main(String argv[]) {
		System.out.println("memCheck starting");
		MemCheck mc = new MemCheck();
		mc.run();
	}
}
