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

import java.util.Vector;
import java.util.Enumeration;

/**
 */
public class mtTestSuite
{
	private Vector cases;
	private Vector last;
	private Vector init;
	private mtTime time;
	private int numThreads;
	private String rootDir = null;


	mtTestSuite(int numThreads, mtTime time, 
			Vector initCases, Vector testCases, Vector finalCases)
	{
		this.numThreads = numThreads;
		this.time = time;
		this.cases = testCases;
		this.init = initCases;
		this.last = finalCases;
	}

	public void init()
	{
		boolean loadInitFailed = loadCases(init);
		boolean loadTestsFailed = loadCases(cases);
		boolean loadLastFailed = loadCases(last);

		if ((loadInitFailed) ||
			(loadTestsFailed) ||
			(loadLastFailed))
		{
			throw new Error("Initialization Error");
		}
	}

	/**
	** @return boolean indicates if there was a problem loading
	** 	the file
	*/
	private boolean loadCases(Vector cases)
	{
		if (cases == null)
			return false;

		boolean gotError = false;
		Enumeration e = cases.elements();
		mtTestCase tcase;
 
		while (e.hasMoreElements())
		{
			tcase = (mtTestCase)e.nextElement();
			try
			{
				tcase.initialize(rootDir);
			}
			catch (Throwable t)
			{
				gotError = true;
			}
		}

		return gotError;
	}

	public void setRoot(String rootDir)
	{
		this.rootDir = rootDir;
	}

	public String getRoot()
	{
		return rootDir;
	}

	public int getNumThreads()
	{
		return numThreads;
	}

	public Vector getCases()
	{
		return cases;
	}

	public Vector getInitCases()
	{
		return init;
	}

	public Vector getFinalCases()
	{
		return last;
	}

	public mtTime getTime()
	{
		return time;
	}

	public long getTimeMillis()
	{
		return ((time.hours * 360) +
				(time.minutes * 60) +
				(time.seconds)) * 1000;
	}

	public String toString()
	{
		StringBuilder str;
		int	len;
		int i;
	
		str = new StringBuilder("TEST CASES\nNumber of Threads: " + numThreads);
		str.append("\nTime: ").append(time);
		str.append("\nNumber of Initializers: ").append(init.size()).append("\n");
		for (i = 0, len = init.size(); i < len; i++)
		{
			str.append(init.elementAt(i).toString()).append("\n");
		}

		str.append("\nNumber of Cases: ").append(cases.size()).append("\n");
		for (i = 0, len = cases.size(); i < len; i++)
		{
			str.append(cases.elementAt(i).toString()).append("\n");
		}

		str.append("\nNumber of Final Cases: ").append(last.size()).append("\n");
		for (i = 0, len = last.size(); i < len; i++)
		{
			str.append(last.elementAt(i).toString()).append("\n");
		}

		return str.toString();
	}

	/*
	** Grab a test case.  Pick one randomly and
	** try to grab that case.  If we get it we are
	** done.  Otherwise, try try again.
	*/
	public mtTestCase grabTestCase() 
	{
		int numCases = cases.size();
		int caseNum;
		mtTestCase testCase;

		do
		{
			caseNum = (int)((java.lang.Math.random() * 1311) % numCases);
			testCase = (mtTestCase)cases.elementAt(caseNum);
		}
		while (!testCase.grab());
	
		return testCase;	
	}
}
