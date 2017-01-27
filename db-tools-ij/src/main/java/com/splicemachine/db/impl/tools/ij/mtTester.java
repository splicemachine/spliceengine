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

package com.splicemachine.db.impl.tools.ij;

import java.io.IOException;
import java.io.FileNotFoundException;
import java.io.BufferedInputStream;
import java.util.Date;

import com.splicemachine.db.iapi.tools.i18n.LocalizedOutput;

/**
 * mtTester grabs test and runs them forever.
 * The spawner of tester is responsible for 
 * killing it.
 */
public class mtTester implements Runnable
{
	private mtTestSuite	suite;
	private String		name;
	private LocalizedOutput	log;
	private LocalizedOutput	out;
	private boolean		stop = false;
	private boolean   testOK = false;
							
	public mtTester(String name, mtTestSuite suite, LocalizedOutput out, LocalizedOutput log)
	{ 
		this.name = name;
		this.suite = suite;
		this.log = log;
		this.out = out;
		log.println("...initialized "+ name + " at " + new Date());
	}

	/**
	** Run until killed or until there is a problem.
	** If we get other than 'connection closed' we'll
	** signal that we recieved a fatal error before
	** quittiing; otherwise, we are silent.
	*/
	public void run()
	{
		int numIterations = 0;

		try 
		{
			mtTestCase testCase;
			BufferedInputStream	in;

			// loop until we get an error or
			// are killed.	
			while (!stop)
			{
				numIterations++;
				testCase = suite.grabTestCase();
				try 
				{
					in = testCase.initialize(suite.getRoot());
				} catch (FileNotFoundException e) 
				{
					System.out.println(e);
					return;
				}
				catch (IOException e)
				{
					System.out.println(e);
					return;
				}
	
				log.println(name + ": "+ testCase.getName() + " " + new Date());
				testCase.runMe(log, out, in);
			}
		}	
		catch (ijFatalException e)
		{

			/*
			** If we got connection closed (XJ010), we'll
			** assume that we were deliberately killed
			** via a Thread.stop() and it was caught by
			** jbms.  Otherwise, we'll print out an
			** error message.
			*/
			if (e.getSQLState() == null || !(e.getSQLState().equals("XJ010")))
			{
				log.println(name + ": TERMINATING due to unexpected error:\n"+e);
				throw new ThreadDeath();
			}
		}
		if (stop)
		{
			log.println(name + ": stopping on request after " + numIterations +
						" iterations");
			testOK = true;
		}
	}

	public void stop()
	{
		stop = true;
	}
	public boolean noFailure()
	{
		return testOK;
	}
}
