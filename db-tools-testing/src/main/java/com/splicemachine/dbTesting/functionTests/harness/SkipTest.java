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

package com.splicemachine.dbTesting.functionTests.harness;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;


/**
  Determine if the named test is one which should not be
  run in a particular framework (defined by the propFileName).
  For instance, there could be a nowl.properties for a list of
  tests which do not currently work under the WebLogic framework.
  */
public class SkipTest
{
 
    private SkipTest()
    {
    }

    public static boolean skipIt(String listFileName, String testName)
        throws Exception
    {
        boolean answer = false;
	    InputStream is =
		RunTest.loadTestResource("suites" + '/' + listFileName);
        if (is == null)
        {
            System.out.println("File not found: " + listFileName);
            answer = false;
            return answer;
        }
        
        // Create a BufferedReader to read the list of tests to skip
        BufferedReader listFile = new BufferedReader(new InputStreamReader(is, "UTF-8"));
        String str = "";
        // Read the list of tests to skip, compare to testName
        while ( (str = listFile.readLine()) != null )
        {
	       if ( (testName.equals(str)) )
	            answer = true;
	    }
        return answer;
    }
}
		
			
