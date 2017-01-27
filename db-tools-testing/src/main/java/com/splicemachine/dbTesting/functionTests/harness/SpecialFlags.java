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

import java.util.Enumeration;
import java.util.Properties;
import java.util.StringTokenizer;

/**
    Parse testJavaFlags for RunTest
    These are special properties that might be
    set for a suite or test, and they can be
    either ij properties or server properties
    which is why they need to be parsed
*/

public class SpecialFlags
{

    public static Properties getSpecialProperties(Properties suiteProperties)
    {
        // Save any special properties which can be used by a
        // suite for ij or server properties (not in the usual list)

        // Define the "usual" properties to exclude from special props
        // FIXME: These should be in a file or something to make it
        // easier to add to this
        String[] excludeList = new String[32];
        excludeList[0] = "jvm";
        excludeList[1] = "classpath";
        excludeList[2] = "classpathServer";
        excludeList[3] = "framework";
        excludeList[4] = "usesystem";
        excludeList[5] = "useprocess";
        excludeList[6] = "outputdir";
        excludeList[7] = "replication";
        excludeList[8] = "keepfiles";
        excludeList[9] = "mtestdir";
        excludeList[10] = "suites";
        excludeList[11] = "searchCP";
        excludeList[12] = "useoutput";
        excludeList[13] = "suitename";
        excludeList[14] = "cleanfiles";
        excludeList[15] = "systemdiff";
        excludeList[16] = "jvmflags";
        excludeList[17] = "testJavaFlags";
        excludeList[18] = "ij.defaultResourcePackage";
        excludeList[19] = "outcopy";
        excludeList[20] = "verbose";
        excludeList[21] = "canondir";
        excludeList[22] = "timeout";
        excludeList[23] = "encryption";
        excludeList[24] = "javaCmd";
        excludeList[25] = "topreportdir";
        excludeList[26] = "jarfile";
        excludeList[27] = "upgradetest";
        excludeList[28] = "jdk12test";
        excludeList[29] = "jdk12exttest";
        excludeList[30] = "skipsed";
		excludeList[31] = "sourceEnv";

        Properties p = new Properties();

        for (Enumeration e = suiteProperties.propertyNames(); e.hasMoreElements();)
        {
            boolean exclude = false;
            String key = (String)e.nextElement();
            for ( int i = 0; i < excludeList.length; i++ )
            {
                if ( excludeList[i].equals(key) )
                {
                    exclude = true;
                    break;
                }
            }
            if ( exclude == false )
            {
                String value = suiteProperties.getProperty(key);
                p.put(key,value);
            }
        }
        return p;
    }

	public static void parse(String flags,
	    Properties ijProps, Properties srvProps)
	{
	    // flags is a list of key-value pairs separated by a ^;
	    // to be parsed and added to either ijProps or srvProps
        if (flags == null)
            flags = "";
	    StringTokenizer st = new StringTokenizer(flags, "^");
	    String str = "";
	    String key = "";
	    String value = "";
	    while (st.hasMoreTokens())
	    {
	        str = st.nextToken();
            // System.out.println("TOKEN:"+str);
	        key = str.substring( 0, str.indexOf("=") );
	        value = str.substring( (str.indexOf("=") + 1) );
	        if ( str.startsWith("derby") )
	        {
	            // This is a server property
	            // Note that some can have a list of values
	            if ( key.equals("derby.debug.true") ||
	                 key.equals("derby.infolog.streams") )
	            {
	                String currval = srvProps.getProperty(key);
	                if ( (currval != null) && (currval.length()>0) )
	                {
	                    value = value + "," + currval;
	                }
	            }
	            srvProps.put(key,value);
	        }
	        else
	            // This is an ij property
	            ijProps.put(key,value);
        }
	}

	// no instances permitted.
	private SpecialFlags(){}
}
