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

import java.io.*;
import java.util.StringTokenizer;

/**
  For tests which require support files.
  Copy them to the output directory for the test.
  */
public class CopySuppFiles
{

	public static void main(String[] args) throws Exception
	{
	}

	public static void copyFiles(File outDir, String suppFiles)
	    throws ClassNotFoundException, IOException
	{
	    // suppFiles is a comma separated list of the files
	    StringTokenizer st = new StringTokenizer(suppFiles,",");
	    String scriptName = ""; // example: test/math.sql
	    InputStream is = null; // To be used for each support file
        while (st.hasMoreTokens())
        {
            scriptName = st.nextToken();
    	    File suppFile = null;
    	    String fileName = "";
    	    // Try to locate the file
            is = RunTest.loadTestResource(scriptName); 
    		if ( is == null )
    			System.out.println("Could not locate: " + scriptName);
    		else
    		{
    		    // Copy the support file so the test can use it
    			int index = scriptName.lastIndexOf('/');
    			fileName = scriptName.substring(index+1);
 //   			suppFile = new File((new File(outDir, fileName)).getCanonicalPath());

		//these calls to getCanonicalPath catch IOExceptions as a workaround to
		//a bug in the EPOC jvm. 
    		try {suppFile = new File((new File(outDir, fileName)).getCanonicalPath());}
		catch (IOException e) {
		    File f = new File(outDir, fileName);
		    FileWriter fw = new FileWriter(f);
		    fw.close();
		    suppFile = new File(f.getCanonicalPath());
		}
                // need to make a guess so we copy text files to local encoding
                // on non-ascii systems...
		        if ((fileName.indexOf("sql") > 0) || (fileName.indexOf("txt") > 0) || (fileName.indexOf(".view") > 0) || (fileName.indexOf(".policy") > 0) || (fileName.indexOf(".multi") > 0) || (fileName.indexOf(".properties") > 0))
                {
                    BufferedReader inFile = new BufferedReader(new InputStreamReader(is, "UTF-8"));
                    PrintWriter pw = new PrintWriter
                       ( new BufferedWriter(new FileWriter(suppFile), 10000), true );
                    int c;
                    while ((c = inFile.read()) != -1)
                        pw.write(c);
                    pw.flush();
                    pw.close();
                }
                else
                {
                    FileOutputStream fos = new FileOutputStream(suppFile);
                    byte[] data = new byte[4096];
                    int len;
                    while ((len = is.read(data)) != -1)
                    {
                        fos.write(data, 0, len);
                    }
                    fos.close();
                }
    		}
        }
	}
}
