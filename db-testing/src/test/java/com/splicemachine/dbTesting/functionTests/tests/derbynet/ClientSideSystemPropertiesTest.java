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

package com.splicemachine.dbTesting.functionTests.tests.derbynet;

import java.io.File;
import java.security.AccessController;
import java.util.Properties;

import junit.framework.Test;
import junit.framework.TestSuite;

import com.splicemachine.dbTesting.junit.BaseJDBCTestCase;
import com.splicemachine.dbTesting.junit.SystemPropertyTestSetup;
import com.splicemachine.dbTesting.junit.TestConfiguration;

/** The test of the jvm properties for enabling client side tracing(DERBY-1275)
  */
public class ClientSideSystemPropertiesTest extends BaseJDBCTestCase { 
	/** Establish a connection and that should start client side tracing
     *  because we have set the system properties to enable client side
     *  tracing. */
    public void testConnection() throws Exception {
        getConnection().setAutoCommit(false);
        //Make sure the connection above created a trace file. This check is 
        //made in the privilege block below by looking inside the 
        //trace Directory and making sure the file count is greater than 0.
        AccessController.doPrivileged
		    (new java.security.PrivilegedAction(){
		    	public Object run(){
		    		File dir = new File(getSystemProperty("derby.client.traceDirectory"));
		    		int fileCounter = 0;
    	            File[] list = dir.listFiles();
    	            File tempFile;
    	            for (;fileCounter<list.length; fileCounter++)
    	            	tempFile = list[fileCounter];
    	            junit.framework.Assert.assertTrue(fileCounter>0);
    	            return null;
    		    }
    		}	 
    	    );
    }
    
    /** If the trace Directory doesn't exist then create one. If there is one
     *  already there, then delete everything under it. */
    protected void setUp() throws Exception
    {
    	AccessController.doPrivileged(
    			new java.security.PrivilegedAction(){
    				public Object run(){
    					File dir = new File(getSystemProperty("derby.client.traceDirectory"));
    					if (dir.exists() == false) //create the trace Directory
    						junit.framework.Assert.assertTrue(dir.mkdir() || dir.mkdirs());
    					else {//cleanup the trace Directory which already exists
    						int fileCounter = 0;
    						File[] list = dir.listFiles();
    						File tempFile;
    						for (;fileCounter<list.length; fileCounter++) {
    							tempFile = list[fileCounter];
    							tempFile.delete();
        					}
		        }
	            return null;
		    }
		}	 
	    );
    }
    
    /** Delete the trace Directory so that the test environment is clean for the
     *  next test run. */
    protected void tearDown() throws Exception
    {
        super.tearDown();
        
        removeDirectory(getSystemProperty("derby.client.traceDirectory"));
    }
    
    /* ------------------- end helper methods  -------------------------- */
    public ClientSideSystemPropertiesTest(String name) {
        super(name);
    }

    /*
     * Set the system properties related to client side tracing.
     */
    public static Test suite() {
        //Create the traceDirectory required by the tests in this class
    	Properties traceRelatedProperties = new Properties();
        traceRelatedProperties.setProperty("derby.client.traceLevel", "64");
        traceRelatedProperties.setProperty("derby.client.traceDirectory", "TraceDir");
        Test suite = TestConfiguration.clientServerSuite(ClientSideSystemPropertiesTest.class);
        return new SystemPropertyTestSetup(suite, traceRelatedProperties); 
    }
    
}
