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
package com.splicemachine.dbTesting.functionTests.tests.tools;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

import junit.framework.Test;
import junit.framework.TestSuite;

import com.splicemachine.dbTesting.junit.BaseJDBCTestCase;
import com.splicemachine.dbTesting.junit.Derby;
import com.splicemachine.dbTesting.junit.JDBC;
import com.splicemachine.dbTesting.junit.SystemPropertyTestSetup;
import com.splicemachine.dbTesting.junit.TestConfiguration;


public class ConnectWrongSubprotocolTest extends BaseJDBCTestCase {

    public ConnectWrongSubprotocolTest(String name) {
        super(name);
    }
    
    public static Test suite() {
        // Test does not run on J2ME
        if (JDBC.vmSupportsJSR169())
            return new TestSuite("empty: no support for Driver.sql.Manager with jsr 169");
        
        if (!Derby.hasTools())
            return new TestSuite("empty: no tools support");
        
    	Properties props = new Properties();        
        props.setProperty("ij.connection.wrongSubprotocol", "jdbc:noone:fruitfly;create=true");
        
        Test test = TestConfiguration.embeddedSuite(ConnectWrongSubprotocolTest.class);
        
    	return new SystemPropertyTestSetup(test, props);
    }
    
    public void testConnectWrongSubprotocolWithSystemProperty()
    		throws UnsupportedEncodingException, SQLException {
    	String emptyIjScript = "";
    	boolean useSystemProperties = true;
    	
    	checkConnectWrongSubprotocol(emptyIjScript, useSystemProperties);    	
    }
    
    public void testConnectWrongSubprotoctestolWithoutSystemProperty()
            throws UnsupportedEncodingException, SQLException {
        String ijScriptConnectWrongSubprotocol = "connect 'jdbc:noone:fruitfly;create=true';";
        boolean useSystemProperties = false;
        
        checkConnectWrongSubprotocol(ijScriptConnectWrongSubprotocol, useSystemProperties);
    }
    
    private void checkConnectWrongSubprotocol(String ijScript, boolean useSystemProperties)
            throws UnsupportedEncodingException, SQLException {
        String ijResult = runIjScript(ijScript, useSystemProperties);       
                assertTrue(ijResult.indexOf("08001") > -1);
        assertTrue(ijResult.indexOf("No suitable driver") > -1);        
    }

    private String runIjScript(String ijScript, boolean useSystemProperties) 
            throws UnsupportedEncodingException, SQLException {
        ByteArrayInputStream bais = 
        		new ByteArrayInputStream(ijScript.getBytes("US-ASCII"));
        ByteArrayOutputStream baos = new ByteArrayOutputStream(10 * 1024);
        Connection conn = getConnection();
        
        com.splicemachine.db.tools.ij.runScript(
                conn,
                bais,
                "US-ASCII",
                baos,
                "US-ASCII",
                useSystemProperties);
        
        if (!conn.isClosed() && !conn.getAutoCommit())
            conn.commit();

        return new String(baos.toByteArray(), "US-ASCII");
    }
}
