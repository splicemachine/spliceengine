/*

   Derby - Class com.splicemachine.db.iapi.jdbc.JDBCBoot

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

package com.splicemachine.db.iapi.jdbc;

import com.splicemachine.db.iapi.reference.Property;
import com.splicemachine.db.iapi.reference.MessageId;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.property.PropertyUtil;
import com.splicemachine.db.iapi.services.monitor.Monitor;
import com.splicemachine.db.jdbc.InternalDriver;

import java.util.Properties;
import java.io.PrintStream;

/**
	A class to boot a Derby system that includes a JDBC driver.
	Should be used indirectly through JDBCDriver or JDBCServletBoot
	or any other useful booting mechanism that comes along.
*/
public class JDBCBoot {

	private Properties bootProperties;

    private static final String NETWORK_SERVER_AUTOSTART_CLASS_NAME = "com.splicemachine.db.iapi.jdbc.DRDAServerStarter";

	public JDBCBoot() {
		bootProperties = new Properties();
	}

	void addProperty(String name, String value) {
		bootProperties.put(name, value);
	}

	/**
		Boot a system requesting a JDBC driver but only if there is
		no current JDBC driver that is handling the required protocol.

	*/
	public void boot(String protocol, PrintStream logging) {

		if (InternalDriver.activeDriver() == null)
		{

			// request that the InternalDriver (JDBC) service and the
			// authentication service be started.
			//
			addProperty("derby.service.jdbc", "com.splicemachine.db.jdbc.InternalDriver");
			addProperty("derby.service.authentication", AuthenticationService.MODULE);

			Monitor.startMonitor(bootProperties, logging);

            /* The network server starter module is started differently from other modules because
             * 1. its start is conditional, depending on a system property, and PropertyUtil.getSystemProperty
             *    does not work until the Monitor has started,
             * 2. we do not want the server to try to field requests before Derby has booted, and
             * 3. if the module fails to start we want to log a message to the error log and continue as
             *    an embedded database.
             */
            if( Boolean.valueOf(PropertyUtil.getSystemProperty(Property.START_DRDA)).booleanValue())
            {
                try
                {
                    Monitor.startSystemModule( NETWORK_SERVER_AUTOSTART_CLASS_NAME);
                }
                catch( StandardException se)
                {
                    Monitor.logTextMessage( MessageId.CONN_NETWORK_SERVER_START_EXCEPTION,
                                            se.getMessage());
                }
            }
		}
	}
}
