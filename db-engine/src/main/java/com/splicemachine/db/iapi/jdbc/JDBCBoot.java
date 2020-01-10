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
            if(Boolean.valueOf(PropertyUtil.getSystemProperty(Property.START_DRDA)))
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
