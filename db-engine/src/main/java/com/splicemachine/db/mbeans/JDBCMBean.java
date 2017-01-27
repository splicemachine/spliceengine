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

package com.splicemachine.db.mbeans;

/** 
 * Management and information for the embedded JDBC driver.
 * <P>
 * Key properties for registered MBean:
 * <UL>
 * <LI> <code>type=JDBC</code>
 * <LI> <code>system=</code><em>runtime system identifier</em> (see overview)
 * </UL>
*/
public interface JDBCMBean {
    
    /**
     * Get the JDBC driver's implementation level
     */
    public String getDriverLevel();

    /**
     * Return the JDBC driver's major version.
     * @return major version
     * @see java.sql.Driver#getMajorVersion()
     */
    public int getMajorVersion();
    
    /**
     * Return the JDBC driver's minor version.
     * @return minor version
     * @see java.sql.Driver#getMinorVersion()
     */
    public int getMinorVersion();
    
    /**
     * Is the JDBC driver compliant.
     * @return compliance state
     * @see java.sql.Driver#jdbcCompliant()
     */
    public boolean isCompliantDriver();
    
    /**
     * Does the driver accept the passed in JDBC URL
     * @param url JDBC URL to check.
     * @return True if it supports it, false otherwise.
     * @see java.sql.Driver#acceptsURL(String)
     */
    public boolean acceptsURL(String url);

}
