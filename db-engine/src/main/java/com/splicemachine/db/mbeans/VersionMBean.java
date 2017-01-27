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
* This interface defines a Standard MBean for exposing the version information
* of a running Derby component.
* 
* Refer to the getters of the interface for defined attributes. All attributes
* are read-only.
*
* The MBean does not define any operations.
 * <P>
 * Key properties for registered MBean:
 * <UL>
 * <LI> <code>type=Version</code>
 * <LI> <code>jar={db.jar|derbynet.jar}</code>
 * <LI> <code>system=</code><em>runtime system identifier</em> (see overview)
 * </UL>
 * <P>
 * If a security manager is installed these permissions are required:
 * <UL>
 * <LI> <code>SystemPermission("server", "monitor")</code> for version information
 * specific to derbynet.jar
 * <LI> <code>SystemPermission("engine", "monitor")</code> for version information
 * specific to db.jar
 * </UL>
 * @see com.splicemachine.db.security.SystemPermission
*/
public interface VersionMBean {
    // attributes
    
    public String getProductName();
    public String getProductTechnologyName();
    public String getProductVendorName();
    
    public int getMajorVersion();
    public int getMinorVersion();
    public int getMaintenanceVersion();
    
    /**
     * Return the full version string.
     * @return Full version string.
     */
    public String getVersionString();
    
    public String getBuildNumber();
    
    public boolean isBeta();
    public boolean isAlpha();
    
}
