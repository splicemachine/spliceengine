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

package com.splicemachine.db.mbeans;

/**
 * JMX MBean inteface to control visibility of Derby's MBeans.
 * When Derby boots it attempts to register its MBeans.
 * It may fail due to lack of valid permissions.
 * If Derby does not register its MBeans then an
 * application may register the Management implementation
 * of ManagementMBean itself and use it to start Derby's
 * JMX management.
 * <P>
 * Key properties for registered MBean when registered by Derby:
 * <UL>
 * <LI> <code>type=Management</code>
 * <LI> <code>system=</code><em>runtime system identifier</em> (see overview)
 * </UL>
 * 
 * @see Management
 * @see ManagementMBean#getSystemIdentifier()
 */
public interface ManagementMBean {
    
    /**
     * Is Derby's JMX management active. If active then Derby
     * has registered MBeans relevant to its current state.
     * @return true Derby has registered beans, false Derby has not
     * registered any beans.
     */
    boolean isManagementActive();
    
    /**
     * Get the system identifier that this MBean is managing.
     * The system identifier is a runtime value to disambiguate
     * multiple Derby systems in the same virtual machine but
     * different class loaders.
     * 
     * @return Runtime identifier for the system, null if Derby is not running.
     */
    String getSystemIdentifier();
    
    /**
     * Inform Derby to start its JMX management by registering
     * MBeans relevant to its current state. If Derby is not
     * booted then no action is taken.
     * <P>
     * Require <code>SystemPermission("jmx", "control")</code> if a security
     * manager is installed.
     * 
     * @see com.splicemachine.db.security.SystemPermission
     */
    void startManagement();
    
    /**
     * Inform Derby to stop its JMX management by unregistering
     * its MBeans. If Derby is not booted then no action is taken.
     * <P>
     * Require <code>SystemPermission("jmx", "control")</code> if a security
     * manager is installed.
     * 
     * @see com.splicemachine.db.security.SystemPermission
     */
    void stopManagement();
}
