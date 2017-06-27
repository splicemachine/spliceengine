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

package com.splicemachine.db.impl.services.jmx;

import java.lang.management.ManagementFactory;
import java.security.AccessControlException;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;

import javax.management.JMException;
import javax.management.MBeanInfo;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.management.StandardMBean;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.info.Version;
import com.splicemachine.db.iapi.services.jmx.ManagementService;
import com.splicemachine.db.iapi.services.monitor.ModuleControl;
import com.splicemachine.db.iapi.services.monitor.Monitor;
import com.splicemachine.db.mbeans.ManagementMBean;
import com.splicemachine.db.mbeans.VersionMBean;
import com.splicemachine.db.security.SystemPermission;

/** 
 * This class implements the ManagementService interface and provides a simple
 * management and monitoring service.
 * 
 * An mbean registered with this service remains until it is unregistered.
 * While registered with this service it may be registered and unregistered
 * with the jmx service a number of times.
 *
 * @see com.splicemachine.db.iapi.services.jmx.ManagementService
 */
public final class JMXManagementService implements ManagementService, ModuleControl {

    /**
     * Platform MBean server, from ManagementFactory.getPlatformMBeanServer().
     * If not null then this service has registered mbeans with the
     * plaform MBean server.
     * If null then this service either has no mbeans registered
     * or one mbean registered (representing itself).
     */
    private MBeanServer mbeanServer;
   
    /**
     * The set of mbeans registered to this service by
     * Derby's code. These beans are registered with
     * the platform mbean server if mbeanServer is not null.
     */
    private Map<ObjectName,StandardMBean> registeredMbeans;
    
    /**
     * If this object is registered as a management mbean
     * then myManagementBean represents its name. This will
     * be non-null when db.system.jmx is true.
     */
    private ObjectName myManagementBean;
    
    private MBeanServer myManagementServer;
    
    /**
     * Runtime value to disambiguate
     * multiple Derby systems in the same virtual machine but
     * different class loaders. Set as the system attribute in
     * the ObjectName for all MBeans registered.
     */
    private String systemIdentifier;

    public JMXManagementService() {

    }

    /**
     * Start the management service if db.system.jmx is true.
     * <P>
     * Starting the service means:
     * <UL>
     * <LI> getting the platform MBeanServer which may require starting it
     * <LI> registering a Version mbean representing the system
     * </UL>
     */
    public synchronized void boot(boolean create, Properties properties)
            throws StandardException {
        
        registeredMbeans = new HashMap<>();
        
        systemIdentifier =
            Monitor.getMonitor().getUUIDFactory().createUUID().toString();
        
        findServer();
             
        myManagementBean = (ObjectName) registerMBean(this,
                ManagementMBean.class,
                "type=Management");
        myManagementServer = mbeanServer;
        
        registerMBean(
                new Version(
                        Monitor.getMonitor().getEngineVersion(),
                        SystemPermission.ENGINE),
                VersionMBean.class,
                "type=Version,jar=db.jar");
    }
    
    public synchronized void stop() {
        
        // If we are currently not registering any mbeans
        // then we might still have this registered as
        // a management mbean. Need to explicitly remove this
        // using the mbean server that created it, which
        // possibly could not be the same as the current server.
        if (mbeanServer == null && myManagementBean != null)
        {
            mbeanServer = myManagementServer;
            unregisterMBean(myManagementBean);
            mbeanServer = null;
        }

        // Need a copy of registeredMbeans since unregisterMBean will remove
        // items from registeredMbeans and thus invalidate any iterator
        // on it directly.
        for (ObjectName mbeanName :
                new HashSet<>(registeredMbeans.keySet()))
            unregisterMBean(mbeanName);
        
        mbeanServer = null;
        
        // registeredMbeans == null indicates service is not active
        registeredMbeans = null;
        
        myManagementServer = null;
        systemIdentifier = null;
    }

    /**
     * Initialize the management service by obtaining the platform
     * MBeanServer and registering system beans. Separate from
     * boot() to allow future changes where the jmx management
     * can be enabled on the fly.
     */
    private synchronized void findServer() {
        
        try {
            mbeanServer = AccessController
                    .doPrivileged(new PrivilegedAction<MBeanServer>() {
                        public MBeanServer run() {
                            return ManagementFactory.getPlatformMBeanServer();
                        }
                    });
            
        } catch (SecurityException se) {
            // Ignoring inability to create or
            // find the mbean server. MBeans can continue
            // to be registered with this service and
            // startMangement() can be called to get
            // them registered with JMX if someone else
            // starts the MBean server.
        }
    }

    /**
     * Registers an MBean with the MBean server as a StandardMBean.
     * Use of the StandardMBean allows the implementation details
     * of Derby's mbeans to be hidden from JMX, thus only exposing
     * the MBean's interface in com.splicemachine.db.mbeans.
     * 
     * 
     * @param bean The MBean to wrap with a StandardMBean and register
     * @param beanInterface The management interface for the MBean.
     * @param keyProperties The String representation of the MBean's key properties,
     * they will be added into the ObjectName with Derby's domain. Key
     * type should be first with a short name for the bean, typically the
     * class name without the package.
     * 
     */
    public synchronized Object registerMBean(final Object bean,
            final Class beanInterface,
            final String keyProperties)
            throws StandardException {

        try {
            final ObjectName beanName = new ObjectName(
                    DERBY_JMX_DOMAIN + ":" + keyProperties
                    + ",system=" + systemIdentifier);

            @SuppressWarnings("unchecked")
            final StandardMBean standardMBean =
                new StandardMBean(bean, beanInterface) {
                
                /**
                 * Hide the implementation name from JMX clients
                 * by providing the interface name as the class
                 * name for the MBean. Allows the permissions
                 * in a policy file to be granted to the public
                 * MBean interfaces.
                 */
                protected String getClassName(MBeanInfo info) {
                    return beanInterface.getName();
                }
                
            };
                // new StandardMBean(bean, beanInterface);
            
            registeredMbeans.put(beanName, standardMBean);
            if (mbeanServer != null)
                jmxRegister(standardMBean, beanName);
            
            return beanName;
        
        } catch (JMException jme) {
            throw StandardException.plainWrapException(jme);
        }
    }
    
    /**
     * Register an mbean with the platform mbean server.
     */
    private void jmxRegister(final StandardMBean standardMBean,
            final ObjectName beanName) throws JMException
    {
        // Already registered? Can happen if we don't have permission
        // to unregister the MBeans.
        if (mbeanServer.isRegistered(beanName))
            return;
            
        try {

            AccessController
               .doPrivileged(new PrivilegedExceptionAction<Object>() {

                    public Object run() throws JMException {
                        mbeanServer.registerMBean(standardMBean, beanName);
                        return null;
                    }

                });

        } catch (PrivilegedActionException pae) {
            throw (JMException) pae.getException();
        } catch (SecurityException se) {
            // If we can't register the MBean then so be it.
            // The application can later enabled the MBeans
            // by using com.splicemachine.db.mbeans.Management
        }
    }
    
    /**
     * Unregister an mbean using an object previous returned from registerMBean.
     */
    public void unregisterMBean(Object mbeanIdentifier)
    {
        if (mbeanIdentifier == null)
            return;
        unregisterMBean((ObjectName) mbeanIdentifier);
    }
    
    /**
     * Unregisters an mbean from this service and JMX plaform server
     * @param mbeanName Bean to unregister.
     */
    private synchronized void unregisterMBean(final ObjectName mbeanName)
    {
        //Has this service been shut down?
        if (registeredMbeans == null)
            return;

        if (registeredMbeans.remove(mbeanName) == null)
            return;
        
        if (mbeanServer == null)
            return;
        
        jmxUnregister(mbeanName);
    }
    
    /**
     * Unregister an mbean from the JMX plaform server
     * but leave it registered to this service. This
     * is so that if jmx is reenabled we can reestablish
     * all vaid mbeans (that are still registered with this service).
     * @param mbeanName
     */
    private void jmxUnregister(final ObjectName mbeanName) {

        if (!mbeanServer.isRegistered(mbeanName))
            return;

        try {

            AccessController
                    .doPrivileged(new PrivilegedExceptionAction<Object>() {

                        public Object run() throws JMException {
                            mbeanServer.unregisterMBean(mbeanName);
                            return null;
                        }

                    });

        } catch (PrivilegedActionException | SecurityException pae) {
            // TODO - this is called on shutdown where
            // we don't really care about errors.
            // JMException jme = (JMException) pae.getException();
            //if (!(jme instanceof InstanceNotFoundException))
                // throw StandardException.plainWrapException(jme);
        }
    }

    public synchronized boolean isManagementActive() {
        return mbeanServer != null;
    }

    public synchronized void startManagement() {
        
        //Has this service been shut down?
        if (registeredMbeans == null)
            return;
        
        checkJMXControl();
        
        // Already active?
        if (isManagementActive())
            return;
        
        findServer();
        
        // If we can't find the server then we can't register.
        if (mbeanServer == null)
            return;
        
        for (Map.Entry<ObjectName, StandardMBean> objectNameStandardMBeanEntry : registeredMbeans.entrySet())
        {
            // If we registered this as a management bean
            // then leave it registered to allow the mbeans
            // to be re-registered with JMX
            if (objectNameStandardMBeanEntry.getKey().equals(myManagementBean) &&
                    mbeanServer.isRegistered(myManagementBean))
                continue;
            
            try {
                jmxRegister(objectNameStandardMBeanEntry.getValue(), objectNameStandardMBeanEntry.getKey());
            } catch (JMException e) {
                // TODO - what to do here?
            }
        }
    }

    public synchronized void stopManagement() {
        
        // Has this service been shut down?
        if (registeredMbeans == null)
            return;
        
        checkJMXControl();
        
        if (isManagementActive()) {
            for (ObjectName mbeanName : registeredMbeans.keySet())
            {
                // If we registered this as a management bean
                // then leave it registered to allow the mbeans
                // to be re-registered with JMX
                if (mbeanName.equals(myManagementBean))
                    continue;
                jmxUnregister(mbeanName);
            }
            mbeanServer = null;
        }
    }
    
    /**
     * Control permission (permissions are immutable).
     */
    private final static SystemPermission CONTROL =
        new SystemPermission(
                SystemPermission.JMX, SystemPermission.CONTROL);

    /**
     * Require SystemPermission("jmx", "control") to change
     * the management state.
     */
    private void checkJMXControl() {
        try {
            if (System.getSecurityManager() != null)
                AccessController.checkPermission(CONTROL);
        } catch (AccessControlException e) {
            // Need to throw a simplified version as AccessControlException
            // will have a reference to Derby's SystemPermission which most likely
            // will not be available on the client.
            throw new SecurityException(e.getMessage());
        }
    }

    public synchronized String getSystemIdentifier() {
        return systemIdentifier;
    }
}
