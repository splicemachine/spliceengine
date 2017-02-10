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

package com.splicemachine.db.impl.services.timer;

import com.splicemachine.db.iapi.services.timer.TimerFactory;
import com.splicemachine.db.iapi.services.monitor.ModuleControl;
import com.splicemachine.db.iapi.error.StandardException;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Timer;
import java.util.Properties;


/**
 * This class implements the TimerFactory interface.
 * It creates a singleton Timer instance.
 *
 * The class implements the ModuleControl interface,
 * because it needs to cancel the Timer at system shutdown.
 *
 * @see TimerFactory
 * @see ModuleControl
 */
public class SingletonTimerFactory
    implements
        TimerFactory,
        ModuleControl
{
    /**
     * Singleton Timer instance.
     */
    private Timer singletonTimer;

    /**
     * Initializes this TimerFactory with a singleton Timer instance.
     */
    public SingletonTimerFactory()
    {
        /**
         * Even though we implement the ModuleControl interface,
         * we initialize the object here rather than in boot, since
         * a) We avoid synchronizing access to singletonTimer later
         * b) We don't need any properties
         */
         // DERBY-3745 We want to avoid leaking class loaders, so 
         // we make sure the context class loader is null before
         // creating the thread
        ClassLoader savecl = null;
        boolean hasGetClassLoaderPerms = false;
        try {
            savecl = (ClassLoader)AccessController.doPrivileged(
            new PrivilegedAction() {
                public Object run()  {
                    return Thread.currentThread().getContextClassLoader();
                }
            });
            hasGetClassLoaderPerms = true;
        } catch (SecurityException se) {
            // Ignore security exception. Versions of Derby before
            // the DERBY-3745 fix did not require getClassLoader 
            // privs.  We may leak class loaders if we are not
            // able to do this but we can't just fail.
        }
        if (hasGetClassLoaderPerms)
            try {
                AccessController.doPrivileged(
                new PrivilegedAction() {
                    public Object run()  {
                        Thread.currentThread().setContextClassLoader(null);
                        return null;
                    }
                });
            } catch (SecurityException se) {
                // ignore security exception.  Earlier versions of Derby, before the 
                // DERBY-3745 fix did not require setContextClassloader permissions.
                // We may leak class loaders if we are not able to set this, but 
                // cannot just fail.
            }
        singletonTimer = new Timer(true); // Run as daemon
        if (hasGetClassLoaderPerms)
            try {
                final ClassLoader tmpsavecl = savecl;
                AccessController.doPrivileged(
                new PrivilegedAction() {
                    public Object run()  {
                        Thread.currentThread().setContextClassLoader(tmpsavecl);
                        return null;
                    }
                });
            } catch (SecurityException se) {
                // ignore security exception.  Earlier versions of Derby, before the 
                // DERBY-3745 fix did not require setContextClassloader permissions.
                // We may leak class loaders if we are not able to set this, but 
                // cannot just fail.
            }
    }

    /**
     * Returns a Timer object that can be used for adding TimerTasks
     * that cancel executing statements.
     *
     * Implements the TimerFactory interface.
     *
     * @return a Timer object for cancelling statements.
     *
     * @see TimerFactory
     */
    public Timer getCancellationTimer()
    {
        return singletonTimer;
    }

    /**
     * Currently does nothing, singleton Timer instance is initialized
     * in the constructor.
     *
     * Implements the ModuleControl interface.
     *
     * @see ModuleControl
     */
    public void boot(boolean create, Properties properties)
        throws
            StandardException
    {
        // Do nothing, instance already initialized in constructor
    }

    /**
     * Cancels the singleton Timer instance.
     * 
     * Implements the ModuleControl interface.
     *
     * @see ModuleControl
     */
    public void stop()
    {
        singletonTimer.cancel();
    }
}
