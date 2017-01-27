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

package com.splicemachine.db.impl.services.monitor;

import com.splicemachine.db.iapi.services.monitor.ModuleControl;
import com.splicemachine.db.iapi.services.monitor.Monitor;

import com.splicemachine.db.iapi.services.monitor.PersistentService;
import com.splicemachine.db.iapi.error.StandardException;

import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.util.InterruptStatus;
import com.splicemachine.db.iapi.reference.SQLState;

import java.util.Hashtable;
import java.util.Vector;
import java.util.Properties;
import java.util.Locale;

/**
	A description of an instance of a module.
*/


final class TopService {

	/*
	** Fields.
	*/

	/**
		The idenity of this service, note that it may not be active yet.
	*/
	ProtocolKey key;

	/**
		The top module instance
	*/
	ModuleInstance topModule;

	/**
		List of protocols.
	*/
	Hashtable		protocolTable;

	/**
	*/
	Vector		moduleInstances;

	/**
	*/
	BaseMonitor	monitor;

	boolean inShutdown;

	/**
		The type of service this was created by. If null then this is a non-persistent service.
	*/
	PersistentService serviceType;

	Locale serviceLocale;

	/*
	** Constructor
	*/


	TopService(BaseMonitor monitor) {
		super();
		this.monitor = monitor;
		protocolTable = new Hashtable();
		moduleInstances = new Vector(0, 5);
	}

	TopService(BaseMonitor monitor, ProtocolKey key, PersistentService serviceType, Locale serviceLocale)
	{
		this(monitor);

		this.key = key;
		this.serviceType = serviceType;
		this.serviceLocale = serviceLocale;
	}

	void setTopModule(Object instance) {
		synchronized (this) {
            ModuleInstance module = findModuleInstance(instance);
            if (module != null) {
                topModule = module;
                notifyAll();
            }

			// now add an additional entry into the hashtable
			// that maps the server name as seen by the user
			// onto the top module. This allows modules to find their
			// top most service moduel using the monitor.getServiceName() call,
			// e.g. Monitor.findModule(ref, inferface, Monitor.getServiceName(ref));
			if (getServiceType() != null) {
				ProtocolKey userKey = new ProtocolKey(key.getFactoryInterface(),
					monitor.getServiceName(instance));
				addToProtocol(userKey, topModule);
			}

		}
	}

	Object getService() {

		return topModule.getInstance();
	}

	boolean isPotentialService(ProtocolKey otherKey) {


		String otherCanonicalName;

		if (serviceType == null)
			otherCanonicalName = otherKey.getIdentifier();
		else {
			try
			{
				otherCanonicalName = serviceType.getCanonicalServiceName(otherKey.getIdentifier());
			} catch (StandardException se)
			{
				return false;
			}

			// if the service name cannot be converted into a canonical name then it is not a service.
			if (otherCanonicalName == null)
				return false;
		}

		if (topModule != null)
			return topModule.isTypeAndName(serviceType, key.getFactoryInterface(), otherCanonicalName);


		if (!otherKey.getFactoryInterface().isAssignableFrom(key.getFactoryInterface()))
			return false;

		return serviceType.isSameService(key.getIdentifier(), otherCanonicalName);
	}

	boolean isActiveService() {
		synchronized (this) {
			return (topModule != null);
		}
	}

	boolean isActiveService(ProtocolKey otherKey) {

		synchronized (this) {
			if (inShutdown)
				return false;

			if (!isPotentialService(otherKey))
				return false;

			if (topModule != null) {
				if (SanityManager.DEBUG) {
					SanityManager.ASSERT(topModule.isTypeAndName(serviceType,
						key.getFactoryInterface(), key.getIdentifier()));
				}

				return true;
			}

			// now wait for topModule to be set
			while (!inShutdown && (topModule == null)) {
				try {
					wait();
				} catch (InterruptedException ioe) {
                    InterruptStatus.setInterrupted();
				}
			}

			if (inShutdown)
				return false;

			return true;
		}
	}

	/**
		Find an module in the protocol table that supports the required protocol
		name combination and can handle the properties.

		Returns the instance of the module or null if one does not exist in
		the protocol table.
	*/
	synchronized Object findModule(ProtocolKey key, boolean findOnly, Properties properties) {

		ModuleInstance module = (ModuleInstance) protocolTable.get(key);

		if (module == null)
			return null;

		Object instance = module.getInstance();

		if (findOnly || BaseMonitor.canSupport(instance, properties))
			return instance;

		return null;
	}

    /**
     * Find a {@code ModuleInstance} object whose {@code getInstance()} method
     * returns the object specified by the {@code instance} parameter.
     *
     * @param instance the instance to look for
     * @return a {@code ModuleInstance} object, or {@code null} if no match
     * was found
     */
    private ModuleInstance findModuleInstance(Object instance) {
        // DERBY-4018: Need to hold the synchronization over the entire loop
        // to prevent concurrent modifications from causing an
        // ArrayIndexOutOfBoundsException.
        synchronized (moduleInstances) {
            for (int i = 0; i < moduleInstances.size(); i++) {
                ModuleInstance module = (ModuleInstance) moduleInstances.get(i);
                if (module.getInstance() == instance) {
                    return module;
                }
            }
        }
        return null;
    }

	/**
		Boot a module, performs three steps.

		<OL>
		<LI> Look for an existing module in the protocol table
		<LI> Look for a module in the implementation table that handles this protocol
		<LI> Create an instance that handles this protocol.
		</OL>
	*/
	Object bootModule(boolean create, Object service, ProtocolKey key, Properties properties) 
		throws StandardException {

		synchronized (this) {
			if (inShutdown)
				throw StandardException.newException(SQLState.SHUTDOWN_DATABASE, getKey().getIdentifier());
		}

		//  see if this system already has a module that will work.
		Object instance = findModule(key, false, properties);
		if (instance != null)
			return instance;
		
		if (monitor.reportOn) {
			monitor.report("Booting Module   " + key.toString() + " create = " + create);
		}

		// see if a running implementation will handle this protocol
		synchronized (this) {

            for (int i = 0;; i++) {
                final ModuleInstance module;

                // DERBY-4018: Synchronized block in order to close the window
                // between size() and elementAt() where the size may change
                // and result in an ArrayIndexOutOfBoundsException.
                synchronized (moduleInstances) {
                    if (i < moduleInstances.size()) {
                        module = (ModuleInstance) moduleInstances.get(i);
                    } else {
                        // No more instances to look at, break out of the loop.
                        break;
                    }
                }

                // DERBY-2074: The module has not been properly booted, so we
                // cannot yet determine whether or not this is a module we can
                // use. Assume that we cannot use it and continue looking. We
                // may end up booting the module twice if the assumption
                // doesn't hold, but we'll detect and resolve that later when
                // we call addToProtocol().
                if (!module.isBooted()) {
                    continue;
                }

				if (!module.isTypeAndName((PersistentService) null, key.getFactoryInterface(), key.getIdentifier()))
					continue;

				instance = module.getInstance();
				if (!BaseMonitor.canSupport(instance, properties))
					continue;

				// add it to the protocol table, if this returns false then we can't use
				// this module, continue looking.
				if (!addToProtocol(key, module))
					continue;

				if (monitor.reportOn) {
					monitor.report("Started Module   " + key.toString());
					monitor.report("  Implementation " + instance.getClass().getName());
				}

				return instance;
			}
		}

		// try and load an instance that will support this protocol
		instance = monitor.loadInstance(key.getFactoryInterface(), properties);
		if (instance == null)
		{
			throw Monitor.missingImplementation(key.getFactoryInterface().getName());
		}
		ModuleInstance module = new ModuleInstance(instance, key.getIdentifier(), service,
				topModule == null ? (Object) null : topModule.getInstance());

		moduleInstances.add(module);

		try {
			BaseMonitor.boot(instance, create, properties);
		} catch (StandardException se) {
			moduleInstances.remove(module);
			throw se;
		}

        module.setBooted();

		synchronized (this) {


			// add it to the protocol table, if this returns false then we can't use
			// this module, shut it down.
			if (addToProtocol(key, module)) {

				if (monitor.reportOn) {
					monitor.report("Started Module   " + key.toString());
					monitor.report("  Implementation " + module.getInstance().getClass().getName());
				}

				return module.getInstance();
			}

			
		}
	
		TopService.stop(instance);
		moduleInstances.remove(module);

		// if we reached here it's because someone else beat us adding the module, so use theirs.
		return findModule(key, true, properties);
	}

	/**	
		If the service is already beign shutdown we return false.
	*/
	boolean shutdown() {

		synchronized (this) {
			if (inShutdown)
				return false;

			inShutdown = true;
			notifyAll();
		}

		for (;;) {

			ModuleInstance module;

			synchronized (this) {

				if (moduleInstances.isEmpty())
					return true;

				module = (ModuleInstance) moduleInstances.get(0);

			}
			
			Object instance = module.getInstance();
			TopService.stop(instance);
			
			synchronized (this) {
				moduleInstances.remove(0);
			}
		}
	}

	/**
		Add a running module into the protocol hash table. Return true
		if the module was added successfully, false if it couldn't
		be added. In the latter case the module should be shutdown
		if its reference count is 0.
	*/

	private boolean addToProtocol(ProtocolKey key, ModuleInstance module) {

		String identifier = module.getIdentifier();

		synchronized (this) {

			Object value = protocolTable.get(key);
			if (value == null) {

				protocolTable.put(key, module);
				return true;
			}

			if (value == module)
				return true;

			return false;
		}
	}

	boolean inService(Object instance) {
        return findModuleInstance(instance) != null;
	}

	public ProtocolKey getKey() {
		return key;
	}

	PersistentService getServiceType() {
		return serviceType;
	}

	private static void stop(Object instance) {
		if (instance instanceof ModuleControl) {
			((ModuleControl) instance).stop();
		}
	}
}
