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

package com.splicemachine.db.iapi.services.monitor;

import com.splicemachine.db.iapi.services.info.ProductVersionHolder;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.stream.InfoStreams;
import com.splicemachine.db.iapi.services.loader.InstanceGetter;

import java.util.Properties;
import java.util.Locale;

/**
The monitor provides a central registry for all modules in the system,
and manages loading, starting, and finding them.
*/

public interface ModuleFactory
{

    /**
     * Find the module in the system with the given module protocol,
	 * protocolVersion and identifier.
	    
     * @return The module instance if found, or null.
     */
	Object findModule(Object service, String protocol, String identifier);

	/**
		Return the name of the service that the passed in module lives in.
	*/
	String getServiceName(Object serviceModule);

	/**
		Return the locale of the service that the passed in module lives in.
		Will return null if no-locale has been defined.
	*/
	Locale getLocale(Object serviceModule);

	/**
		Translate a string of the form ll[_CC[_variant]] to a Locale.
		This is in the Monitor because we want this translation to be
		in only one place in the code.
	 */
	Locale getLocaleFromString(String localeDescription)
					throws StandardException;


	/**
		Set the locale for the service *outside* of boot time.

		@param userDefinedLocale	A String in the form xx_YY, where xx is the
									language code and YY is the country code.

		@return		The new Locale for the service

		@exception StandardException	Thrown on error
	 */
	Locale setLocale(Object serviceModule, String userDefinedLocale)
						throws StandardException;

	/**
		Set the locale for the service at boot time. The passed-in
		properties must be the one passed to the boot method.

		@exception StandardException	Derby error.
	 */
	Locale setLocale(Properties serviceProperties,
					 String userDefinedLocale)
						throws StandardException;

	/**
		Return the PersistentService object for a service.
		Will return null if the service does not exist.
	*/
	PersistentService getServiceType(Object serviceModule);

    /**
     * Return the PersistentService for a subsubprotocol.
     *
     * @return the PersistentService or null if it does not exist
     *
     * @exception StandardException
     */
	PersistentService getServiceProvider(String subSubProtocol) throws StandardException;
    
    /**
     * Return the application set of properties which correspond
     * to the set of properties in the file db.properties.
     */
	Properties getApplicationProperties();

	/**
		Shut down the complete system that was started by this Monitor. Will
		cause the stop() method to be called on each loaded module.
	*/
	void shutdown();

	/**
		Shut down a service that was started by this Monitor. Will
		cause the stop() method to be called on each loaded module.
		Requires that a context stack exist.
	*/
	void shutdown(Object service);


	/**
		Obtain a class that supports the given identifier.

		@param identifier	identifer to associate with class

		@return a reference InstanceGetter

		@exception StandardException See Monitor.classFromIdentifier
	*/
	InstanceGetter classFromIdentifier(int identifier)
		throws StandardException;

	/**
		Obtain an new instance of a class that supports the given identifier.

		@param identifier	identifer to associate with class

		@return a reference to a newly created object

		@exception StandardException See Monitor.newInstanceFromIdentifier
	
	*/
	Object newInstanceFromIdentifier(int identifier)
		throws StandardException;

	/**
		Return the environment object that this system was booted in.
		This is a free form object that is set by the method the
		system is booted. For example when running in a Marimba system
		it is set to the maribma application context. In most environments
		it will be set to a java.io.File object representing the system home directory.
		Code that call this method usualy have predefined knowledge of the type of the returned object, e.g.
		Marimba store code knows that this will be set to a marimba application
		context.
	*/
	Object getEnvironment();


	/**
		Return an array of the service identifiers that are running and
		implement the passed in protocol (java interface class name).
		This list is a snapshot of the current running systesm, once
		the call returns the service may have been shutdown or
		new ones added.

		@return The list of service names, if no services exist that
		implement the protocol an array with zero elements is returned.
	*/
	String[] getServiceList(String protocol);

	/**
		Start a persistent service.
		<BR>
		<B>Do not call directly - use Monitor.startPersistentService()</B>
		
		<P> The poperty set passed in is for boot options for the modules
		required to start the service. It does not support defining different
		or new modules implementations.
		
		@param serviceName Name of the service to be started
		@param properties Property set made available to all modules booted
		for this service, through their ModuleControl.boot method.

		@return true if the service type is handled by the monitor, false if it isn't

		@exception StandardException An attempt to start the service failed.

		@see Monitor#startPersistentService
	*/
	boolean startPersistentService(String serviceName, Properties properties)
		throws StandardException;

	/**
		Create a persistent service.
		<BR>
		<B>Do not call directly - use Monitor.startPersistentService()</B>

		@exception StandardException An attempt to create the service failed.

		@see Monitor#createPersistentService
	*/
	Object createPersistentService(String factoryInterface, String serviceName, Properties properties)
		throws StandardException;
    void removePersistentService(String name)
        throws StandardException;
   
	/**
		Start a non-persistent service.
		
		<BR>
		<B>Do not call directly - use Monitor.startNonPersistentService()</B>

		@exception StandardException An attempt to start the service failed.

		@see Monitor#startNonPersistentService
	*/
	Object startNonPersistentService(String factoryInterface, String serviceName, Properties properties)
		throws StandardException;


	/**
		Canonicalize a service name, mapping different user-specifications of a database name
        onto a single, standard name.
	*/
	String  getCanonicalServiceName(String userSpecifiedName)
        throws StandardException;
    
	/**
		Find a service.

		<BR>
		<B>Do not call directly - use Monitor.findService()</B>

		@return a refrence to a module represeting the service or null if the service does not exist.

		@see Monitor#findService
	*/
	Object findService(String protocol, String identifier);


	/**
		Start a module.
		
		<BR>
		<B>Do not call directly - use Monitor.startSystemModule() or Monitor.bootServiceModule()</B>

		@exception StandardException An attempt to start the module failed.

		@see Monitor#startSystemModule
		@see Monitor#bootServiceModule
	*/
	Object startModule(boolean create, Object service, String protocol,
					   String identifier, Properties properties)
									 throws StandardException;


	/**	
		Get the defined default system streams object.
	*/
	InfoStreams getSystemStreams();


	/**
		Start all services identified by db.service.*
		in the property set. If bootAll is true the services
		that are persistent will be booted.
	*/
	void startServices(Properties properties, boolean bootAll);

	/**
		Return a property from the JVM's system set.
		In a Java2 environment this will be executed as a privileged block
		if and only if the property starts with 'db.'.
		If a SecurityException occurs, null is returned.
	*/
	String getJVMProperty(String key);

	/**
		Get a newly created background thread.
		The thread is set to be a daemon but is not started.
	*/
	Thread getDaemonThread(Runnable task, String name, boolean setMinPriority);

	/**
		Set the priority of the current thread.
		If the current thread was not returned by getDaemonThread() then no action is taken.
	*/
	void setThreadPriority(int priority);

	ProductVersionHolder getEngineVersion();

	/**
	 * Get the UUID factory for the system.  The UUID factory provides
	 * methods to create and recreate database unique identifiers.
	 */
	com.splicemachine.db.iapi.services.uuid.UUIDFactory getUUIDFactory();
        
	/**
	 * Get the Timer factory for the system. The Timer factory provides
     * access to Timer objects for various purposes.
     *
     * @return the system's Timer factory.
	 */
	com.splicemachine.db.iapi.services.timer.TimerFactory getTimerFactory();
}
