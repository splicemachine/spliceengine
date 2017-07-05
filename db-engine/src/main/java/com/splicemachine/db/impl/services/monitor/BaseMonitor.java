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

import com.splicemachine.db.iapi.services.monitor.Monitor;
import com.splicemachine.db.iapi.services.monitor.ModuleFactory;
import com.splicemachine.db.iapi.services.monitor.ModuleControl;
import com.splicemachine.db.iapi.services.monitor.ModuleSupportable;

import com.splicemachine.db.iapi.services.monitor.PersistentService;

import com.splicemachine.db.iapi.services.io.FormatIdUtil;
import com.splicemachine.db.iapi.services.io.RegisteredFormatIds;
import com.splicemachine.db.iapi.services.io.StoredFormatIds;

import com.splicemachine.db.iapi.services.context.ContextManager;
import com.splicemachine.db.iapi.services.context.Context;
import com.splicemachine.db.iapi.services.context.ContextService;

import com.splicemachine.db.iapi.services.stream.InfoStreams;
import com.splicemachine.db.iapi.services.stream.PrintWriterGetHeader;

import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.error.ErrorStringBuilder;
import com.splicemachine.db.iapi.error.ShutdownException;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.uuid.UUIDFactory;
import com.splicemachine.db.iapi.services.timer.TimerFactory;
import com.splicemachine.db.iapi.reference.MessageId;
import com.splicemachine.db.iapi.reference.Module;
import com.splicemachine.db.iapi.reference.Property;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.reference.Attribute;
import com.splicemachine.db.iapi.services.property.PropertyUtil;

import com.splicemachine.db.iapi.services.io.AccessibleByteArrayOutputStream;
import com.splicemachine.db.iapi.services.loader.ClassInfo;
import com.splicemachine.db.iapi.services.loader.InstanceGetter;
import com.splicemachine.db.iapi.services.io.FormatableInstanceGetter;
import com.splicemachine.db.iapi.error.ExceptionSeverity;

import  com.splicemachine.db.io.StorageFactory;

import com.splicemachine.db.iapi.services.info.JVMInfo;
import com.splicemachine.db.iapi.services.i18n.BundleFinder;
import com.splicemachine.db.iapi.services.i18n.MessageService;

import java.io.IOException;
import java.io.InputStream;
import java.io.BufferedInputStream;
import java.io.PrintWriter;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.ByteArrayInputStream;
import java.io.PrintStream;

import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Properties;
import java.util.Enumeration;
import java.util.StringTokenizer;
import java.util.Vector;
import java.util.Locale;
import java.util.ResourceBundle;
import java.util.NoSuchElementException;

import java.lang.reflect.InvocationTargetException;

import java.net.URL;

/**
	Implementation of the monitor that uses the class loader
	that the its was loaded in for all class loading.

*/

abstract class BaseMonitor
	implements ModuleFactory, BundleFinder {

	/* Fields */

	/**
		Hash table of objects that implement PersistentService keyed by their getType() method.
	*/
	private HashMap serviceProviders = new HashMap();
	private static final String LINE = 
        "----------------------------------------------------------------";

	// Vector of class objects of implementations, found in the System, application
	// and default (modules.properties) properties

	Vector[]     implementationSets;

	private Vector	  services;					// Vector of TopServices

	Properties bootProperties;		// specifc properties provided by the boot method, override everything else
	Properties applicationProperties;

	boolean inShutdown;

	// Here are the list of modules that we always boot
	private InfoStreams systemStreams;
	private ContextService contextService;
	private UUIDFactory uuidFactory;
    private TimerFactory timerFactory;

	boolean reportOn;
	private PrintStream logging;

	ThreadGroup daemonGroup;

	// class registry
/* one byte  format identifiers never used
	private InstanceGetter[]	rc1;
*/
	private InstanceGetter[]	rc2;
//	private InstanceGetter[]	rc4;

	/* Constructor  */
	BaseMonitor() {
		super();

		services = new Vector(0, 1);
		services.add(new TopService(this));	// first element is always the free-floating service
	}

	/* Methods of ModuleFactory includes BootStrap and Runnable */

	public InfoStreams getSystemStreams() {
		return systemStreams;
	}

	public void shutdown() {

		// allow only one caller to shut the monitor down
		synchronized (this) {
			if (inShutdown)
				return;
			inShutdown = true;
		}
 
		Monitor.getStream().println(LINE);
		//Make a note of Engine shutdown in the log file
		Monitor.getStream().println(
                MessageService.getTextMessage(
                    MessageId.CONN_SHUT_DOWN_ENGINE,
                    new Date().toString()));

		if (SanityManager.DEBUG && reportOn) {
			report("Shutdown request");
		}

		// Shutdown all threads by iterrupting them
		contextService.notifyAllActiveThreads((Context) null);

		for (;;) {

			TopService ts;
			int position;
			synchronized (this) {
				position = services.size()  - 1;
				if (position == 0)
					break;

				ts = (TopService) services.get(position);
			}

			// push a new context manager
			ContextManager cm = contextService.newContextManager();
			try {
				// pop the default shutdown context, we are shutting down
				cm.popContext();

				contextService.setCurrentContextManager(cm);


				shutdown(ts.getService());

			} finally {
				contextService.resetCurrentContextManager(cm);
			}

		}
		
		Monitor.getStream().println(LINE);
		((TopService) services.get(0)).shutdown();

		ContextService.stop();
		Monitor.clearMonitor();
	}

	/**
		Shut down a service that was started by this Monitor. Will
		cause the stop() method to be called on each loaded module.
	*/
	public void shutdown(Object serviceModule) {
		if (serviceModule == null)
			return;

		TopService ts = findTopService(serviceModule);
		if (ts == null)
			return;

		// shutdown() returns false if the service is already being shutdown
		boolean removeService = true;
		try {
			removeService = ts.shutdown();
		} finally {
			synchronized (this) {
				if (removeService) {
					boolean found = services.remove(ts);
					if (SanityManager.DEBUG) {
						SanityManager.ASSERT(found, "service was not found " + serviceModule);
					}
				}
			}
		}
	}

	protected final void runWithState(Properties properties, PrintStream log) {

		bootProperties = properties;
		logging = log;

		// false indicates the full monitor is required, not the lite.
		if (!initialize(false))
			return;

		// if monitor is already set then the system is already
		// booted or in the process of booting or shutting down.
		if (!Monitor.setMonitor(this))
			return;

		MessageService.setFinder(this);

		if (SanityManager.DEBUG) {
			reportOn = Boolean.valueOf(PropertyUtil.getSystemProperty("derby.monitor.verbose"));
		}

		// Set up the application properties
		applicationProperties = readApplicationProperties();

		// The security manager may not let us get the System properties
		// object itself, although it may let us look at the properties in it.
		Properties systemProperties = null;

		if (SanityManager.DEBUG) {
			// In a production system having this call would
			// mean would we have to document it for security
			// permission reasons. Since we don't require it and
			// its a big security hole to allow external code to
			// overwrite our own implementations we just support
			// it for debugging. This means VM executions such as
			// java -Dderby.module.javaCompiler=com.ibm.db2j.impl.BasicServices.JavaCompiler.JavaLang.JLJava ...
			// would only work with a sane codeline.
			try {
				systemProperties = System.getProperties();
			} catch (SecurityException ignored) {
			}
		}

		Vector bootImplementations = getImplementations(bootProperties, false);

		Vector systemImplementations = null;
		Vector applicationImplementations = null;

		// TEMP - making this sanity only breaks the unit test code
		// I will fix soon, djd.
		// Don't allow external code to override our implementations.
		systemImplementations = getImplementations(systemProperties, false);
		applicationImplementations = getImplementations(applicationProperties, false);

		Vector defaultImplementations = getDefaultImplementations();

		int implementationCount = 0;
		if (bootImplementations != null)
			implementationCount++;

		// TEMP - making this sanity only breaks the unit test code
		// Don't allow external code to override our implementations.
		if (systemImplementations != null)
            implementationCount++;
		if (applicationImplementations != null)
            implementationCount++;

		if (defaultImplementations != null)
			implementationCount++;
		implementationSets = new Vector[implementationCount];

		implementationCount = 0;
		if (bootImplementations != null)
			implementationSets[implementationCount++] = bootImplementations;

		// Don't allow external code to override our implementations.
		if (systemImplementations != null)
            implementationSets[implementationCount++] = systemImplementations;
		if (applicationImplementations != null)
            implementationSets[implementationCount++] = applicationImplementations;

		if (defaultImplementations != null)
			implementationSets[implementationCount++] = defaultImplementations;


		if (SanityManager.DEBUG) {
			// Look for the db.debug.* properties.
			if (applicationProperties != null) {
				addDebugFlags(applicationProperties.getProperty(Monitor.DEBUG_FALSE), false);
				addDebugFlags(applicationProperties.getProperty(Monitor.DEBUG_TRUE), true);
			}

			addDebugFlags(PropertyUtil.getSystemProperty(Monitor.DEBUG_FALSE), false);
			addDebugFlags(PropertyUtil.getSystemProperty(Monitor.DEBUG_TRUE), true);
		}

		try {
			systemStreams = (InfoStreams) Monitor.startSystemModule("com.splicemachine.db.iapi.services.stream.InfoStreams");

			if (SanityManager.DEBUG) {
				SanityManager.SET_DEBUG_STREAM(systemStreams.stream().getPrintWriter());
			}

			contextService = ContextService.getService();

			uuidFactory = (UUIDFactory) Monitor.startSystemModule("com.splicemachine.db.iapi.services.uuid.UUIDFactory");

            timerFactory = (TimerFactory)Monitor.startSystemModule("com.splicemachine.db.iapi.services.timer.TimerFactory");
            
            Monitor.startSystemModule(Module.JMX);

		} catch (StandardException se) {

			// if we can't create an error log or a context then there's no point going on
			reportException(se);
			// dump any messages we have been saving ...
			dumpTempWriter(true);

			return;
		}

		// switch cover to the real error stream and
		// dump any messages we have been saving ...
		dumpTempWriter(false);

		if (SanityManager.DEBUG && reportOn) {
			dumpProperties("-- Boot Properties --", bootProperties);
			dumpProperties("-- System Properties --", systemProperties);
			dumpProperties("-- Application Properties --", applicationProperties);
		}
        
        

		// bootup all the service providers
		determineSupportedServiceProviders();

		// See if automatic booting of persistent services is required
		boolean bootAll = Boolean.valueOf(PropertyUtil.getSystemProperty(Property.BOOT_ALL));


		startServices(bootProperties, bootAll);
		startServices(systemProperties, bootAll);
		startServices(applicationProperties, bootAll);

		if (bootAll) // only if automatic booting is required
			bootPersistentServices( );
	}

    public  String  getCanonicalServiceName( String userSpecifiedName )
        throws StandardException
    {
        if ( userSpecifiedName == null ) { return null; }
        
        PersistentService   correspondingService = findProviderForCreate(  userSpecifiedName );

        if ( correspondingService == null ) { return null; }
        else { return correspondingService.getCanonicalServiceName( userSpecifiedName ); }
    }

	public Object findService(String factoryInterface, String serviceName) {

		if (serviceName == null)
			return null;

		ProtocolKey key;

		try {
			key = ProtocolKey.create(factoryInterface, serviceName);
		} catch (StandardException se) {
			return null;
		}

		TopService myts = null;
		synchronized (this) {
			for (int i = 1; i < services.size(); i++) {
				TopService ts = (TopService) services.get(i);
				if (ts.isPotentialService(key)) {
					myts = ts;
					break;
				}
			}
		}

		// the isActiveService() call may sleep
		// so don't hold the 'this' synchronization
		if (myts != null) {
			if (myts.isActiveService(key))
				return myts.getService();
		}

		return null;
	}

	public Locale getLocale(Object serviceModule) {

		TopService ts = findTopService(serviceModule);

		if (ts == null)
			return null;

		return ts.serviceLocale;

	}

	public Locale getLocaleFromString(String localeDescription)
											throws StandardException {
		return staticGetLocaleFromString(localeDescription);
	}

	/**
		Return the name of the service that the passed in module lives in.
	*/
	public String getServiceName(Object serviceModule) {

		TopService ts = findTopService(serviceModule);

		if (ts == null)
			return null;

		return ts.getServiceType().getUserServiceName(ts.getKey().getIdentifier());
	}

	/**
		Set the locale for the service *outside* of boot time.

		@exception StandardException Standard Derby error.
	*/
	public Locale setLocale(Object serviceModule, String userDefinedLocale)
		throws StandardException {

		TopService ts = findTopService(serviceModule);

		if (ts == null)
			return null;

		PersistentService provider = ts.getServiceType();
		if (provider == null)
			return null;

		String serviceName = ts.getKey().getIdentifier();

		Properties properties = provider.getServiceProperties(serviceName, (Properties) null);

		properties = new UpdateServiceProperties(provider, serviceName, properties, true);

		return setLocale(properties, userDefinedLocale);

	}

	/**
		Set the locale for the service at boot time. The passed in
		properties must be the one passed to the boot method.

		@exception StandardException Standard Derby error.
	*/
	public Locale setLocale(Properties serviceProperties, String userDefinedLocale)
		throws StandardException {

		Locale locale = staticGetLocaleFromString(userDefinedLocale);

		// this will write the property through to the service.properties file.
		serviceProperties.put(Property.SERVICE_LOCALE, locale.toString());

		return locale;
	}

	/**
		Return the PersistentService object for a service.
		Will return null if the service does not exist.
	*/
	public PersistentService getServiceType(Object serviceModule) {
		TopService ts = findTopService(serviceModule);

		if (ts == null)
			return null;

		return ts.getServiceType();
	}


	/**
		Start a module.

		@exception StandardException se An attempt to start the module failed.

		@see ModuleFactory#startModule
	*/
	public Object startModule(boolean create, Object serviceModule, String factoryInterface,
		String identifier, Properties properties) throws StandardException {


		ProtocolKey key = ProtocolKey.create(factoryInterface, identifier);

		TopService ts = findTopService(serviceModule);

		Object instance = ts.bootModule(create, serviceModule, key, properties);

		if (instance == null)
			throw Monitor.missingImplementation(factoryInterface);

		return instance;
	}

	private synchronized TopService findTopService(Object serviceModule) {

		if (serviceModule == null)
			return (TopService) services.get(0);

		for (int i = 1; i < services.size(); i++) {
			TopService ts = (TopService) services.get(i);
			if (ts.inService(serviceModule))
				return ts;
		}

		return null;
	}

	public Object findModule(Object serviceModule, String factoryInterface, String identifier)
	{

		ProtocolKey key;

		try {
			key = ProtocolKey.create(factoryInterface, identifier);
		} catch (StandardException se) {
			return null;
		}

		TopService ts = findTopService(serviceModule);
		if (ts == null)
			return null;

		return ts.findModule(key, true, null);
	}


	/**
		Obtain a class that supports the given identifier.

		@param fmtId identifer to associate with class

		@return a reference InstanceGetter

		@exception StandardException See Monitor.classFromIdentifier

		@see ModuleFactory#classFromIdentifier
	*/
	public InstanceGetter classFromIdentifier(int fmtId)
		throws StandardException {

		String className;
		int off;
		InstanceGetter[] iga;
		InstanceGetter ig;

		try {

			off = fmtId - StoredFormatIds.MIN_TWO_BYTE_FORMAT_ID;
			iga = rc2;
			if (iga == null) {
				iga = rc2 = new InstanceGetter[RegisteredFormatIds.TwoByte.length];
			}

			ig = iga[off];
			if (ig != null) {
				return ig;
			}
			className = RegisteredFormatIds.TwoByte[off];

		} catch (ArrayIndexOutOfBoundsException aioobe) {
			className = null;
			iga = null;
			off = 0;
		}

		if (className != null) {

			Throwable t;
			try {
				Class clazz = Class.forName(className);

				// See if it is a FormatableInstanceGetter
				if (FormatableInstanceGetter.class.isAssignableFrom(clazz)) {
					FormatableInstanceGetter tfig = (FormatableInstanceGetter) clazz.newInstance();
					tfig.setFormatId(fmtId);
					return iga[off] = tfig;
				}

				return iga[off] = new ClassInfo(clazz);

			} catch (ClassNotFoundException | LinkageError | InstantiationException | IllegalAccessException cnfe) {
				t = cnfe;
			}
			throw StandardException.newException(SQLState.REGISTERED_CLASS_LINAKGE_ERROR,
				t, FormatIdUtil.formatIdToString(fmtId), className);
		}

		throw StandardException.newException(SQLState.REGISTERED_CLASS_NONE, FormatIdUtil.formatIdToString(fmtId));
	}


	/**
		Obtain an new instance of a class that supports the given identifier.

		@return a reference to a newly created object or null if a matching class
			    cannot be found.
	*/
	public Object newInstanceFromIdentifier(int identifier)
		throws StandardException {

		InstanceGetter ci = classFromIdentifier(identifier);

		Throwable t;
		try {
			/*
				if (SanityManager.DEBUG) {
					if(SanityManager.DEBUG_ON(Monitor.NEW_INSTANCE_FROM_ID_TRACE_DEBUG_FLAG))
					{
						String traceResult = "null";

						if (result != null) traceResult = "not null";

						SanityManager.DEBUG(Monitor.NEW_INSTANCE_FROM_ID_TRACE_DEBUG_FLAG,
											"newInstanceFromIdentifier("+identifier+") "+
											" ClassName: "+
											result.getClass().getName() +
											" returned "+
											traceResult);
					}
				}
*/
			return ci.getNewInstance();
		}
		catch (InstantiationException | LinkageError | InvocationTargetException | IllegalAccessException ie) {
			t = ie;
		}
		throw StandardException.newException(SQLState.REGISTERED_CLASS_INSTANCE_ERROR,
			t, identifier, "XX" /*ci.getClassName()*/);
	}

	private Boolean exceptionTrace;

	/**
		load a module instance.

		Look through the implementations for a module that implements the
		required factory interface and can handle the properties given.

		The module's start or create method is not called.
	*/

	protected Object loadInstance(Class factoryInterface, Properties properties) {

		Object instance = null;

		Vector localImplementations = getImplementations(properties, false);
		if (localImplementations != null) {
			instance = loadInstance(localImplementations, factoryInterface, properties);
		}

		for (Vector implementationSet : implementationSets) {
			instance = loadInstance(implementationSet, factoryInterface, properties);
			if (instance != null)
				break;
		}

		return instance;
	}


	private Object loadInstance(Vector implementations, Class factoryInterface, Properties properties) {

		for (int index = 0; true; index++) {

			// find an implementation
			index = findImplementation(implementations, index, factoryInterface);
			if (index < 0)
				return null;

			// try to create an instance
			Object instance = newInstance((Class) implementations.get(index));

			if (BaseMonitor.canSupport(instance, properties))
				return instance;
		}
	}


	/**
		Find a class that implements the required index, return the index
		into the implementations vecotr of that class. Returns -1 if no class
		could be found.
	*/
	private static int findImplementation(Vector implementations, int startIndex, Class factoryInterface) {

		for (int i = startIndex; i < implementations.size(); i++) {

			//try {
				Class factoryClass = (Class) implementations.get(i);
				if (!factoryInterface.isAssignableFrom(factoryClass)) {
					continue;
				}

				return i;
			//}
			//catch (ClassNotFoundException e) {
			//	report("Class not found " + (String) implementations.elementAt(i));
			//	continue;
			//}
		}

		return -1;
	}

	/**
	*/
	private Object newInstance(String className) {

		try {

			Class factoryClass = Class.forName(className);
			return factoryClass.newInstance();
		}
		catch (ClassNotFoundException | IllegalAccessException | InstantiationException e) {
			report(className + " " + e.toString());
		} catch (LinkageError le) {
			report(className + " " + le.toString());
			reportException(le);
		}

		return null;
	}
	/**
	*/
	private Object newInstance(Class classObject) {

		try {
			return classObject.newInstance();
		}
		catch (InstantiationException | IllegalAccessException e) {
			report(classObject.getName() + " " + e.toString());
		} catch (LinkageError le) {
			report(classObject.getName() + " " + le.toString());
			reportException(le);
		}

		return null;
	}

	public Properties getApplicationProperties() {
		return applicationProperties;
	}

	/**
		Return an array of the service identifiers that are running and
		implement the passed in protocol (java interface class name).

		@return The list of service names, if no services exist that
		implement the protocol an array with zero elements is returned.

		@see ModuleFactory#getServiceList
	*/
	public String[] getServiceList(String protocol) {

		TopService ts;

		synchronized (this) {
			int count = 0;

			// count the number of services that implement the required protocol
			for (int i = 1; i < services.size(); i++) {
				ts = (TopService) services.get(i);
				if (ts.isActiveService()) {
					if (ts.getKey().getFactoryInterface().getName().equals(protocol))
						count++;
				}
			}

			// and then fill in the newly allocated string array
			String[] list = new String[count];
			if (count != 0) {
				int j = 0;
				for (int i = 1; i < services.size(); i++) {
					ts = (TopService) services.get(i);
					if (ts.isActiveService()) {
						if (ts.getKey().getFactoryInterface().getName().equals(protocol)) {
							list[j++] = ts.getServiceType().getUserServiceName(ts.getKey().getIdentifier());
							if (j == count)
								break;
						}
					}
				}
			}
			return list;
		}
	}

	/*
	** non-public methods.
	*/


	void dumpProperties(String title, Properties props) {
		if (SanityManager.DEBUG) {
			// this method is only called if reportOn is true, so no need to check it here
			report(title);
			if (props != null) {
				for (Enumeration e = props.propertyNames(); e.hasMoreElements(); ) {
					String key = (String) e.nextElement();
					// Get property as object in case of non-string properties
					report(key + "=" + props.getProperty(key));
				}
			}
			report("-- end --");
		}

	}


	/**
		Should only be called if reportOn is true
		apart from report/Exception().
	*/
	protected void report(String message)	{

		PrintWriter tpw = getTempWriter();

		if (tpw != null)
			tpw.println(message);

		if (systemStreams != null)
			systemStreams.stream().printlnWithHeader(message);
	}

	protected void reportException(Throwable t) {


		PrintWriterGetHeader pwgh = null;
		if (systemStreams != null)
			pwgh = systemStreams.stream().getHeader();

		ErrorStringBuilder esb = new ErrorStringBuilder(pwgh);

		esb.appendln(t.getMessage());
		esb.stackTrace(t);

		report(esb.get().toString());
	}

	private void addDebugFlags(String flags, boolean set) {
		if (SanityManager.DEBUG) {
			if (flags == null)
				return;

			StringTokenizer st = new StringTokenizer(flags, ",");
			for (; st.hasMoreTokens(); ) {
				String flag = st.nextToken();

				if (set)
					SanityManager.DEBUG_SET(flag);
				else
					SanityManager.DEBUG_CLEAR(flag);
			}
		}
	}

	/**
		Look for any services in the a properties set and the application
		property set and then start them.

		A service is defined by db.service.name=protocol
	*/
	private static final String SERVICE = "derby.service.";

	public void startServices(Properties properties, boolean bootAll) {

		if (properties == null)
			return;

		for (Enumeration e = properties.propertyNames(); e.hasMoreElements(); ) {

			String key = (String) e.nextElement();
			if (key.startsWith(SERVICE)) {
				String name = key.substring(SERVICE.length());

				String protocolOrType = properties.getProperty(key);

				try {
					if (protocolOrType.equals(Monitor.SERVICE_TYPE_DIRECTORY)) {
						if (bootAll)	// only if automatic booting is required
							findProviderAndStartService(name, properties, true);
					} else {
						bootService((PersistentService) null,
							protocolOrType, name, (Properties)null, false);
					}

				} catch (StandardException se) {
					// error already in error log, just continue booting
					// for persistent services, but non-persistent ones
					// will not have put the error in the log
					if (!protocolOrType.equals(Monitor.SERVICE_TYPE_DIRECTORY))
						reportException(se);
				}
			}
		}
	}

	/**
		Start a peristent service.

		@see ModuleFactory#startPersistentService
		@see Monitor#startPersistentService
	*/
	public final boolean startPersistentService(String name, Properties properties)
		throws StandardException {

		return findProviderAndStartService(name, properties, false);

	}

	/**
		Create a persistent service.

		@return The module from the service if it was created successfully, null if a service already existed.

		@exception StandardException An exception was thrown trying to create the service.

		@see Monitor#createPersistentService
	*/

	public Object createPersistentService(String factoryInterface, String name, Properties properties)
		throws StandardException {


		PersistentService provider = findProviderForCreate(name);
		if (provider == null) {
			throw StandardException.newException(SQLState.PROTOCOL_UNKNOWN, name);
		}

		return bootService(provider, factoryInterface, name, properties, true);
	}
    /**
     *  Removes a PersistentService.
     *  Could be used for drop database.
       @param name : Service name to be removed.
       
    */
    public void removePersistentService(String name)
         throws StandardException 
    {
        PersistentService provider=null;
		provider = findProviderForCreate(name);
        String serviceName = provider.getCanonicalServiceName(name);
        boolean removed = provider.removeServiceRoot(serviceName);
        if (!removed)
			throw StandardException.newException(SQLState.SERVICE_DIRECTORY_REMOVE_ERROR,serviceName);
    }
	/**
		Start a non-persistent service.

		@see Monitor#startNonPersistentService
		@see ModuleFactory#startNonPersistentService
	*/
	public Object startNonPersistentService(String factoryInterface, String serviceName, Properties properties)
		throws StandardException {

		return bootService((PersistentService) null, factoryInterface, serviceName, properties, false);
	}


	/**
		Create an implementation set.
		Look through the properties object for all properties that
		start with derby.module and add the value into the vector.

		If no implementations are listed in the properties object
		then null is returned.
	*/
	private Vector getImplementations(Properties moduleList, boolean actualModuleList) {

		if (moduleList == null)
			return null;

		Vector implementations = actualModuleList ? new Vector(moduleList.size()) : new Vector(0,1);

		// Get my current JDK environment
		int theJDKId = JVMInfo.JDK_ID;

		int[] envModuleCount = new int[theJDKId + 1];

nextModule:
		for (Enumeration e = moduleList.propertyNames(); e.hasMoreElements(); ) {

			String key = (String) e.nextElement();
            
            // module tagged name in the modules.properties file.
            // used as the tag  for dependent properties.
            String tag;
            
            // Dynamically loaded code is defined by a property of
            // the form:
            // derby.module.<modulename>=<class name>
            // or
            // db.subSubProtocol.<modulename>=<classname>
            
			if (key.startsWith(Property.MODULE_PREFIX)) {
				tag = key.substring(Property.MODULE_PREFIX.length());
            } else if (key.startsWith(Property.SUB_SUB_PROTOCOL_PREFIX)) {
                tag = key.substring(Property.MODULE_PREFIX.length());
            } else {
                continue;
            }
            

			// Check to see if it has any environment requirements

			// db.env.jdk.<modulename> - Any JDK requirements.
			String envKey = Property.MODULE_ENV_JDK_PREFIX + tag;
			String envJDK = moduleList.getProperty(envKey);
			int envJDKId = 0;
			
			if (envJDK != null) {
				envJDKId = Integer.parseInt(envJDK.trim());
				if (envJDKId > theJDKId) {
					continue;
				}
			}

			// db.env.classes.<tag> - Any class requirements
			envKey = Property.MODULE_ENV_CLASSES_PREFIX + tag;
			String envClasses = moduleList.getProperty(envKey);
			if (envClasses != null) {

				StringTokenizer st = new StringTokenizer(envClasses, ",");
				for (; st.hasMoreTokens(); ) {
					try {
						Class.forName(st.nextToken().trim());
					} catch (ClassNotFoundException cnfe) {
						continue nextModule;
					} catch (LinkageError le) {
						continue nextModule;
					}
				}
			}



			// Try to load the class
			// if we can't load the class or create an instance then
			// we don't use this calls as a valid module implementation
			String className = moduleList.getProperty(key);

			if (SanityManager.DEBUG && reportOn) {
				report("Accessing module " + className + " to run initializers at boot time");
			}

			try {
				Class possibleModule = Class.forName(className);

				// Look for the monitors special modules, PersistentService ones.
				if (getPersistentServiceImplementation(possibleModule))
                    continue;
                
                
                if( StorageFactory.class.isAssignableFrom(possibleModule)) {
                    storageFactories.put(tag, className);
                    continue;
                }


				// If this is a specific JDK version (environment) module
				// then it must be ordered in the implementation list by envJDKId.
				// Those with a higher number are at the front, e.g.
				//
				//	JDK 1.4 modules (envJDKId == 4)
				//  JDK 1.2/1.3 modules (envJDKId == 2)
				//  JDK 1.1 modules (envJDKId == 1)
				//  generic modules (envJDKId == 0 (not set in modules.properties)
				//
				//  Note modules with envJDKId > theJDKId do not get here

				if (envJDKId != 0) {

					// total how many modules with a higher envJDKId are ahead of us
					int offset = 0;
					for (int eji = theJDKId; eji > envJDKId; eji--) {
						offset += envModuleCount[eji];
					}

					implementations.add(offset, possibleModule);
					envModuleCount[envJDKId]++;

				}
				else {
					// just add to the end of the vector
					implementations.add(possibleModule);
				}

				// Since ModuleControl and ModuleSupportable are not called directly
				// check that if the have the methods then the class implements the
				// interface.
				if (SanityManager.DEBUG) {
					// ModuleSupportable
					Class[] csParams = {Properties.class};
					try {
						possibleModule.getMethod("canSupport", csParams);
						if (!ModuleSupportable.class.isAssignableFrom(possibleModule)) {
							SanityManager.THROWASSERT("Module does not implement ModuleSupportable but has canSupport() - " + className);
						}
					} catch (NoSuchMethodException nsme){/* ok*/}

					// ModuleControl
					boolean eitherMethod = false;

					Class[] bootParams = {Boolean.TYPE, Properties.class};
					try {
						possibleModule.getMethod("boot", bootParams);
						eitherMethod = true;
					} catch (NoSuchMethodException nsme){/*ok*/}

					Class[] stopParams = {};
					try {
						possibleModule.getMethod("stop", stopParams);
						eitherMethod = true;
					} catch (NoSuchMethodException nsme){/*ok*/}

					if (eitherMethod) {
						if (!ModuleControl.class.isAssignableFrom(possibleModule)) {
							SanityManager.THROWASSERT("Module does not implement ModuleControl but has its methods - " + className);
						}
					}
				}

			}
			catch (ClassNotFoundException | LinkageError cnfe) {
				report("Class " + className + " " + cnfe.toString() + ", module ignored.");
			}
		}
        
		if (implementations.isEmpty())
			return null;
		implementations.trimToSize();

		return implementations;
	}

    private boolean getPersistentServiceImplementation( Class possibleModule)
    {
        if( ! PersistentService.class.isAssignableFrom(possibleModule))
            return false;

        PersistentService ps = (PersistentService) newInstance(possibleModule);
        if (ps == null) {
            report("Class " + possibleModule.getName() + " cannot create instance, module ignored.");
        } else {
            serviceProviders.put(ps.getType(), ps);
        }
        return true;
    } // end of getPersistentServiceImplementation
        
	private Vector getDefaultImplementations() {

		Properties moduleList = getDefaultModuleProperties();

		return getImplementations(moduleList, true);
	} // end of getDefaultImplementations
	
	/**
	 * Get the complete set of module properties by
	 * loading in contents of all the org/apache/db/modules.properties
	 * files. This must be executed in a privileged block otherwise
	 * when running in a security manager environment no properties will
	 * be returned.
	 */
	Properties getDefaultModuleProperties()
	{
		// SECURITY PERMISSION - IP1 for modules in this jar
		// or other jars shipped with the Derby release.
		Properties moduleList = new Properties();
        boolean firstList = true;

        ClassLoader cl = getClass().getClassLoader();
        try {
        	Enumeration e = cl == null ?
        		ClassLoader.getSystemResources("com/splicemachine/db/modules.properties") :
        		cl.getResources("com/splicemachine/db/modules.properties");
            while (e.hasMoreElements()) {
                URL modulesPropertiesURL = (URL) e.nextElement();
                InputStream is = null;
                try {
                    is = modulesPropertiesURL.openStream();
                    if( firstList) {
                        moduleList.load( is);
                        firstList = false;
                    }
                    else {
                        // Check for duplicates
                        Properties otherList = new Properties();
                        otherList.load( is);
                        for( Enumeration newKeys = otherList.keys(); newKeys.hasMoreElements() ;)
                        {
                            String key = (String) newKeys.nextElement();
                            if (moduleList.containsKey(key))
                                // RESOLVE how do we localize messages before we have finished initialization?
                                report( "Ignored duplicate property " + key + " in " + modulesPropertiesURL.toString());
                            else
                                moduleList.setProperty( key, otherList.getProperty( key));
                        }
                    }
                } catch (IOException ioe) {
                    if (SanityManager.DEBUG)
                        report("Can't load implementation list " + modulesPropertiesURL.toString() + ": " + ioe.toString());
                } finally {
                    try {
                        if( is != null)
                            is.close();
                    } catch (IOException ignored) {
                    }
                }
            }
        } catch (IOException ioe) {
            if (SanityManager.DEBUG)
                report("Can't load implementation list: " + ioe.toString());
        }
        if (SanityManager.DEBUG)
        {
			if (firstList)
				report("Default implementation list not found");
		}
 
        return moduleList;
	}

	/*
	** Class methods
	*/

	/**
		Return a property set that has the runtime properties removed.
	*/
	protected static Properties removeRuntimeProperties(Properties properties) {

		Properties subset = new Properties();

		for (Enumeration e = properties.keys(); e.hasMoreElements(); ) {

			String key = (String) e.nextElement();
			if (key.startsWith(Property.PROPERTY_RUNTIME_PREFIX))
				continue;

			subset.put(key, properties.get(key));
		}

		return subset;
	}


	/**	
		Get InputStream for application properties file Returns nul if it does not exist.
	*/
	abstract InputStream applicationPropertiesStream()
	  throws IOException;


	/**
	*/
	protected Properties readApplicationProperties() {

		InputStream is = null;

		try {
			// SECURITY PERMISSION - OP3
			is = applicationPropertiesStream();
			if (is == null)
				return null;

			Properties properties = new Properties();

			// Trim off excess whitespace from properties file, if any,
			// and then load the properties into 'properties'.
			com.splicemachine.db.iapi.util.PropertyUtil.loadWithTrimmedValues(
				new BufferedInputStream(is), properties);

			return properties;

		} catch (SecurityException se) {
			return null;
		} catch (IOException ioe) {
			report(ioe.toString() + " (" + Property.PROPERTIES_FILE + ")");
			reportException(ioe);
			return null;

		}finally {


			try {
				if (is != null) {
					is.close();
					is = null;
				}

			} catch (IOException ignored) {
			}
		}
	}


	/*
	** Methods related to service providers.
	**
	** A service provider implements PersistentService and
	** abstracts out:
	**
	**    Finding all serivces that should be started at boot time.
	**    Finding the service.properties file for a service
	**    Creating a service's root.
	**
	** A monitor can have any number of service providers installed,
	** any module that implements PersistentService is treated specially
	** and stored only in the serviceProviders hashtable, indexed by
	** its getType() method.
	**
	** Once all the implementations have loaded the service providers
	** are checked to see if they run in the current environment.
	*/

	/**
     * Determine which of the set of service providers (PersistentService objects)
     * are supported in the current environment. If a PersistentService
     * implementation does not implement ModuleControl then it is assumed
     * it does support the current environment. Otherwise the canSupport()
     * method makes the determination. Any providers that are not supported
     * are removed from the list.
	*/
	private void determineSupportedServiceProviders() {

		for (Iterator i = serviceProviders.values().iterator(); i.hasNext(); ) {

			Object provider = i.next();

			// see if this provider can live in this environment
			if (!BaseMonitor.canSupport(provider, (Properties) null)) {
				i.remove();
			}
		}
	}
	/**
		Boot all persistent services that can be located at run time.

		<BR>
		This method enumerates through all the service providers that
		are active and calls bootPersistentServices(PersistentService)
		to boot all the services that that provider knows about.
	*/
	private void bootPersistentServices() {
		Enumeration e = new ProviderEnumeration( applicationProperties);
		while (e.hasMoreElements()) {
			PersistentService provider = (PersistentService) e.nextElement();
			bootProviderServices(provider);
		}

	}

	/**
		Boot all persistent services that can be located by a single service provider

		<BR>
		This method enumerates through all the service providers that
		are active and calls bootPersistentServices(PersistentService)
		to boot all the services that that provider knows about.
	*/
	protected void bootProviderServices(PersistentService provider) {

		if (SanityManager.DEBUG && reportOn) {
			report("Booting persistent services for provider: " + provider.getType());
		}

		for (Enumeration e = provider.getBootTimeServices(); (e != null) && e.hasMoreElements(); ) {

			String serviceName = (String) e.nextElement();

			Properties serviceProperties;
			try {
				serviceProperties = provider.getServiceProperties(serviceName, null);
			} catch (StandardException mse) {
				report("Failed to load service properties, name: " + serviceName + ", type = " + provider.getType());
				reportException(mse);
				continue;
			}

			// see if this service does not want to be auto-booted.
			if (Boolean.valueOf(serviceProperties.getProperty(Property.NO_AUTO_BOOT)))
				continue;


			try {
				startProviderService(provider, serviceName, serviceProperties);
			} catch (StandardException mse) {
				report("Service failed to boot, name: " + serviceName + ", type = " + provider.getType());
				reportException(mse);
			}
		}
	}
	/**
		Find a provider and start  a service.
	*/
	private boolean findProviderAndStartService(String name, 
						  Properties properties, boolean bootTime)
		throws StandardException {

		PersistentService actualProvider = null;

		Properties serviceProperties = null;
		String serviceName = null;

		// see if the name already includes a service type
		int colon = name.indexOf(':');
		if (colon != -1) {
			actualProvider = findProviderFromName(name, colon);

			// if null is returned here then its a sub-sub protocol/provider
			// that we don't understand. Attempt to load it as an untyped name.
			// If we have a protool
			// that we do understand and we can't open the service we will
			// throw an exception
			if (actualProvider != null) {

				serviceName = actualProvider.getCanonicalServiceName(name);
				if (serviceName == null)
					return true;  // we understand the type, but the service does not exist

				serviceProperties =
					actualProvider.getServiceProperties(serviceName, properties);

				if (serviceProperties == null)
					return true; // we understand the type, but the service does not exist

				// see if this service does not want to be auto-booted.
				if (bootTime && Boolean.valueOf(serviceProperties.getProperty(Property.NO_AUTO_BOOT)))
					return true;

				startProviderService(actualProvider, serviceName, serviceProperties);
				return true; // we understand the type
			}
		}

		StandardException savedMse = null;

		for (Enumeration e = new ProviderEnumeration( properties); e.hasMoreElements(); ) {

			PersistentService provider = (PersistentService) e.nextElement();

			String sn = provider.getCanonicalServiceName(name);
			if (sn == null)
				continue;

			Properties p = null;
			try {
				p = provider.getServiceProperties(sn, properties);
				// service does not exist.
				if (p == null)
					continue;

			} catch (StandardException mse) {
				savedMse = mse;
			}


			// yes we can attempt to boot this service
			if (actualProvider == null) {
				actualProvider = provider;
				serviceName = sn;
				serviceProperties = p;
				continue;
			}

			// we have an ambigious service name
			throw StandardException.newException(SQLState.AMBIGIOUS_PROTOCOL, name);
		}

		// no such service, if this was a name with no type, ie just name instead of type:name.
		// the monitor claims to always understand these.
		if (actualProvider == null)
			return colon == -1;

		if (savedMse != null)
			throw savedMse;

		// see if this service does not want to be auto-booted.
		if (bootTime && Boolean.valueOf(serviceProperties.getProperty(Property.NO_AUTO_BOOT)))
			return true;

		startProviderService(actualProvider, serviceName, serviceProperties);
		return true;
	}

	protected PersistentService findProviderForCreate(String name) throws StandardException {
		// RESOLVE - hard code creating databases in directories for now.
		return (PersistentService) findProviderFromName(name, name.indexOf(':'));
	}

	/**
		Find the service provider from a name that includes a service type,
		ie. is of the form 'type:name'. If type is less than 3 characters
		then it is assumed to be of type directory, i.e. a windows driver letter.
	*/
	private PersistentService findProviderFromName(String name, int colon) throws StandardException
    {
		// empty type, treat as a unknown protocol
		if (colon == 0)
			return null;

		String serviceType;
		if (colon < 2) {
			// assume it's a windows path (a:/foo etc.) and set the type to be DIRECTORY
			serviceType = PersistentService.DIRECTORY;
		} else {
			serviceType = name.substring(0, colon);
		}
		return getServiceProvider(serviceType);
	}

    public PersistentService getServiceProvider(String subSubProtocol) throws StandardException
    {
        if( subSubProtocol == null)
            return null;
        if( serviceProviders != null)
        {
            PersistentService ps = (PersistentService) serviceProviders.get( subSubProtocol);
            if( ps != null)
                return ps;
        }
        return getPersistentService(subSubProtocol);
    } // end of getServiceProvider

 
    /**
     * Return a PersistentService implementation to handle the subSubProtocol.
     * @return Valid PersistentService or null if the protocol is not handled.
      */
    private PersistentService getPersistentService(String subSubProtocol)
        throws StandardException
    {
        String className = getStorageFactoryClassName(subSubProtocol);
        return getPersistentService( className, subSubProtocol);
    }

    private PersistentService getPersistentService( final String className, String subSubProtocol) throws StandardException
    {
        if( className == null)
            return null;
        Class storageFactoryClass = null;
        try
        {
            storageFactoryClass = Class.forName( className);
       }
        catch (Throwable e)
        {
            throw StandardException.newException( SQLState.INSTANTIATE_STORAGE_FACTORY_ERROR,
                                                  e,
                                                  subSubProtocol, className);
        }
        return new StorageFactoryService( subSubProtocol, storageFactoryClass);
    } // end of getPersistentService

    /**
     * Find the StorageFactory class name that handles the subSub protocol.
     * Looks in the system property set and the set defined during boot.
 
      * @return Valid class name, or null if no StorageFactory handles the protocol.
     */
    private String getStorageFactoryClassName(String subSubProtocol)
    {
        String propertyName = Property.SUB_SUB_PROTOCOL_PREFIX + subSubProtocol;
        String className = PropertyUtil.getSystemProperty( propertyName);
        if( className != null)
            return className;
        return (String) storageFactories.get( subSubProtocol);
    } // end of getStorageFactoryClassName

    private static final HashMap storageFactories = new HashMap();
    static {
		String dirStorageFactoryClass;
        if (!JVMInfo.J2ME)
            dirStorageFactoryClass = "com.splicemachine.db.impl.io.DirStorageFactory4";
        else
            dirStorageFactoryClass = "com.splicemachine.db.impl.io.DirStorageFactory";


        storageFactories.put( PersistentService.DIRECTORY, dirStorageFactoryClass);
        storageFactories.put( PersistentService.CLASSPATH,
                                "com.splicemachine.db.impl.io.CPStorageFactory");
        storageFactories.put( PersistentService.JAR,
                                "com.splicemachine.db.impl.io.JarStorageFactory");
        storageFactories.put( PersistentService.HTTP,
                                "com.splicemachine.db.impl.io.URLStorageFactory");
        storageFactories.put( PersistentService.HTTPS,
                                "com.splicemachine.db.impl.io.URLStorageFactory");
        storageFactories.put( PersistentService.INMEMORY,
                            "com.splicemachine.db.impl.io.VFMemoryStorageFactory");
    }

	/**
		Boot a service under the control of the provider
	*/
	protected void startProviderService(PersistentService provider, String serviceName, Properties serviceProperties)
		throws StandardException {

		String protocol = serviceProperties.getProperty(Property.SERVICE_PROTOCOL);

		if (protocol == null) {
			throw StandardException.newException(SQLState.PROPERTY_MISSING, Property.SERVICE_PROTOCOL);
		}

		bootService(provider, protocol, serviceName, serviceProperties, false);
	}

	/**
		Boot (start or create) a service (persistent or non-persistent).
	*/
	protected Object bootService(PersistentService provider,
		String factoryInterface, String serviceName, Properties properties,
		boolean create) throws StandardException {

		//reget the canonical service name in case if it was recreated
		//after we got service name.(like in case of restoring from backup).
		if(provider != null)
			serviceName = provider.getCanonicalServiceName(serviceName);
		ProtocolKey serviceKey = ProtocolKey.create(factoryInterface, serviceName);
		if (SanityManager.DEBUG && reportOn) {
			report("Booting service " + serviceKey + " create = " + create);
		}

		ContextManager previousCM = contextService.getCurrentContextManager();
		ContextManager cm = previousCM;
		Object instance;
		TopService ts = null;
		Context sb = null;


		try {


			synchronized (this) {

				if (inShutdown) {
					throw StandardException.newException(SQLState.CLOUDSCAPE_SYSTEM_SHUTDOWN);
				}

				for (int i = 1; i < services.size(); i++) {
					TopService ts2 = (TopService) services.get(i);
					if (ts2.isPotentialService(serviceKey)) {
						// if the service already exists then  just return null
						return null;
					}
				}


				Locale serviceLocale = null;
				if (create) {

					
					// always wrap the property set in an outer set.
					// this ensures that any random attributes from
					// a JDBC URL are not written into the service.properties
					// file (e.g. like user and password :-)
					properties = new Properties(properties);

					serviceLocale = setLocale(properties);

					properties.put(Property.SERVICE_PROTOCOL, factoryInterface);

					serviceName = provider.createServiceRoot(serviceName,
							Boolean.valueOf(properties.getProperty(Property.DELETE_ON_CREATE)));

					serviceKey = ProtocolKey.create(factoryInterface, serviceName);
				} else if (properties != null) {
					String serverLocaleDescription = properties.getProperty(Property.SERVICE_LOCALE);
					if ( serverLocaleDescription != null)
						serviceLocale = staticGetLocaleFromString(serverLocaleDescription);
				}

				ts = new TopService(this, serviceKey, provider, serviceLocale);
				services.add(ts);
			}

			if (SanityManager.DEBUG) {
				if (provider != null)
				{
					SanityManager.ASSERT(provider.getCanonicalServiceName(serviceName).equals(serviceName),
						"mismatched canonical names " + provider.getCanonicalServiceName(serviceName)
						+ " != " + serviceName);
					SanityManager.ASSERT(serviceName.equals(serviceKey.getIdentifier()),
						"mismatched names " + serviceName + " != " + serviceKey.getIdentifier());
				}
			}


			if (properties != null) {

				// these properties must not be stored in the persistent properties,
				// otherwise moving databases from one directory to another
				// will not work. Thus they all have a fixed prefix

				// the root of the data
				properties.put(PersistentService.ROOT, serviceName);

				// the type of the service
				properties.put(PersistentService.TYPE, provider.getType());
			}

			if (SanityManager.DEBUG && reportOn) {
				dumpProperties("Service Properties: " + serviceKey.toString(), properties);
			}

			// push a new context manager
			if (previousCM == null) {
				cm = contextService.newContextManager();

				contextService.setCurrentContextManager(cm);
			}
			sb = new ServiceBootContext(cm);

			UpdateServiceProperties usProperties;
			Properties serviceProperties;


			//while doing restore from backup, we don't want service properties to be
			//updated until all the files are copied from backup.
			boolean inRestore = (properties != null && properties.getProperty(Property.IN_RESTORE_FROM_BACKUP) != null);
			
			if ((provider != null) && (properties != null)) {
				// we need to track to see if the properties have
				// been updated or not. If the database is not created yet, we don't create the
				// services.properties file yet. We let the following if (create) statement do
				//that at the end of the database creation. After that, the changes in
				// services.properties file will be tracked by UpdateServiceProperties.
				usProperties = new UpdateServiceProperties(provider,
														   serviceName,
														   properties, 
														   !(create || inRestore));
				serviceProperties = usProperties;
			} else {
				usProperties = null;
				serviceProperties = properties;
			}

			instance = ts.bootModule(create, null, serviceKey, serviceProperties);

			if (create || inRestore) {
				// remove all the in-memory properties
				provider.saveServiceProperties(serviceName, usProperties.getStorageFactory(),
						BaseMonitor.removeRuntimeProperties(properties), false);
				usProperties.setServiceBooted();
			}
            
            if (cm != previousCM) {
                //Assume database is not active. DERBY-4856 thread dump
                cm.cleanupOnError(StandardException.closeException(), false);
            }
            
		} catch (Throwable t) {

			StandardException se;
			// ensure that the severity will shutdown the service
			if ((t instanceof StandardException) && (((StandardException) t).getSeverity() == ExceptionSeverity.DATABASE_SEVERITY))
				se = (StandardException) t;
			else
				se = Monitor.exceptionStartingModule(t);

			if (cm != previousCM) {
                //Assume database is not active. DERBY-4856 thread dump
                cm.cleanupOnError(se, false);
			}

			if (ts != null) {
				ts.shutdown();
				synchronized (this) {
					services.remove(ts);
				}

				// Service root will only have been created if
				// ts is non-null.
				boolean deleteOnError = (properties != null && properties.getProperty(Property.DELETE_ROOT_ON_ERROR) != null);
				if (create || deleteOnError)
					provider.removeServiceRoot(serviceName);
			}


			Throwable nested = se.getCause();

			// never hide ThreadDeath
			if (nested instanceof ThreadDeath)
				throw (ThreadDeath) nested;

			throw se;

		} finally {
			if ((previousCM == cm) && (sb != null))
				sb.popMe();

			if (previousCM == null)
				contextService.resetCurrentContextManager(cm);
		}

		// from this point onwards the service is open for business
		ts.setTopModule(instance);

		//
		// The following yield allows our background threads to
		// execute their run methods. This is needed due to
		// bug 4081540 on Solaris. When the bug is fixed we can
		// remove this yield.
		Thread.yield();

		return instance;
	}

	/*
	** Methods of com.ibm.db2j.system.System
	*/

	/**
	Return the UUID factory for this system.  Returns null
	if there isn't one.
	See com.ibm.db2j.system.System
	*/
	public UUIDFactory getUUIDFactory()	{

		return uuidFactory;
	}
        
    /**
     * Returns the Timer factory for this system.
     *
     * @return the system's Timer factory.
     */
    public TimerFactory getTimerFactory() {
        return timerFactory;
    }

	/*
	** Methods to deal with storing error messages until an InfoStreams is available.
	*/

	private PrintWriter tmpWriter;
	private AccessibleByteArrayOutputStream tmpArray;
	private boolean dumpedTempWriter;

	private PrintWriter getTempWriter() {
		if (tmpWriter == null && !dumpedTempWriter) {
			tmpArray = new AccessibleByteArrayOutputStream();
			tmpWriter = new PrintWriter(tmpArray);
		}
		return tmpWriter;
	}

	private void dumpTempWriter(boolean bothPlaces) {

		if (tmpWriter == null)
			return;

		tmpWriter.flush();

		BufferedReader lnr = new BufferedReader(
			new InputStreamReader(
				new ByteArrayInputStream(tmpArray.getInternalByteArray())));
		try {
			String s;
			while ((s = lnr.readLine()) != null) {
				if (systemStreams != null)
					systemStreams.stream().printlnWithHeader(s);

				if ((systemStreams == null) || bothPlaces)
					logging.println(s);
			}
		} catch (IOException ignored) {
		}

		if ((systemStreams == null) || bothPlaces)
			logging.flush();

		tmpWriter = null;
		tmpArray = null;
		dumpedTempWriter = true;
		logging = null;
	}

	/**
		If the module implements ModuleSupportable then call its
		canSupport() method to see if it can or should run in
		this setup. If it doesn't then it can always run.
	*/
	static boolean canSupport(Object instance, Properties properties) {
		if (instance instanceof ModuleSupportable) {
			// see if the instance can support the properties
			if (!((ModuleSupportable) instance).canSupport(properties))
				return false;
		}
		return true;
	}


	/**
		Boot a module. If the module implements ModuleControl
		then its boot() method is called. Otherwise all the
		boot code is assumed to take place in its constructor.
	*/
	static void boot(Object module, boolean create, Properties properties)
		throws StandardException {

		if (module instanceof ModuleControl)
			((ModuleControl) module).boot(create, properties);
	}

	/*
	** Locale handling
	*/
	private static Locale staticGetLocaleFromString(String localeDescription)
		throws StandardException {

		// Check String is of expected format
		// even though country should not be optional
		// some jvm's support this, so go with the flow.
		// xx[_YY[_variant]]

		int len = localeDescription.length();

		boolean isOk = (len == 2) || (len == 5) || (len > 6);

		// must have underscores at position 2
		if (isOk && (len != 2))
			isOk = localeDescription.charAt(2) == '_';

		// must have underscores at position 2
		if (isOk && (len > 5))
			isOk = localeDescription.charAt(5) == '_';

		if (!isOk)
			throw StandardException.newException(SQLState.INVALID_LOCALE_DESCRIPTION, localeDescription);

		String language = localeDescription.substring(0, 2);
		String country = len == 2 ? "" : localeDescription.substring(3, 5);

		if (len < 6) {
			return new Locale(language, country);
		}

		String variant = (len > 6) ? localeDescription.substring(6, len) : null;

		return new Locale(language, country, variant);
	}

	private static Locale setLocale(Properties properties)
		throws StandardException {

		String userDefinedLocale = properties.getProperty(Attribute.TERRITORY);
		Locale locale;
		if (userDefinedLocale == null)
			locale = Locale.getDefault();
		else {
			// validate the passed in string
			locale = staticGetLocaleFromString(userDefinedLocale);
		}

		properties.put(Property.SERVICE_LOCALE, locale.toString());
		return locale;
	}

	/*
	** BundleFinder
	*/

	//private Hashtable localeBundles;

	/**
		Get the locale from the ContextManager and then find the bundle
		based upon that locale.
	*/
	public ResourceBundle getBundle(String messageId) {
		ContextManager cm;
		try {
			cm = ContextService.getFactory().getCurrentContextManager();
		} catch (ShutdownException se) {
			cm = null;
		}

		if (cm != null) {
			return MessageService.getBundleForLocale(cm.getMessageLocale(), messageId);
		}
		return null;
	}

	public Thread getDaemonThread(Runnable task, String name, boolean setMinPriority) {
		Thread t =  new Thread(daemonGroup, task, "derby." + name);
		t.setDaemon(true);

		if (setMinPriority) {
			t.setPriority(Thread.MIN_PRIORITY);
		}
		return t;

	}

	public void setThreadPriority(int priority) {

		Thread t = Thread.currentThread();

		if (t.getThreadGroup() == daemonGroup) {
			t.setPriority(priority);
		}
	}

	/**
		Initialize the monitor wrt the current environemnt.
		Returns false if the monitor cannot be initialized, true otherwise.
	*/
	abstract boolean initialize(boolean lite);

    class ProviderEnumeration implements Enumeration
    {
        private Enumeration serviceProvidersKeys = (serviceProviders == null) ? null :
            Collections.enumeration(serviceProviders.keySet());
        private Properties startParams;
        private Enumeration paramEnumeration;
        private boolean enumeratedDirectoryProvider;
        private PersistentService storageFactoryPersistentService;

        ProviderEnumeration( Properties startParams)
        {
            this.startParams = startParams;
            if( startParams != null)
                paramEnumeration = startParams.keys();
        }

        public Object nextElement() throws NoSuchElementException
        {
            if( serviceProvidersKeys != null && serviceProvidersKeys.hasMoreElements())
                return serviceProviders.get( serviceProvidersKeys.nextElement());
            getNextStorageFactory();
            Object ret = storageFactoryPersistentService;
            storageFactoryPersistentService = null;
            return ret;
        }

        private void getNextStorageFactory()
        {
            if( storageFactoryPersistentService != null)
                return;
            if( paramEnumeration != null)
            {
                while( paramEnumeration.hasMoreElements())
                {
                    String prop = (String) paramEnumeration.nextElement();
                    if( prop.startsWith( Property.SUB_SUB_PROTOCOL_PREFIX))
                    {
                        try
                        {
                            String storageFactoryClassName = (String) startParams.get( prop);
                            if( storageFactoryClassName != null)
                            {
                                storageFactoryPersistentService =
                                  getPersistentService( (String) startParams.get( prop),
                                                        prop.substring( Property.SUB_SUB_PROTOCOL_PREFIX.length()));
                                if( storageFactoryPersistentService != null)
                                    return;
                            }
                        }
                        catch( StandardException ignored){}
					}
                }
            }
            if( ! enumeratedDirectoryProvider)
            {
                try
                {
                    storageFactoryPersistentService
                      = getPersistentService( getStorageFactoryClassName(PersistentService.DIRECTORY),
                                              PersistentService.DIRECTORY);
                }
                catch( StandardException se){ storageFactoryPersistentService = null; }
                enumeratedDirectoryProvider = true;
            }
        } // end of getNextStorageFactory

        public boolean hasMoreElements()
        {
            if( serviceProvidersKeys != null && serviceProvidersKeys.hasMoreElements())
                return true;
            getNextStorageFactory();
            return storageFactoryPersistentService != null;
        }
    } // end of class ProviderEnumeration
} // end of class BaseMonitor
