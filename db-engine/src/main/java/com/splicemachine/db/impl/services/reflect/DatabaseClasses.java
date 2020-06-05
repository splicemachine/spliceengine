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

package com.splicemachine.db.impl.services.reflect;

import com.splicemachine.db.iapi.reference.*;
import com.splicemachine.db.iapi.services.sanity.SanityManager;

import com.splicemachine.db.iapi.services.loader.ClassFactory;
import com.splicemachine.db.iapi.services.loader.GeneratedClass;
import com.splicemachine.db.iapi.services.loader.ClassInspector;

import com.splicemachine.db.iapi.services.monitor.ModuleControl;
import com.splicemachine.db.iapi.services.monitor.Monitor;

import com.splicemachine.db.iapi.error.StandardException;

import com.splicemachine.db.iapi.services.stream.HeaderPrintWriter;

import com.splicemachine.db.iapi.services.compiler.*;
import java.lang.reflect.Modifier;
import com.splicemachine.db.iapi.sql.compile.CodeGeneration;

import com.splicemachine.db.iapi.util.ByteArray;
import com.splicemachine.db.iapi.services.io.FileUtil;
import com.splicemachine.db.iapi.services.i18n.MessageService;

import java.util.Properties;

import java.io.ObjectStreamClass;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;

/**

    An abstract implementation of the ClassFactory. This package can
	be extended to fully implement a ClassFactory. Implementations can
	differ in two areas, how they load a class and how they invoke methods
	of the generated class.

    <P>
	This class manages a hash table of loaded generated classes and
	their GeneratedClass objects.  A loaded class may be referenced
	multiple times -- each class has a reference count associated
	with it.  When a load request arrives, if the class has already
	been loaded, its ref count is incremented.  For a remove request,
	the ref count is decremented unless it is the last reference,
	in which case the class is removed.  This is transparent to users.

	@see com.splicemachine.db.iapi.services.loader.ClassFactory
*/

public abstract class DatabaseClasses
	implements ClassFactory, ModuleControl
{
	/*
	** Fields
	*/

	private	ClassInspector	classInspector;
	private JavaFactory		javaFactory;

	private UpdateLoader		applicationLoader;

	/*
	** Constructor
	*/

	public DatabaseClasses() {
	}

	/*
	** Public methods of ModuleControl
	*/

	public void boot(boolean create, Properties startParams)
		throws StandardException
	{

		classInspector = makeClassInspector( this );

		//
		//The ClassFactory runs per service (database) mode (booted as a service module after AccessFactory).
		//If the code that booted
		//us needs a per-database classpath then they pass in the classpath using
		//the runtime property BOOT_DB_CLASSPATH in startParams


		String classpath = null;
		if (startParams != null) {
			classpath = startParams.getProperty(Property.BOOT_DB_CLASSPATH);
		}

		if (classpath != null) {
			applicationLoader = UpdateLoader.getInstance(classpath, this, true);
		}

		javaFactory = (JavaFactory) com.splicemachine.db.iapi.services.monitor.Monitor.startSystemModule(Module.JavaFactory);
	}



	public void stop() {
		if (applicationLoader != null)
			applicationLoader.close();
	}

	/**
     * For creating the class inspector. On Java 5 and higher, we have a more
	 * capable class inspector.
	 */
	protected   ClassInspector  makeClassInspector( DatabaseClasses dc )
	{
	    return new ClassInspector( dc );
	}

	/*
	**	Public methods of ClassFactory
	*/

	/**
		Here we load the newly added class now, rather than waiting for the
		findGeneratedClass(). Thus we are assuming that the class is going
		to be used sometime soon. Delaying the load would mean storing the class
		data in a file, this wastes cycles and compilcates the cleanup.

		@see ClassFactory#loadGeneratedClass

		@exception	StandardException Class format is bad.
	*/
	public final GeneratedClass loadGeneratedClass(String fullyQualifiedName, ByteArray classDump)
		throws StandardException {


			try {


				return loadGeneratedClassFromData(fullyQualifiedName, classDump);

			} catch (LinkageError le) {

			    WriteClassFile(fullyQualifiedName, classDump, le);

				throw StandardException.newException(SQLState.GENERATED_CLASS_LINKAGE_ERROR,
							le, fullyQualifiedName);

    		} catch (VirtualMachineError vme) { // these may be beyond saving, but fwiw

			    WriteClassFile(fullyQualifiedName, classDump, vme);

			    throw vme;
		    }

	}

    private static void WriteClassFile(String fullyQualifiedName, ByteArray bytecode, Throwable t) {

		// get the un-qualified name and add the extension
        int lastDot = fullyQualifiedName.lastIndexOf((int)'.');
        String filename = fullyQualifiedName.substring(lastDot + 1, fullyQualifiedName.length()) + ".class";

		Object env = Monitor.getMonitor().getEnvironment();
		File dir = env instanceof File ? (File) env : null;

		final File classFile = FileUtil.newFile(dir,filename);

		// find the error stream
		HeaderPrintWriter errorStream = Monitor.getStream();

		try {
            FileOutputStream fis;
            try {
                fis = (FileOutputStream) AccessController.doPrivileged(
                        new PrivilegedExceptionAction() {
                            public Object run() throws IOException {
                                return new FileOutputStream(classFile);
                            }
                        });
            } catch (PrivilegedActionException pae) {
                throw (IOException) pae.getCause();
            }
			fis.write(bytecode.getArray(),
				bytecode.getOffset(), bytecode.getLength());
			fis.flush();
			if (t!=null) {				
				errorStream.printlnWithHeader(MessageService.getTextMessage(MessageId.CM_WROTE_CLASS_FILE, fullyQualifiedName, classFile, t));
			}
			fis.close();
		} catch (IOException e) {
			if (SanityManager.DEBUG)
				SanityManager.THROWASSERT("Unable to write .class file", e);
		}
	}

	public ClassInspector getClassInspector() {
		return classInspector;
	}


	public final Class loadApplicationClass(String className)
		throws ClassNotFoundException {
        
        if (className.startsWith("com.splicemachine.db.")) {
            // Assume this is an engine class, if so
            // try to load from this class loader,
            // this ensures in strange class loader
            // environments we do not get ClassCastExceptions
            // when an engine class is loaded through a different
            // class loader to the rest of the engine.
            try {
                return Class.forName(className);
            } catch (ClassNotFoundException cnfe)
            {
                // fall through to the code below,
                // could be client or tools class
                // in a different loader.
            }
        }
 
		Throwable loadError;
		try {
			try {
				return loadClassNotInDatabaseJar(className);
			} catch (ClassNotFoundException cnfe) {
				if (applicationLoader == null)
					throw cnfe;
				Class c = applicationLoader.loadClass(className, true);
				if (c == null)
					throw cnfe;
				return c;
			}
		}
		catch (SecurityException | LinkageError se)
		{
			// Thrown if the class has been comprimised in some
			// way, e.g. modified in a signed jar.
			loadError = se;	
		}
        throw new ClassNotFoundException(className + " : " + loadError.getMessage());
	}
	
	protected abstract Class loadClassNotInDatabaseJar(String className)
		throws ClassNotFoundException;

	public final Class loadApplicationClass(ObjectStreamClass classDescriptor)
		throws ClassNotFoundException {
		return loadApplicationClass(classDescriptor.getName());
	}

	public boolean isApplicationClass(Class theClass) {

		return theClass.getClassLoader()
			instanceof JarLoader;
	}

	public void notifyModifyJar(boolean reload) throws StandardException  {
		if (applicationLoader != null) {
			applicationLoader.modifyJar(reload);
		}
	}

	/**
		Notify the class manager that the classpath has been modified.

		@exception StandardException thrown on error
	*/
	public void notifyModifyClasspath(String classpath) throws StandardException {

		if (applicationLoader != null) {
			applicationLoader.modifyClasspath(classpath);
		}
	}


	public int getClassLoaderVersion() {
		if (applicationLoader != null) {
			return applicationLoader.getClassLoaderVersion();
		}

		return -1;
	}

	public ByteArray buildSpecificFactory(String className, String factoryName)
		throws StandardException {

		ClassBuilder cb = javaFactory.newClassBuilder(this, CodeGeneration.GENERATED_PACKAGE_PREFIX,
			Modifier.PUBLIC | Modifier.FINAL, factoryName, "com.splicemachine.db.impl.services.reflect.GCInstanceFactory");

		MethodBuilder constructor = cb.newConstructorBuilder(Modifier.PUBLIC);

		constructor.callSuper();
		constructor.methodReturn();
		constructor.complete();
		constructor = null;

		MethodBuilder noArg = cb.newMethodBuilder(Modifier.PUBLIC, ClassName.GeneratedByteCode, "getNewInstance");
		noArg.pushNewStart(className);
		noArg.pushNewComplete(0);
		noArg.methodReturn();
		noArg.complete();
		noArg = null;

		return cb.getClassBytecode();
	}

	/*
	** Class specific methods
	*/
	
	/*
	** Keep track of loaded generated classes and their GeneratedClass objects.
	*/

	protected abstract LoadedGeneratedClass loadGeneratedClassFromData(String fullyQualifiedName, ByteArray classDump); 
}
