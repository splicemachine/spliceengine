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

package com.splicemachine.db.impl.services.bytecode;

import com.splicemachine.db.iapi.services.compiler.ClassBuilder;
import com.splicemachine.db.iapi.services.loader.ClassFactory;
import com.splicemachine.db.iapi.services.loader.GeneratedClass;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.services.stream.HeaderPrintWriter;
import com.splicemachine.db.iapi.services.monitor.Monitor;

import com.splicemachine.db.iapi.util.ByteArray;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;

/**
 * This is a common superclass for the various impls.
 * Saving class files is a common thing to do.
 *
 */
public abstract class GClass implements ClassBuilder {

	protected ByteArray bytecode;
	protected final ClassFactory cf;
	protected final String qualifiedName;



	public GClass(ClassFactory cf, String qualifiedName) {
		this.cf = cf;
		this.qualifiedName = qualifiedName;
	}
	public String getFullName() {
		return qualifiedName;
	}
	public GeneratedClass getGeneratedClass() throws StandardException {
		return cf.loadGeneratedClass(qualifiedName, getClassBytecode());
	}

	protected void writeClassFile(String dir, boolean logMessage, Throwable t)
		throws StandardException {

		if (SanityManager.DEBUG) {

		if (bytecode ==  null) getClassBytecode(); // not recursing...

		if (dir == null) dir="";

		String filename = getName(); // leave off package

		filename = filename + ".class";

		final File classFile = new File(dir,filename);

		FileOutputStream fos = null;
		try {
			try {
				fos =  (FileOutputStream)AccessController.doPrivileged(
						new PrivilegedExceptionAction() {
							public Object run()
							throws FileNotFoundException {
								return new FileOutputStream(classFile);
							}
						});
			} catch (PrivilegedActionException pae) {
				throw (FileNotFoundException)pae.getCause();
			}
			fos.write(bytecode.getArray(),
				bytecode.getOffset(), bytecode.getLength());
			fos.flush();
			if (logMessage) {
		        // find the error stream
		        HeaderPrintWriter errorStream = Monitor.getStream();
				errorStream.printlnWithHeader("Wrote class "+getFullName()+" to file "+classFile.toString()+". Please provide support with the file and the following exception message: "+t);
			}
			fos.close();
		} catch (IOException e) {
			if (SanityManager.DEBUG)
				SanityManager.THROWASSERT("Unable to write .class file", e);
		}
		}
	}

	final void validateType(String typeName1)
	{
	    if (SanityManager.DEBUG)
	    {
            SanityManager.ASSERT(typeName1 != null);

            String typeName = typeName1.trim();

            if ("void".equals(typeName)) return;

	        // first remove all array-ness
	        while (typeName.endsWith("[]")) typeName = typeName.substring(0,typeName.length()-2);

            SanityManager.ASSERT(!typeName.isEmpty());

	        // then check for primitive types
	        if ("boolean".equals(typeName)) return;
	        if ("byte".equals(typeName)) return;
	        if ("char".equals(typeName)) return;
	        if ("double".equals(typeName)) return;
	        if ("float".equals(typeName)) return;
	        if ("int".equals(typeName)) return;
	        if ("long".equals(typeName)) return;
	        if ("short".equals(typeName)) return;

	        // then see if it can be found
	        // REVISIT: this will fail if ASSERT is on and the
	        // implementation at hand is missing the target type.
	        // We do plan to generate classes against
	        // different implementations from the compiler's implementation
	        // at some point...
			try {
				if (cf == null)
					Class.forName(typeName);
				else
					cf.loadApplicationClass(typeName);
			} catch (ClassNotFoundException cnfe) {
				SanityManager.THROWASSERT("Class "+typeName+" not found", cnfe);
			}

	        // all the checks succeeded, it must be okay.
        }
	}
}
