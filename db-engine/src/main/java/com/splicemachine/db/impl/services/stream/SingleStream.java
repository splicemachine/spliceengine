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

package com.splicemachine.db.impl.services.stream;

import com.splicemachine.db.iapi.services.stream.InfoStreams;
import com.splicemachine.db.iapi.services.stream.HeaderPrintWriter;
import com.splicemachine.db.iapi.services.stream.PrintWriterGetHeader;
import com.splicemachine.db.iapi.services.monitor.ModuleControl;
import com.splicemachine.db.iapi.services.monitor.Monitor;
import com.splicemachine.db.iapi.reference.Property;
import com.splicemachine.db.iapi.services.property.PropertyUtil;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.File;
import java.io.Writer;
import java.util.Properties;
import java.lang.reflect.Method;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.lang.reflect.Member;
import java.lang.reflect.InvocationTargetException;

import com.splicemachine.db.iapi.services.i18n.MessageService;
import com.splicemachine.db.iapi.services.io.FileUtil;
import com.splicemachine.db.shared.common.reference.MessageId;

/**
 *
 * The Basic Services provide InfoStreams for reporting
 * information. Two streams are provided: trace and error.
 * It is configurable where these streams are directed.
 * <p>
 * Errors will be printed to the error stream in addition
 * to being sent to the client.
 * <p>
 * By default both streams are sent to an error log
 * for the system. When creating a message for a stream,
 * you can create an initial entry with header information
 * and then append to it as many times as desired.
 * <p>
 * Note: if character encodings are needed, the use of
 * java.io.*OutputStream's should be replaced with
 * java.io.*Writer's (assuming the Writer interface
 * remains stable in JDK1.1)
 *
 */
public class SingleStream implements InfoStreams, ModuleControl {

	/*
	** Instance fields
	*/
	private HeaderPrintWriter theStream;


	/**
		  The no-arg public constructor for ModuleControl's use.
	 */
	public SingleStream() {
	}

	/**
	 * @see com.splicemachine.db.iapi.services.monitor.ModuleControl#boot
	 */
	public void boot(boolean create, Properties properties) {
		theStream = makeStream();
	}


	/**
	 * @see com.splicemachine.db.iapi.services.monitor.ModuleControl#stop
	 */
	public void stop()	{
		((BasicHeaderPrintWriter) theStream).complete();
	}

	/*
	 * InfoStreams interface
	 */

	/**
	 * @see com.splicemachine.db.iapi.services.stream.InfoStreams#stream
	 */
	public HeaderPrintWriter stream() {
		return theStream;
	}

	//
	// class interface
	//

	/**
		Make the stream; note that service properties override
		application and system properties.

	 */
	protected HeaderPrintWriter makeStream() {

		// get the header
		PrintWriterGetHeader header = makeHeader();
		HeaderPrintWriter hpw = makeHPW(header);

		// If hpw == null then no properties were specified for the stream
		// so use/create the default stream.
		if (hpw == null)
			hpw = createDefaultStream(header);
		return hpw;
	}

	/**
		Return a new header object.
	*/
	private PrintWriterGetHeader makeHeader() {

		return new BasicGetLogHeader(true, true, (String) null);
	}

	/**
		create a HeaderPrintWriter based on the header.
		Will still need to determine the target type.
	 */
	private HeaderPrintWriter makeHPW(PrintWriterGetHeader header) {

		// the type of target is based on which property is used
		// to set it. choices are file, method, field, stream

		String target = PropertyUtil.
                   getSystemProperty(Property.ERRORLOG_FILE_PROPERTY);
		if (target!=null)
			return makeFileHPW(target, header);

		target = PropertyUtil.
                   getSystemProperty(Property.ERRORLOG_METHOD_PROPERTY);
		if (target!=null) 
			return makeMethodHPW(target, header);

		target = PropertyUtil.
                   getSystemProperty(Property.ERRORLOG_FIELD_PROPERTY);
		if (target!=null) 
			return makeFieldHPW(target, header);

		return null;
	}

	
	private HeaderPrintWriter makeMethodHPW(String methodInvocation,
											PrintWriterGetHeader header) {

		int lastDot = methodInvocation.lastIndexOf('.');
		String className = methodInvocation.substring(0, lastDot);
		String methodName = methodInvocation.substring(lastDot+1);

		Throwable t;
		try {
			Class theClass = Class.forName(className);

			try {
				Method theMethod = theClass.getMethod(methodName,  new Class[0]);

				if (!Modifier.isStatic(theMethod.getModifiers())) {
					HeaderPrintWriter hpw = useDefaultStream(header);
					hpw.printlnWithHeader(theMethod.toString() + " is not static");
					return hpw;
				}

				try {
					return makeValueHPW(theMethod, theMethod.invoke((Object) null, 
						new Object[0]), header, methodInvocation);
				} catch (IllegalAccessException | IllegalArgumentException iae) {
					t = iae;
				} catch (InvocationTargetException ite) {
					t = ite.getTargetException();
				}

			} catch (NoSuchMethodException nsme) {
				t = nsme;
			}
		} catch (ClassNotFoundException | SecurityException cnfe) {
			t = cnfe;
		}
        return useDefaultStream(header, t);

	}


	private HeaderPrintWriter makeFieldHPW(String fieldAccess,
											PrintWriterGetHeader header) {

		int lastDot = fieldAccess.lastIndexOf('.');
		String className = fieldAccess.substring(0, lastDot);
		String fieldName = fieldAccess.substring(lastDot+1,
							  fieldAccess.length());

		Throwable t;
		try {
			Class theClass = Class.forName(className);

			try {
				Field theField = theClass.getField(fieldName);
		
				if (!Modifier.isStatic(theField.getModifiers())) {
					HeaderPrintWriter hpw = useDefaultStream(header);
					hpw.printlnWithHeader(theField.toString() + " is not static");
					return hpw;
				}

				try {
					return makeValueHPW(theField, theField.get((Object) null), 
						header, fieldAccess);
				} catch (IllegalAccessException | IllegalArgumentException iae) {
					t = iae;
				}

            } catch (NoSuchFieldException nsfe) {
				t = nsfe;
			}
		} catch (ClassNotFoundException | SecurityException cnfe) {
			t = cnfe;
		}
        return useDefaultStream(header, t);

		/*
			If we decide it is a bad idea to use reflect and need
			an alternate implementation, we can hard-wire those
			fields that we desire to give configurations access to,
			like so:

		if ("java.lang.System.out".equals(fieldAccess))
		 	os = System.out;
		else if ("java.lang.System.err".equals(fieldAccess))
		 	os = System.err;
		*/
	}

	private HeaderPrintWriter makeValueHPW(Member whereFrom, Object value,
		PrintWriterGetHeader header, String name) {

		return new BasicHeaderPrintWriter(null, header, false, name);
	}
 

	/**
		Used when no configuration information exists for a stream.
	*/
	private HeaderPrintWriter createDefaultStream(PrintWriterGetHeader header) {
		return makeFileHPW("derby.log", header);
	}

	/**
		Used when creating a stream creates an error.
	*/
	private HeaderPrintWriter useDefaultStream(PrintWriterGetHeader header) {

		return new BasicHeaderPrintWriter(null, header, false, "System.err");
	}

	private HeaderPrintWriter useDefaultStream(PrintWriterGetHeader header, Throwable t) {

		HeaderPrintWriter hpw = useDefaultStream(header);

        while (t != null) {
            Throwable causedBy = t.getCause();
            String causedByStr =
                MessageService.getTextMessage(MessageId.CAUSED_BY);
            hpw.printlnWithHeader(
                t.toString() + (causedBy != null ? " " + causedByStr : ""));
            t = causedBy;
        }

		return hpw;
	}

	/*
	** Priv block code, moved out of the old Java2 version.
	*/

    private String PBfileName;
    private PrintWriterGetHeader PBheader;

	private HeaderPrintWriter makeFileHPW(String fileName, PrintWriterGetHeader header)
    {
        this.PBfileName = fileName;
        this.PBheader = header;
        return new BasicHeaderPrintWriter(null, header, false, fileName);
    }
}

