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

package com.splicemachine.db.iapi.services.classfile;

import com.splicemachine.db.iapi.services.io.AccessibleByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;


/** A wrapper around DataOutputStream to provide input functions in terms
    of the types defined on pages 83 of the Java Virtual Machine spec.

	For this types use these methods of DataOutputStream
	<UL>
	<LI>float - writeFloat
	<LI>long - writeLong
	<LI>double - writeDouble
	<LI>UTF/String - writeUTF
	<LI>U1Array - write(byte[])
	</UL>
 */

public final class ClassFormatOutput extends DataOutputStream {

	public ClassFormatOutput() {
		this(512);
	}

	public ClassFormatOutput(int size) {
		this(new AccessibleByteArrayOutputStream(size));
	}
	public ClassFormatOutput(java.io.OutputStream stream) {
		super(stream);
	}
	public void putU1(int i) throws IOException {
		// ensure the format of the class file is not
		// corrupted by writing an incorrect, truncated value.
		if (i > 255)
			ClassFormatOutput.limit("U1", 255, i);
		write(i);
	}
	public void putU2(int i) throws IOException {
		putU2("U2", i);

	}
	public void putU2(String limit, int i) throws IOException {
		
		// ensure the format of the class file is not
		// corrupted by writing an incorrect, truncated value.
		if (i > 65535)
			ClassFormatOutput.limit(limit, 65535, i);
		write(i >> 8);
		write(i);
	}
	public void putU4(int i) throws IOException {
		writeInt(i);
	}

	public void writeTo(OutputStream outTo) throws IOException {
		((AccessibleByteArrayOutputStream) out).writeTo(outTo);
	}

	/**
		Get a reference to the data array the class data is being built
		in. No copy is made.
	*/
	public byte[] getData() {
		return ((AccessibleByteArrayOutputStream) out).getInternalByteArray();
	}

	/**
	 * Throw an ClassFormatError if a limit of the Java class file format is reached.
	 * @param name Terse limit description from JVM spec.
	 * @param limit What the limit is.
	 * @param value What the value for the current class is
	 * @throws IOException Thrown when limit is exceeded.
	 */
	static void limit(String name, int limit, int value)
		throws IOException
	{
		throw new IOException(name + "(" + value + " > " + limit + ")");
	}
}
