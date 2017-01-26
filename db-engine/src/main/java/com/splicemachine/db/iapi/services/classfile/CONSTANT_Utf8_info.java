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

package com.splicemachine.db.iapi.services.classfile;

import java.io.IOException;

/** Constant Pool class - pages 92-99 */

/** Utf8- page 100 - Section 4.4.7 */
public final class CONSTANT_Utf8_info extends ConstantPoolEntry {
	private final String value;
	private int asString;
	private int asCode;
	
	CONSTANT_Utf8_info(String value) {
		super(VMDescriptor.CONSTANT_Utf8);
		this.value = value;
	}

	Object getKey() {
		return value;
	}

	/**
		We assume here that the String is ASCII, thus this
		might return a size smaller than actual size.
	*/
	int classFileSize() {
		// 1 (tag) + 2 (utf length) + string length
		return 1 + 2 + value.length();
	}

	public String toString() {
		return value;
	}

	// if this returns 0 then the caller must put another CONSTANT_Utf8_info into the
	// constant pool with no hash table entry and then call setAlternative() with its index.
	int setAsCode() {
		if (ClassHolder.isExternalClassName(value))
		{
			if (asString == 0) {
				// only used as code at the moment
				asCode = getIndex();
			}

			return asCode;
		}
		// no dots in the string so it can be used as a JVM internal string and
		// an external string.
		return getIndex();
	}

	int setAsString() {
		if (ClassHolder.isExternalClassName(value))
		{

			if (asCode == 0) {
				// only used as String at the moment
				asString = getIndex();
			}
			return asString;
		}
		
		// no dots in the string so it can be used as a JVM internal string and
		// an external string.
		return getIndex();
	}

	void setAlternative(int index) {

		if (asCode == 0)
			asCode = index;
		else
			asString = index;
	}

	void put(ClassFormatOutput out) throws IOException {
		super.put(out);

		if (getIndex() == asCode)
		{
			out.writeUTF(ClassHolder.convertToInternalClassName(value));
		}
		else
			out.writeUTF(value);
	}
}
