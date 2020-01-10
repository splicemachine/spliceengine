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

import com.splicemachine.db.iapi.services.classfile.VMDescriptor;
import com.splicemachine.db.iapi.services.classfile.ClassHolder;

final class Type {

	static final Type LONG = new Type("long", VMDescriptor.LONG);
	static final Type INT = new Type("int", VMDescriptor.INT);
	static final Type SHORT = new Type("short", VMDescriptor.SHORT);
	static final Type BYTE = new Type("byte", VMDescriptor.BYTE);
	static final Type BOOLEAN = new Type("boolean", VMDescriptor.BOOLEAN);
	static final Type FLOAT = new Type("float", VMDescriptor.FLOAT);
	static final Type DOUBLE = new Type("double", VMDescriptor.DOUBLE);
	static final Type STRING = new Type("java.lang.String", "Ljava/lang/String;");

	private final String javaName; // e.g. java.lang.Object
	private final short vmType; // e.g. BCExpr.vm_reference
	private final String vmName; // e.g. Ljava/lang/Object;
	final String vmNameSimple; // e.g. java/lang/Object

	Type(String javaName, String vmName) {
		this.vmName = vmName;
		this.javaName = javaName;
		vmType = BCJava.vmTypeId(vmName);
		vmNameSimple = ClassHolder.convertToInternalClassName(javaName);
	}

	/*
	** Class specific methods.
	*/
	
	String javaName() {
		return javaName;
	}

	/**
	 * Get the VM Type name (java/lang/Object)
	 */
	String vmName() {
		return vmName;
	}
	/**
		Get the VM type (eg. VMDescriptor.INT)
	*/
	short vmType() {
		return vmType;
	}

	int width() {
		return Type.width(vmType);
	}

	static int width(short type) {
		switch (type) {
		case BCExpr.vm_void:
			return 0;
		case BCExpr.vm_long:
		case BCExpr.vm_double:
			return 2;
		default:
			return 1;
		}
	}
}
