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

package com.splicemachine.db.impl.services.bytecode;

import com.splicemachine.db.iapi.services.classfile.VMDescriptor;

/**
	A method descriptor. Ie. something that describes the
	type of a method, parameter types and return types.
	It is not an instance of a method.
	<BR>
	This has no generated class specific state.
 */
class BCMethodDescriptor {

	static final String[] EMPTY = new String[0];

	private final String[] vmParameterTypes;
	private final String vmReturnType;

	private final String vmDescriptor;

	 BCMethodDescriptor(String[] vmParameterTypes, String vmReturnType, BCJava factory) {

		this.vmParameterTypes = vmParameterTypes;
		this.vmReturnType = vmReturnType;

		vmDescriptor = factory.vmType(this);
	}
/*
	static String get(Expression[] vmParameters, String vmReturnType, BCJava factory) {

		int count = vmParameters.length;
		String[] vmParameterTypes;
		if (count == 0) {
			vmParameterTypes = BCMethodDescriptor.EMPTY;
		} else {
			vmParameterTypes = new String[count];
			for (int i =0; i < count; i++) {
				vmParameterTypes[i] = ((BCExpr) vmParameters[i]).vmType();
			}
		}

		return new BCMethodDescriptor(vmParameterTypes, vmReturnType, factory).toString();
	}
*/
	static String get(String[] vmParameterTypes, String vmReturnType, BCJava factory) {

		return new BCMethodDescriptor(vmParameterTypes, vmReturnType, factory).toString();
	}

	/**
	 * builds the JVM method descriptor for this method as
	 * defined in JVM Spec 4.3.3, Method Descriptors.
	 */
	String buildMethodDescriptor() {

		int paramCount = vmParameterTypes.length;

		int approxLength = (30 * (paramCount + 1));

		StringBuffer methDesc = new StringBuffer(approxLength);

		methDesc.append(VMDescriptor.C_METHOD);

		for (int i = 0; i < paramCount; i++) {
			methDesc.append(vmParameterTypes[i]);
		}

		methDesc.append(VMDescriptor.C_ENDMETHOD);
		methDesc.append(vmReturnType);

		return methDesc.toString();
	}

	public String toString() {
		return vmDescriptor;
	}
		
	
	public int hashCode() {
		return vmParameterTypes.length | (vmReturnType.hashCode() & 0xFFFFFF00);
	}

	public boolean equals(Object other) {
		if (!(other instanceof BCMethodDescriptor))
			return false;

		BCMethodDescriptor o = (BCMethodDescriptor) other;


		if (o.vmParameterTypes.length != vmParameterTypes.length)
			return false;

		for (int i = 0; i < vmParameterTypes.length; i++) {
			if (!vmParameterTypes[i].equals(o.vmParameterTypes[i]))
				return false;
		}

		return vmReturnType.equals(o.vmReturnType);
	}
}
