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
package com.splicemachine.db.iapi.services.io;

import java.io.*;

import com.splicemachine.db.iapi.services.sanity.SanityManager;


class DebugByteTeeOutputStream extends FilterOutputStream {
	private AccessibleByteArrayOutputStream tee = new AccessibleByteArrayOutputStream(256);

	DebugByteTeeOutputStream(OutputStream out) {
		super(out);
	}
	
	public void write(int b) throws IOException {
		out.write(b);
		tee.write(b);
	}

	public void write(byte[] b, int off, int len) throws IOException {

		out.write(b,off,len);
		tee.write(b,off,len);
	}


	void checkObject(Formatable f) {

		ByteArrayInputStream in = 
            new ByteArrayInputStream(tee.getInternalByteArray(), 0, tee.size());

		FormatIdInputStream fin = new FormatIdInputStream(in);

		// now get an empty object given the format identification
		// read it in
		// then compare it???

		Formatable f1 = null;
		try {

			f1 = (Formatable) fin.readObject();

			if (f1.equals(f)) {
				return;
			}

			// If the two objects are not equal and it looks
			// like they don't implement their own equals()
			// (which requires a matching hashCode() then
			// just return. The object was read sucessfully.

			if ((f1.hashCode() == System.identityHashCode(f1)) &&
				(f.hashCode() == System.identityHashCode(f))) {
            }

		} catch (Throwable t) {

            // for debugging purposes print this both to db.log and to
            // System.out.
            String err_msg = 
                "FormatableError:read error    : " + t.toString() + 
                "\nFormatableError:class written : " + f.getClass();

            err_msg += (f1 == null) ? 
                "FormatableError:read back as null" :
                ("FormatableError:class read    : " + f1.getClass());

            err_msg +=
                "FormatableError:write id      : " + 
                    FormatIdUtil.formatIdToString(f.getTypeFormatId());

            if (f1 != null) {
                err_msg += "FormatableError:read id       : " + 
                    FormatIdUtil.formatIdToString(f1.getTypeFormatId());
            }

            System.out.println(err_msg);
			t.printStackTrace(System.out);

            if (SanityManager.DEBUG) {
                SanityManager.DEBUG_PRINT("DebugByteTeeOutputStream", err_msg);
                SanityManager.showTrace(t);
            }
		}

		//System.out.println("FormatableError:Class written " + f.getClass() + " format id " + f.getTypeFormatId());
		//if (f1 != null)
			//System.out.println("FormatableError:Class read    " + f1.getClass() + " format id " + f1.getTypeFormatId());
	}
}
