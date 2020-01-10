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

package com.splicemachine.db.iapi.services.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
  Utility class with static methods for constructing and reading the byte array
  representation of format id's.

  <P>This utility supports a number of families of format ids. The byte array
  form of each family is a different length. In all cases the first two bits
  of the first byte indicate the family for an id. The list below describes
  each family and gives its two bit identifier in parens.

  <UL> 
  <LI> (0) - The format id is a one byte number between 0 and 63 inclusive. 
             The byte[] encoding fits in one byte.
  <LI> (1) - The format id is a two byte number between 16384 to 32767
             inclusive. The byte[] encoding stores the high order byte
			 first. 
  <LI> (2) - The format id is four byte number between 2147483648 and
             3221225471 inclusive. The byte[] encoding stores the high
			 order byte first.
  <LI> (3) - Future expansion.
  </UL>
 */
public final class FormatIdUtil
{

	private FormatIdUtil() {
	}

	public static int getFormatIdByteLength(int formatId) {
			return 2;
	}

	public static void writeFormatIdInteger(DataOutput out, int formatId) throws IOException {
		out.writeShort(formatId);
	}

	public static int readFormatIdInteger(DataInput in)
		throws IOException {

		return in.readUnsignedShort();
	}

	public static int readFormatIdInteger(byte[] data) {

		int a = data[0];
		int b = data[1];
		return (((a & 0xff) << 8) | (b & 0xff));
	}

	public static String formatIdToString(int fmtId) {

		return Integer.toString(fmtId);
	}

}
