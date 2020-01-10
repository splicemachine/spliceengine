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

package com.splicemachine.db.iapi.util;

/**
	Factory methods for reusable objects. So far, the objects allocated
	by this factory are all immutable. Any immutable object can be re-used.

	All the methods in this class are static.
*/
public class ReuseFactory {

	/** Private constructor so no instances can be made */
	private ReuseFactory() {
	}

	private static final Integer[] staticInts =
		{0, 1, 2, 3,
                4, 5, 6, 7,
                8, 9, 10, 11,
                12, 13, 14, 15,
                16, 17, 18};
	private static final Integer FIFTY_TWO = 52;
	private static final Integer TWENTY_THREE = 23;
	private static final Integer MAXINT = Integer.MAX_VALUE;
	private static final Integer MINUS_ONE = -1;

	public static Integer getInteger(int i)
	{
		if (i >= 0 && i < staticInts.length)
		{
			return staticInts[i];
		}
		else
		{
			// Look for other common values
			switch (i)
			{
			  case 23:
				return TWENTY_THREE;	// precision of Int

			  case 52:
				return FIFTY_TWO;	// precision of Double

			  case Integer.MAX_VALUE:
				return MAXINT;

			  case -1:
				return MINUS_ONE;

			  default:
				return i;
			}
		}
	}

	private static final Short[] staticShorts =
		{(short) 0, (short) 1, (short) 2,
                (short) 3, (short) 4, (short) 5,
                (short) 6, (short) 7, (short) 8,
                (short) 9, (short) 10};

	public static Short getShort(short i)
	{
		if (i >= 0 && i < staticShorts.length)
			return staticShorts[i];
		else
			return i;
	}

	private static final Byte[] staticBytes =
		{(byte) 0, (byte) 1, (byte) 2,
                (byte) 3, (byte) 4, (byte) 5,
                (byte) 6, (byte) 7, (byte) 8,
                (byte) 9, (byte) 10};

	public static Byte getByte(byte i)
	{
		if (i >= 0 && i < staticBytes.length)
			return staticBytes[i];
		else
			return i;
	}

	private static final Long[] staticLongs =
		{0L, 1L, 2L,
                3L, 4L, 5L,
                6L, 7L, 8L,
                9L, 10L};

	public static Long getLong(long i)
	{
		if (i >= 0 && i < staticLongs.length)
			return staticLongs[(int) i];
		else
			return i;
	}

    public static Boolean getBoolean( boolean b)
    {
        return b ? Boolean.TRUE : Boolean.FALSE;
    }

	private static final byte[] staticZeroLenByteArray = new byte[0];
	public static byte[] getZeroLenByteArray() 
	{
		return staticZeroLenByteArray;
	}
}
