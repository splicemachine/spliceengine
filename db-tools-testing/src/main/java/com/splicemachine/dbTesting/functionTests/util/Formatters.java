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

package com.splicemachine.dbTesting.functionTests.util;


public class Formatters {

	static final char[] hexDigits = { '0', '1', '2', '3',
									  '4', '5', '6', '7',
									  '8', '9', 'A', 'B',
									  'C', 'D', 'E', 'F' };

	/** This method converts the non-ASCII characters in the input
	 *  parameter to unicode escape sequences.
	 * @param in    String to format
	 * @return String containing unicode escape sequences for non-ASCII chars
	 */
	public static String format(String in) {
		if (in == null)
			return null;

		StringBuffer out = new StringBuffer(in.length());
		char hexValue[] = new char[4];

		for (int i = 0; i < in.length(); i++) {
			char inChar = in.charAt(i);

			if (inChar < 128) {
				out.append(inChar);
			} else {
				out.append("\\u");

				int number = (int) inChar;

				int digit = number % 16;

				hexValue[3] = hexDigits[digit];

				number /= 16;

				digit = number % 16;

				hexValue[2] = hexDigits[digit];

				number /= 16;

				digit = number %16;

				hexValue[1] = hexDigits[digit];

				number /= 16;

				digit = number % 16;

				hexValue[0] = hexDigits[digit];

				out.append(hexValue);
			}
		}

		return out.toString();
	}


	/**
	 * repeatChar is used to create strings of varying lengths.
	 * called from various tests to test edge cases and such.
	 *
	 * @param c             character to repeat
	 * @param repeatCount   Number of times to repeat character
	 * @return              String of repeatCount characters c
	 */
   public static String repeatChar(String c, int repeatCount)
   {
	   char ch = c.charAt(0);

	   char[] chArray = new char[repeatCount];
	   for (int i = 0; i < repeatCount; i++)
	   {
		   chArray[i] = ch;
	   }

	   return new String(chArray);

   }

	/**
	 * Pads out a string to the specified size
	 *
	 * @param oldValue value to be padded
	 * @param size     size of resulting string
	 * @return oldValue padded with spaces to the specified size
	 */
	public static String padString(String oldValue, int size)
	{
		String newValue = oldValue;
		if (newValue != null)
		{
			char [] newCharArr = new char[size];					
			oldValue.getChars(0,oldValue.length(),newCharArr,0);
			java.util.Arrays.fill(newCharArr,oldValue.length(),
								  newCharArr.length -1, ' ');
			newValue = new String (newCharArr);
		}
			
		return newValue;
	}

}
