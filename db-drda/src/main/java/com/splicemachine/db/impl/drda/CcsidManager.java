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
package com.splicemachine.db.impl.drda;

import java.nio.ByteBuffer;

// Peforms character conversions.
abstract class CcsidManager
{
  byte space; // ' ' character
  byte dot;   // '.' character

  // Byte array used to convert numbers into
  // bytes containing the character representation "value" for the particular ccsid.
  byte[] numToCharRepresentation;

  /* DRDA CCSID level for UTF8 */
  public static final int UTF8_CCSID = 1208;
  
  CcsidManager (byte space, byte dot, byte[] numToCharRepresentation)
  {
    this.space = space;
    this.dot = dot;
    this.numToCharRepresentation = numToCharRepresentation;
  }

  /**
   * Returns the length in bytes for the String str using a particular ccsid.
   * @param str The Java String from which to obtain the length.
   * @return The length in bytes of the String str.
   */
  abstract int getByteLength (String str);
  
  // Convert a Java String into bytes for a particular ccsid.
  //
  // @param sourceString A Java String to convert.
  // @return A new byte array representing the String in a particular ccsid.
  abstract byte[] convertFromJavaString (String sourceString);


    /**
     * Convert a Java String into bytes for a particular ccsid.
     * The String is converted into a buffer provided by the caller.
     *
     * @param sourceString  A Java String to convert.
     * @param buffer        The buffer to convert the String into.
     */
    abstract void convertFromJavaString(String sourceString, ByteBuffer buffer);

  // Convert a byte array representing characters in a particular ccsid into a Java String.
  //
  // @param sourceBytes An array of bytes to be converted.
  // @return String A new Java String Object created after conversion.
  abstract String convertToJavaString (byte[] sourceBytes);


  /**
   * Convert a byte array representing characters in a particular ccsid into a Java String.
   * 
   * Mind the fact that for certain encodings (e.g. UTF8), the offset and numToConvert
   * actually represent characters and 1 character does not always equal to 1 byte.
   * 
   * @param sourceBytes An array of bytes to be converted.
   * @param offset An offset indicating first byte to convert.
   * @param numToConvert The number of bytes to be converted.
   * @return A new Java String Object created after conversion.
   */
  abstract String convertToJavaString (byte[] sourceBytes, int offset, int numToConvert);

}
