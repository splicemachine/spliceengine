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
package com.splicemachine.db.impl.drda;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import com.splicemachine.db.iapi.services.sanity.SanityManager;

public class Utf8CcsidManager extends CcsidManager {

    public Utf8CcsidManager() {
        super((byte)' ',
              (byte)'.',
              new byte[] {
              //      '0',       '1',      '2',        '3',      '4',
                (byte)0xf0,(byte)0xf1,(byte)0xf2,(byte)0xf3,(byte)0xf4,
              //      '5',       '6',       '7',       '8',      '9',
                (byte)0xf5,(byte)0xf6,(byte)0xf7,(byte)0xf8,(byte)0xf9,
              //      'A',       'B',       'C',       'D',      'E',
                (byte)0xc1,(byte)0xc2,(byte)0xc3,(byte)0xc4,(byte)0xc5,
              //      'F',       'G',       'H',      'I',       'J',
                (byte)0xc6,(byte)0xc7,(byte)0xc8,(byte)0xc9,(byte)0xd1,
              //     'K',        'L',       'M',       'N',      'O',
                (byte)0xd2,(byte)0xd3,(byte)0xd4,(byte)0xd5,(byte)0xd6,
              //     'P'
                (byte)0xd7
              });
    }
    
    public byte[] convertFromJavaString(String sourceString){
        try {
            return sourceString.getBytes("UTF-8");
        } catch (UnsupportedEncodingException e) {
            if (SanityManager.DEBUG) {
                SanityManager.THROWASSERT("Could not convert Java String to byte[] in UTF-8", e);
            }
        }
        return null;
    }
    
    public String convertToJavaString(byte[] sourceBytes) {
       try {
           return new String(sourceBytes,"UTF-8");
        } catch (UnsupportedEncodingException e) {
            if (SanityManager.DEBUG) {
                SanityManager.THROWASSERT("Could not convert byte[] to Java String using UTF-8 encoding", e);
            }
        }
        return null;
    }

    /**
     * Offset and numToConvert are given in terms of bytes! Not characters!
     */
    public String convertToJavaString(byte[] sourceBytes, int offset, int numToConvert) {
        try {
            return new String(sourceBytes, offset, numToConvert, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            if (SanityManager.DEBUG) {
                SanityManager.THROWASSERT("Could not convert byte[] to Java String using UTF-8 encoding with offset",e);
            }
        }
        return null;
    }

    /* Keeping this method out for now.
     * The CcsidManager for **client** has this method as abstract
     * but the one for the server doesn't seem to have it.
     */
    /*int maxBytesPerChar() {
        return 4;
    }*/

    public void convertFromJavaString(String sourceString, ByteBuffer buffer) {
        buffer.put(convertFromJavaString(sourceString));
    }

    int getByteLength(String str) {
        try {
            return str.getBytes("UTF-8").length;
        } catch (UnsupportedEncodingException e) {
            if (SanityManager.DEBUG) {
                SanityManager.THROWASSERT("Could not obtain byte length of Java String in Utf8CcsidManager",e);
            }
        }
        return -1;
    }

}
