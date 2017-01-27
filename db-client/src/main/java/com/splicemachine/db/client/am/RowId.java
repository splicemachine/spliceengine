/*
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
 * Splice Machine, Inc. has modified this file.
 *
 * All Splice Machine modifications are Copyright 2012 - 2017 Splice Machine, Inc.,
 * and are licensed to you under the License; you may not use this file except in
 * compliance with the License.
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 */

package com.splicemachine.db.client.am;

/**
 * Created by jyuan on 9/29/14.
 */
public class RowId implements java.sql.RowId{

    private static final char[] hexArray = {'0','1','2','3','4','5','6','7','8','9','A','B','C','D','E','F'};
    byte[] bytes;

    public RowId() {
        bytes = null;
    }

    public RowId(byte[] bytes) {
        this.bytes = bytes;
    }

    public  boolean equals(Object obj) {
        if (this == obj)
            return true;

        if (obj instanceof RowId) {
            RowId other = (RowId) obj;
            if (bytes != null && other.bytes != null && bytes.length == other.bytes.length) {
                for (int i = 0; i < bytes.length; ++i) {
                    if (bytes[i] != other.bytes[i]) {
                        return false;
                    }
                }
                return true;
            }
        }

        return false;
    }

    public  byte[] 	getBytes() {
        return bytes;
    }

    public  int hashCode() {
        return 0;
    }

    public  String 	toString() {
        return toHex(bytes, 0, bytes.length);
    }

    private String toHex(byte[] bytes,int offset,int length) {
        if(bytes==null || length<=0) return "";
        char[] hexChars = new char[length * 2];
        int v;
        for ( int j = 0,k=offset; j < length; k++,j++ ) {
            v = bytes[k] & 0xFF;
            hexChars[j * 2] = hexArray[v >>> 4];
            hexChars[j * 2 + 1] = hexArray[v & 0x0F];
        }
        return new String(hexChars);
    }
}
