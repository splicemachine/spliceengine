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

package com.splicemachine.db.iapi.types;

import java.io.CharArrayReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Serializable;
import java.io.Writer;
import java.sql.Clob;
import java.sql.SQLException;

import com.splicemachine.db.iapi.reference.SQLState;

/**
 * Copied from the Harmony project's implementation of javax.sql.rowset.serial.SerialClob
 * at subversion revision 946981.
 */
public class HarmonySerialClob implements Clob, Serializable, Cloneable {

    // required by serialized form
    private static final long serialVersionUID = -1662519690087375313L;

    private char[] buf;

    // required by serialized form
    private Clob clob;

    private long len;

    // required by serialized form
    private long origLen;

    public HarmonySerialClob( String raw ) { this( raw.toCharArray() ); }

    public HarmonySerialClob(char[] ch) {
        buf = new char[ch.length];
        origLen = ch.length;
        len = origLen;
        System.arraycopy(ch, 0, buf, 0, (int) len);
    }

    public HarmonySerialClob(Clob clob) throws SQLException {
        Reader characterStream;

        if (clob == null) { throw new IllegalArgumentException(); }
        if ((characterStream = clob.getCharacterStream()) == null
                && clob.getAsciiStream() == null) { throw new IllegalArgumentException(); }

        this.clob = clob;
        origLen = clob.length();
        len = origLen;
        buf = new char[(int) len];
        try {
            characterStream.read(buf);
        } catch (IOException e) {

            throw new SQLException("SerialClob: "
                    + e.getMessage(), e);
        }
    }

    public long length() throws SQLException {
        checkValidation();
        return len;
    }

    public InputStream getAsciiStream() throws SQLException {
        checkValidation();
        if (clob == null) { throw new IllegalStateException(); }
        return clob.getAsciiStream();
    }

    public Reader getCharacterStream() throws SQLException {
        checkValidation();
        return new CharArrayReader(buf);
    }

    public String getSubString(long pos, int length) throws SQLException {
        checkValidation();
        if (length < 0)
        {
            throw HarmonySerialBlob.makeSQLException( SQLState.BLOB_NONPOSITIVE_LENGTH, new Object[] {length} );
        }
        if (pos < 1 || pos > len || pos + length > len + 1)
        {
            throw HarmonySerialBlob.makeSQLException( SQLState.BLOB_BAD_POSITION, new Object[] {pos} );
        }
        try {
            return new String(buf, (int) (pos - 1), length);
        } catch (StringIndexOutOfBoundsException e) {
            throw new SQLException();
        }
    }

    public long position(Clob searchClob, long start) throws SQLException {
        checkValidation();
        String searchString = searchClob.getSubString(1, (int) searchClob
                .length());
        return position(searchString, start);
    }

    public long position(String searchString, long start)
            throws SQLException, SQLException {
        checkValidation();
        if (start < 1 || len - (start - 1) < searchString.length()) {
            return -1;
        }
        char[] pattern = searchString.toCharArray();
        for (int i = (int) start - 1; i < len; i++) {
            if (match(buf, i, pattern)) {
                return i + 1;
            }
        }
        return -1;
    }

    /*
     * Returns true if the chars array contains exactly the same elements from
     * start position to start + pattern.length as pattern. Otherwise returns
     * false.
     */
    private boolean match(char[] chars, int start, char[] pattern) {
        for (int i = 0; i < pattern.length;) {
            if (chars[start++] != pattern[i++]) {
                return false;
            }
        }
        return true;
    }

    public OutputStream setAsciiStream(long pos) throws SQLException {
        checkValidation();
        if (clob == null) { throw new IllegalStateException(); }
        OutputStream os = clob.setAsciiStream(pos);
        if (os == null) { throw new IllegalStateException(); }
        return os;
    }

    public Writer setCharacterStream(long pos) throws SQLException {
        checkValidation();
        if (clob == null) { throw new IllegalStateException(); }
        Writer writer = clob.setCharacterStream(pos);
        if (writer == null) { throw new IllegalStateException(); }
        return writer;
    }

    public int setString(long pos, String str) throws SQLException {
        checkValidation();
        return setString(pos, str, 0, str.length());
    }

    public int setString(long pos, String str, int offset, int length)
            throws SQLException {
        checkValidation();
        if (pos < 1)
        {
            throw HarmonySerialBlob.makeSQLException( SQLState.BLOB_BAD_POSITION, new Object[] {pos} );
        }
        if (length < 0)
        {
            throw HarmonySerialBlob.makeSQLException( SQLState.BLOB_NONPOSITIVE_LENGTH, null );
        }
        if (pos > (len - length + 1))
        {
            throw HarmonySerialBlob.makeSQLException( SQLState.BLOB_POSITION_TOO_LARGE, null );
        }
        if (offset < 0 || offset > (str.length() - length))
        {
            throw HarmonySerialBlob.makeSQLException( SQLState.BLOB_INVALID_OFFSET, null );
        }
        if (length > len + offset)
        {
            throw HarmonySerialBlob.makeSQLException( SQLState.BLOB_INVALID_OFFSET, null );
        }
        str.getChars(offset, offset + length, buf, (int) pos - 1);
        return length;
    }

    public void truncate(long length) throws SQLException {
        checkValidation();
        if (length < 0)
        {
            throw HarmonySerialBlob.makeSQLException( SQLState.BLOB_NONPOSITIVE_LENGTH, new Object[] {length} );
        }
        if (length > len)
        {
            throw HarmonySerialBlob.makeSQLException( SQLState.BLOB_LENGTH_TOO_LONG, new Object[] {length} );
        }
        char[] truncatedBuffer = new char[(int) length];
        System.arraycopy(buf, 0, truncatedBuffer, 0, (int) length);
        buf = truncatedBuffer;
        len = length;
    }

    public void free() throws SQLException {
        if (this.len != -1) {
            this.len = -1;
            this.clob = null;
            this.buf = null;
        }
    }

    public Reader getCharacterStream(long pos, long length) throws SQLException {
        checkValidation();
        return new CharArrayReader(buf, (int) pos, (int) length);
    }

    private void checkValidation() throws SQLException {
        if (len == -1)
        {
            throw HarmonySerialBlob.makeSQLException( SQLState.LOB_OBJECT_INVALID, null );
        }
    }
}
