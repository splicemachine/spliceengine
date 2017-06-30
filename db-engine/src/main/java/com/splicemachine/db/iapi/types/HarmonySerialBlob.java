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

package com.splicemachine.db.iapi.types;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.sql.Blob;
import java.sql.SQLException;

import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.error.StandardException;

/**
 * Copied from the Harmony project's implementation of javax.sql.rowset.serial.SerialBlob
 * at subversion revision 946981.
 */
public class HarmonySerialBlob implements Blob, Serializable, Cloneable {

    private static final long serialVersionUID = -8144641928112860441L;

    // required by serialized form
    private byte[] buf;

    // required by serialized form
    private Blob blob;

    // required by serialized form
    private long len;

    // required by serialized form
    private long origLen;

    /**
     * Constructs an instance by the given <code>blob</code>
     * 
     * @param blob
     *            the given blob
     * @throws SQLException
     *             if an error is encountered during serialization
     * @throws SQLException
     *             if <code>blob</code> is null
     */
    public HarmonySerialBlob(Blob blob) throws SQLException {
        if (blob == null) { throw new IllegalArgumentException(); }
        
        this.blob = blob;
        buf = blob.getBytes(1, (int) blob.length());
        len = buf.length;
        origLen = len;
    }

    /**
     * Constructs an instance by the given <code>buf</code>
     * 
     * @param buf
     *            the given buffer
     * @throws SQLException
     *             if an error is encountered during serialization
     * @throws SQLException
     *             if a SQL error is encountered
     */
    public HarmonySerialBlob(byte[] buf) throws SQLException {
        this.buf = new byte[buf.length];
        len = buf.length;
        origLen = len;
        System.arraycopy(buf, 0, this.buf, 0, (int) len);
    }

    /**
     * Returns an input stream of this SerialObject.
     * 
     * @throws SQLException
     *             if an error is encountered
     */
    public InputStream getBinaryStream() throws SQLException {
        return new ByteArrayInputStream(buf);
    }

    /**
     * Returns a copied array of this SerialObject, starting at the
     * <code> pos </code> with the given <code> length</code> number. If
     * <code> pos </code> + <code> length </code> - 1 is larger than the length
     * of this SerialObject array, the <code> length </code> will be shortened
     * to the length of array - <code>pos</code> + 1.
     * 
     * @param pos
     *            the starting position of the array to be copied.
     * @param length
     *            the total length of bytes to be copied
     * @throws SQLException
     *             if an error is encountered
     */
    public byte[] getBytes(long pos, int length) throws SQLException {

        if (pos < 1 || pos > len)
        {
            throw makeSQLException( SQLState.BLOB_BAD_POSITION, new Object[] {pos} );
        }
        if (length < 0)
        {
            throw makeSQLException( SQLState.BLOB_NONPOSITIVE_LENGTH, new Object[] {length} );
        }

        if (length > len - pos + 1) {
            length = (int) (len - pos + 1);
        }
        byte[] copiedArray = new byte[length];
        System.arraycopy(buf, (int) pos - 1, copiedArray, 0, length);
        return copiedArray;
    }

    /**
     * Gets the number of bytes in this SerialBlob object.
     * 
     * @return an long value with the length of the SerialBlob in bytes
     * @throws SQLException
     *             if an error is encoutnered
     */
    public long length() throws SQLException {
        return len;
    }

    /**
     * Search for the position in this Blob at which a specified pattern begins,
     * starting at a specified position within the Blob.
     * 
     * @param pattern
     *            a Blob containing the pattern of data to search for in this
     *            Blob
     * @param start
     *            the position within this Blob to start the search, where the
     *            first position in the Blob is 1
     * @return a long value with the position at which the pattern begins. -1 if
     *         the pattern is not found in this Blob.
     * @throws SQLException
     *             if an error occurs accessing the Blob
     * @throws SQLException
     *             if an error is encountered
     */
    public long position(Blob pattern, long start) throws SQLException {
        byte[] patternBytes = pattern.getBytes(1, (int) pattern.length());
        return position(patternBytes, start);
    }

    /**
     * Search for the position in this Blob at which the specified pattern
     * begins, starting at a specified position within the Blob.
     * 
     * @param pattern
     *            a byte array containing the pattern of data to search for in
     *            this Blob
     * @param start
     *            the position within this Blob to start the search, where the
     *            first position in the Blob is 1
     * @return a long value with the position at which the pattern begins. -1 if
     *         the pattern is not found in this Blob.
     * @throws SQLException
     *             if an error is encountered
     * @throws SQLException
     *             if an error occurs accessing the Blob
     */
    public long position(byte[] pattern, long start) throws SQLException {
        if (start < 1 || len - (start - 1) < pattern.length) {
            return -1;
        }

        for (int i = (int) (start - 1); i <= (len - pattern.length); ++i) {
            if (match(buf, i, pattern)) {
                return i + 1;
            }
        }
        return -1;
    }

    /*
     * Returns true if the bytes array contains exactly the same elements from
     * start position to start + subBytes.length as subBytes. Otherwise returns
     * false.
     */
    private boolean match(byte[] bytes, int start, byte[] subBytes) {
        for (int i = 0; i < subBytes.length;) {
            if (bytes[start++] != subBytes[i++]) {
                return false;
            }
        }
        return true;
    }

    public OutputStream setBinaryStream(long pos) throws SQLException {
        if (blob == null) { throw new IllegalStateException(); }
        OutputStream os = blob.setBinaryStream(pos);
        if (os == null) { throw new IllegalStateException(); }
        return os;
    }

    public int setBytes(long pos, byte[] theBytes) throws SQLException {
        return setBytes(pos, theBytes, 0, theBytes.length);
    }

    public int setBytes(long pos, byte[] theBytes, int offset, int length)
            throws SQLException {
        if (pos < 1 || length < 0 || pos > (len - length + 1))
        {
            throw makeSQLException( SQLState.BLOB_BAD_POSITION, new Object[] {pos} );
        }
        if (offset < 0 || length < 0 || offset > (theBytes.length - length))
        {
            throw makeSQLException( SQLState.BLOB_INVALID_OFFSET, new Object[] {offset} );
        }
        System.arraycopy(theBytes, offset, buf, (int) pos - 1, length);
        return length;
    }

    public void truncate(long length) throws SQLException {
        if (length > this.len)
        {
            throw makeSQLException( SQLState.BLOB_LENGTH_TOO_LONG, new Object[] {len} );
        }
        buf = getBytes(1, (int) length);
        len = length;
    }

    public void free() throws SQLException {
        throw new UnsupportedOperationException("Not supported");
    }

    public InputStream getBinaryStream(long pos, long length)
            throws SQLException {
        if (len < 0)
        {
            throw makeSQLException( SQLState.BLOB_NONPOSITIVE_LENGTH, new Object[] {len} );
        }
        if (length < 0)
        {
            throw makeSQLException( SQLState.BLOB_NONPOSITIVE_LENGTH, new Object[] {length} );
        }
        if (pos < 1 || pos + length > len)
        {
            throw makeSQLException( SQLState.POS_AND_LENGTH_GREATER_THAN_LOB, new Object[] {pos, length} );
        }
        return new ByteArrayInputStream(buf, (int) (pos - 1), (int) length);
    }

    /**
     * Create a SQLException from Derby message arguments.
     */
    public static SQLException makeSQLException( String messageID, Object[] args )
    {
        StandardException se = StandardException.newException( messageID, args );

        return new SQLException( se.getMessage(), se.getSQLState() );
    }
}
