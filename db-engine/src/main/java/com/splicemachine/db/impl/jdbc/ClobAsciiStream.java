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
package com.splicemachine.db.impl.jdbc;

import java.io.IOException;
import java.io.OutputStream;
import java.io.Writer;

/**
 * Wrap a Writer as an OutputStream to support Clob.setAsciiStream().
 * Any value written to the OutputStream is a valid ASCII value
 * (0-255 from JDBC 4 spec appendix C2) thus this class simply
 * passes the written values onto the Writer.
 *
 */
final class ClobAsciiStream extends OutputStream {

    private final Writer writer;
    private final char[] buffer = new char[1024];
    
    ClobAsciiStream (Writer writer){
        this.writer = writer;
    }

    /**
     * Writes the specified byte to this output stream.
     * <p>
     * The general contract for <code>write</code> is that one byte is written
     * to the output stream. The byte to be written is the eight low-order bits
     * of the argument <code>b</code>. The 24 high-order bits of <code>b</code>
     * are ignored.
     * 
     * @param b   the <code>byte</code>.
     * @exception IOException  if an I/O error occurs. In particular, 
     *             an <code>IOException</code> may be thrown if the 
     *             output stream has been closed.
     */
    public void write(int b) throws IOException {
        writer.write(b & 0xff);
    }

    /**
     * Writes <code>len</code> bytes from the specified byte array 
     * starting at offset <code>off</code> to this output stream. 
     * <p>
     * The general contract for <code>write(b, off, len)</code> is that 
     * some of the bytes in the array <code>b</code> are written to the 
     * output stream in order; element <code>b[off]</code> is the first 
     * byte written and <code>b[off+len-1]</code> is the last byte written 
     * by this operation.
     * <p>
     * The <code>write</code> method of <code>OutputStream</code> calls 
     * the write method of one argument on each of the bytes to be 
     * written out. Subclasses are encouraged to override this method and 
     * provide a more efficient implementation. 
     * <p>
     * If <code>b</code> is <code>null</code>, a 
     * <code>NullPointerException</code> is thrown.
     * <p>
     * If <code>off</code> is negative, or <code>len</code> is negative, or 
     * <code>off+len</code> is greater than the length of the array 
     * <code>b</code>, then an <tt>IndexOutOfBoundsException</tt> is thrown.
     * 
     * @param b     the data.
     * @param off   the start offset in the data.
     * @param len   the number of bytes to write.
     * @exception IOException  if an I/O error occurs. In particular, 
     *             an <code>IOException</code> is thrown if the output 
     *             stream is closed.
     */
    public void write(byte[] b, int off, int len) throws IOException {
        
        while (len > 0)
        {
            int clen = Math.min(len, buffer.length);
            for (int i = 0; i < clen; i++) {
                buffer[i] = (char)(b[off + i] & 0xff);
            }
            writer.write(buffer, 0, clen);
            off += clen;
            len -= clen;
        }
    }    
}
