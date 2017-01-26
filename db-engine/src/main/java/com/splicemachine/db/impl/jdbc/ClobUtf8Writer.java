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
import java.io.Writer;
import java.sql.SQLException;
import com.splicemachine.db.iapi.reference.MessageId;
import com.splicemachine.db.iapi.services.i18n.MessageService;

/**
 * Writer implementation for <code>Clob</code>.
 */
final class ClobUtf8Writer extends Writer {
    private TemporaryClob control;    
    private long pos; // Position in characters.
    private boolean closed;
    
    /**
     * Constructor.
     *
     * @param control worker object for the CLOB value
     * @param pos initial <b>byte</b> position in the CLOB value
     */
    ClobUtf8Writer(TemporaryClob control, long pos) {
        this.control = control;
        this.pos = pos;
        closed = false;
    }    

    /**
     * Flushes the stream.
     * <p>
     * Flushing the stream after {@link #close} has been called will cause an
     * exception to be thrown.
     * <p>
     * <i>Implementation note:</i> In the current implementation, this is a
     * no-op. Flushing is left to the underlying stream(s). Note that when
     * programming against/with this class, always follow good practice and call
     * <code>flush</code>.
     *
     * @throws IOException if the stream has been closed
     */
    public void flush() throws IOException {
        if (closed)
            throw new IOException (
                MessageService.getTextMessage(MessageId.OBJECT_CLOSED));
        // A no-op.
        // Flushing is currently the responsibility of the underlying stream(s).
    }

    /**
     * Closes the stream.
     * <p>
     * Once the stream has been closed, further <code>write</code> or 
     * {@link #flush} invocations will cause an <code>IOException</code> to be
     * thrown. Closing a previously closed stream has no effect.
     */
    public void close() {
        closed = true;
    }

    /**
     * Writes a portion of an array of characters to the CLOB value.
     * 
     * @param cbuf array of characters
     * @param off offset into <code>cbuf</code> from which to start writing
     *      characters
     * @param len number of characters to write
     * @throws IOException if an I/O error occurs
     */
    public void write(char[] cbuf, int off, int len) throws IOException {
        if (closed)
            throw new IOException (
                MessageService.getTextMessage(MessageId.OBJECT_CLOSED));
        try {
            long ret = control.insertString (String.copyValueOf (
                                                    cbuf, off, len), 
                                              pos);
            if (ret > 0)
                pos += ret;
        }
        catch (SQLException e) {
            throw Util.newIOException(e);
        }
    }
}
