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
package com.splicemachine.db.iapi.services.io;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

import com.splicemachine.db.iapi.reference.MessageId;
import com.splicemachine.db.iapi.services.i18n.MessageService;

/**
 * A stream that will throw an exception if its methods are invoked after it
 * has been closed.
 */
public class CloseFilterInputStream
        extends FilterInputStream {

    /** Message, modeled after CloseFilterInputStream in the client. */
    private static final String MESSAGE =
            MessageService.getTextMessage(MessageId.OBJECT_CLOSED); 
    
    /** Tells if this stream has been closed. */
    private boolean closed;

    public CloseFilterInputStream(InputStream in) {
        super(in);
    }

    public void close() throws IOException {
        closed = true;        
        super.close();
    }

    public int available() throws IOException {
        checkIfClosed();
        return super.available();
    }

    public int read() throws IOException {
        checkIfClosed();
        return super.read();
    }

    public int read(byte[] b) throws IOException {
        checkIfClosed();
        return super.read(b);
    }

    public int read(byte[] b, int off, int len) throws IOException {
        checkIfClosed();
        return super.read(b, off, len);
    }

    public long skip(long n) throws IOException {
        checkIfClosed();
        return super.skip(n);
    }
    
    /** Throws exception if this stream has been closed. */
    private void checkIfClosed() throws IOException {
        if (closed) {
            throw new IOException(MESSAGE);
        }
    }
}
