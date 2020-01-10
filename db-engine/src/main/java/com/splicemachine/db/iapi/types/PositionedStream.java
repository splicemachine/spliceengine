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

import java.io.IOException;
import java.io.InputStream;
import com.splicemachine.db.iapi.error.StandardException;

/**
 * This interface describes a stream that is aware of its own position and can
 * reposition itself on request.
 * <p>
 * This interface doesn't convey any information about how expensive it is for
 * the stream to reposition itself.
 */
public interface PositionedStream {

    /**
     * Returns a reference to self as an {@code InputStream}.
     * <p>
     * This method is not allowed to return {@code null}.
     *
     * @return An {@code InputStream} reference to self.
     */
    InputStream asInputStream();

    /**
     * Returns the current byte position of the stream.
     *
     * @return Current byte position of the stream.
     */
    long getPosition();

    /**
     * Repositions the stream to the requested byte position.
     * <p>
     * If the repositioning fails because the stream is exhausted, most likely
     * because of an invalid position specified by the user, the stream is
     * reset to position zero and an {@code EOFException} is thrown.
     *
     * @param requestedPos requested byte position, first position is {@code 0}
     * @throws IOException if accessing the stream fails
     * @throws EOFException if the requested position is equal to or larger
     *      than the length of the stream
     * @throws StandardException if an error occurs in store
     */
    void reposition(long requestedPos)
            throws IOException, StandardException;
}
