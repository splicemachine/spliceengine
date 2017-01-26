/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.derby.utils;

import com.splicemachine.db.iapi.error.StandardException;
import java.io.IOException;

/**
 * Iterator which is allowed to throw exceptions.
 *
 * @author Scott Fines
 * Created on: 11/1/13
 */
public interface StandardIterator<T> {

    /**
     * Open the iterator for operations. Should be called
     * before the first call to next();
     *
     * @throws StandardException
     * @throws IOException
     */
    void open() throws StandardException, IOException;

    /**
     * Get the next element in the interation
     * @return the next element in the iteration, or {@code null} if no
     * more elements exist
     *
     * @throws StandardException
     * @throws IOException
     */
    T next() throws StandardException,IOException;

    /**
     * Close the iterator. Should be called after the last
     * call to next(); once called, {@link #next()} should
     * no longer be called.
     *
     * @throws StandardException
     * @throws IOException
     */
    void close() throws StandardException,IOException;
}
