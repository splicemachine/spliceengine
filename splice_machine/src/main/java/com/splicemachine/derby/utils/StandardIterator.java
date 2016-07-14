/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
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
