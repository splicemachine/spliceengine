/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
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

package com.splicemachine.stream;

/**
 * @author Scott Fines
 * Date: 8/13/14
 */
public interface PeekableStream<T> extends Stream<T> {

    T peek() throws StreamException;

    /**
     * Called when you want to remove the peeked element from the Stream. This means that
     *
     * {@code
     *  T n = stream.peek();
     *  stream.take();
     * }
     *
     * is functionally equivalent to calling
     * {@code
     * T n = stream.peek();
     * n = stream.next();
     * }
     * But the error handling is easier.
     */
    void take();
}
