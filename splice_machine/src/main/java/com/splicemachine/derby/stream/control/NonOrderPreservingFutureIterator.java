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

package com.splicemachine.derby.stream.control;

import java.util.Iterator;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Future;

/**
 *
 * Iterator over Futures
 *
 *
 */
public class NonOrderPreservingFutureIterator<T> implements Iterator<T> {

    private int numOfFutures;
    private ExecutorCompletionService<Iterator<T>> completionService;
    private volatile Iterator<T> current;


    public NonOrderPreservingFutureIterator(ExecutorCompletionService<Iterator<T>> cs, int numOfFutures) {
        this.completionService = cs;
        this.numOfFutures = numOfFutures;
    }

    @Override
    public boolean hasNext() {
        try {
            while (true) {
                if (current == null) {
                    if (numOfFutures == 0)
                        return false;
                    Future<Iterator<T>> future = completionService.take();
                    if (future == null)
                        return false;
                    else
                        current = future.get();
                    numOfFutures --;
                } else {
                    if (current.hasNext())
                        return true;
                    else
                        current = null; // cycle
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public T next() {
        return current.next();
    }

}




