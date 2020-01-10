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

package com.splicemachine.tools;

/**
 * Represents a Poolable Resource.
 *
 * @author Scott Fines
 * Created on: 3/11/13
 */
public interface ResourcePool<E,K extends ResourcePool.Key>{

    /**
     * Unique reference Key for the pool
     */
    interface Key{

    }

    interface Generator<T,K extends Key>{

        T makeNew(K refKey) throws Exception;

        void close(T entity) throws Exception;
    }

    E get(K key) throws Exception;

    void release(K key) throws Exception;

}
