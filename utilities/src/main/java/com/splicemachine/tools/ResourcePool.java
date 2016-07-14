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
    public interface Key{

    }

    public interface Generator<T,K extends Key>{

        T makeNew(K refKey) throws Exception;

        void close(T entity) throws Exception;
    }

    public E get(K key) throws Exception;

    public void release(K key) throws Exception;

}
