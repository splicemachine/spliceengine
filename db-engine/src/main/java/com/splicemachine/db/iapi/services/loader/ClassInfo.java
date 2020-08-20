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
package com.splicemachine.db.iapi.services.loader;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import splice.com.google.common.cache.CacheBuilder;
import splice.com.google.common.cache.CacheLoader;
import splice.com.google.common.cache.LoadingCache;

public class ClassInfo implements InstanceGetter {

    private static final Class[] NO_PARAMETERS = new Class[0];
    private static final Object[] NO_ARGUMENTS = new Object[0];
    private static final LoadingCache<Class, Constructor> CONSTRUCTOR_CACHE = CacheBuilder.newBuilder().concurrencyLevel(64).
    maximumSize(100).expireAfterAccess(5, TimeUnit.MINUTES).build(new ConstructorCacheLoader());

    private static class ConstructorCacheLoader extends CacheLoader<Class, Constructor> {

        @Override
        public Constructor load(Class clazz) throws Exception {
            return clazz.getConstructor(NO_PARAMETERS);
        }
    }

    private final Class clazz;
    private boolean useConstructor = true;
    private Constructor noArgConstructor;

    public ClassInfo(Class clazz) {
        this.clazz = clazz;
    }

    /**
     * Return the name of this class.
     */
    public final String getClassName() {
        return clazz.getName();
    }

    /**
     * Return the class object for this class.
     */
    public final Class getClassObject() {
        return clazz;
    }

    /**
     * Create an instance of this class. Assumes that clazz has already been
     * initialized. Optimizes Class.newInstance() by caching and using the
     * no-arg Constructor directly. Class.newInstance() looks up the constructor
     * each time.
     *
     * @throws InstantiationException    Zero arg constructor can not be executed
     * @throws IllegalAccessException    Class or zero arg constructor is not public.
     * @throws InvocationTargetException Exception throw in zero-arg constructor.
     */
    @Override
    public Object getNewInstance() throws InstantiationException, IllegalAccessException, InvocationTargetException {

        if (!useConstructor) {
            return clazz.newInstance();
        }

        if (noArgConstructor == null) {

            try {
                noArgConstructor = CONSTRUCTOR_CACHE.get(clazz);
            } catch (ExecutionException | SecurityException e) {
                // let Class.newInstance() generate the exception
                useConstructor = false;
                return clazz.newInstance();
            }
        }

        try {
            return noArgConstructor.newInstance(NO_ARGUMENTS);
        } catch (IllegalArgumentException iae) {
            // can't happen since no arguments are passed.
            return null;
        }
    }
}
