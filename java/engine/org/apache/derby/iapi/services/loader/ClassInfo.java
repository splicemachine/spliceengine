/*
   Derby - Class org.apache.derby.iapi.services.loader.ClassInfo

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */
package org.apache.derby.iapi.services.loader;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.LinkedHashMap;
import java.util.Map.Entry;

public class ClassInfo implements InstanceGetter {

    private static final Class[] NO_PARAMETERS = new Class[0];
    private static final Object[] NO_ARGUMENTS = new Object[0];
    private static final NoArgumentConstructorMap<String, Constructor> CONSTRUCTOR_CACHE = new NoArgumentConstructorMap<String, Constructor>(100);

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
                noArgConstructor = getNoArgConstructor(clazz);
            } catch (NoSuchMethodException nsme) {
                // let Class.newInstance() generate the exception
                useConstructor = false;
                return clazz.newInstance();
            } catch (SecurityException se) {
                // not allowed to to get a handle on the constructor just use the standard mechanism.
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

    private static Constructor<?> getNoArgConstructor(Class<?> clazz) throws NoSuchMethodException {
        String cacheKey = clazz.getCanonicalName();
        synchronized (CONSTRUCTOR_CACHE) {
            if (!CONSTRUCTOR_CACHE.containsKey(cacheKey)) {
                CONSTRUCTOR_CACHE.put(cacheKey, clazz.getConstructor(NO_PARAMETERS));
            }
            return CONSTRUCTOR_CACHE.get(cacheKey);
        }
    }

    // LRU CACHE
    private static class NoArgumentConstructorMap<K, V> extends LinkedHashMap<K,V> {
        private int maxCapacity;

        public NoArgumentConstructorMap(int maxCapacity) {
            super(0, 0.75F, true);
            this.maxCapacity = maxCapacity;
        }

        @Override
        protected boolean removeEldestEntry(Entry eldest) {
            return size() >= this.maxCapacity;
        }
    }

}
