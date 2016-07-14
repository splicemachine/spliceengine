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

package com.splicemachine.testutil;

import org.junit.Assert;
import org.junit.runners.model.TestClass;

import java.lang.reflect.Constructor;

/**
 * @author Scott Fines
 *         Date: 7/8/15
 */
public class CachedConstructorTestClass extends TestClass{
    private final Constructor onlyConstructor;
    /**
     * Creates a {@code TestClass} wrapping {@code clazz}. Each time this
     * constructor executes, the class is scanned for annotations, which can be
     * an expensive process (we hope in future JDK's it will not be.) Therefore,
     * try to share instances of {@code TestClass} where possible.
     *
     * @param clazz
     */
    public CachedConstructorTestClass(Class<?> clazz){
        super(clazz);

        Constructor[] constructors = clazz.getConstructors();
        Assert.assertEquals(1,constructors.length);
        onlyConstructor = constructors[0];
    }

    @Override
    public Constructor<?> getOnlyConstructor(){
        return onlyConstructor;
    }
}
