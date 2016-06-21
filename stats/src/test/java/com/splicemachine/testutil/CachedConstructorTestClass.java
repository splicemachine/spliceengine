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
