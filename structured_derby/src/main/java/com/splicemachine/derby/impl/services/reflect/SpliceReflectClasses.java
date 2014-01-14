package com.splicemachine.derby.impl.services.reflect;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.derby.iapi.util.ByteArray;
import org.apache.derby.impl.services.reflect.DatabaseClasses;
import org.apache.derby.impl.services.reflect.LoadedGeneratedClass;
import org.apache.derby.impl.services.reflect.ReflectGeneratedClass;
import org.apache.derby.impl.services.reflect.ReflectLoaderJava2;

import java.security.PrivilegedAction;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

/**
 * @author Scott Fines
 *         Created on: 10/28/13
 */
public class SpliceReflectClasses extends DatabaseClasses {
    private Cache<String,LoadedGeneratedClass> preCompiled;


    private final PrivilegedAction<ReflectLoaderJava2> reflectLoader = new PrivilegedAction<ReflectLoaderJava2>() {
        @Override
        public ReflectLoaderJava2 run() {
            return new ReflectLoaderJava2(SpliceReflectClasses.this.getClass().getClassLoader(),SpliceReflectClasses.this);
        }
    };

    private final PrivilegedAction<ClassLoader> threadLoader = new PrivilegedAction<ClassLoader>() {
        @Override
        public ClassLoader run() {
            return Thread.currentThread().getContextClassLoader();
        }
    };

    public SpliceReflectClasses() {
        super();

        preCompiled = CacheBuilder.newBuilder().maximumSize(1000).build();
    }


    @Override
    protected LoadedGeneratedClass loadGeneratedClassFromData(final String fullyQualifiedName, final ByteArray classDump){
        if (classDump == null || classDump.getArray() == null) {
            // not a generated class, just load the class directly.
            try {
                Class jvmClass = Class.forName(fullyQualifiedName);
                return new ReflectGeneratedClass(this, jvmClass, null);
            } catch (ClassNotFoundException cnfe) {
                throw new NoClassDefFoundError(cnfe.toString());
            }
        } else {
            try {
                return preCompiled.get(fullyQualifiedName,new Callable<LoadedGeneratedClass>() {
                    @Override
                    public LoadedGeneratedClass call() throws Exception {
                        return java.security.AccessController.doPrivileged(reflectLoader).loadGeneratedClass(fullyQualifiedName,classDump);
                    }
                });
            } catch (ExecutionException e) {
                //should never happen
                throw new RuntimeException(e.getCause());
            }
        }
    }

    @Override
    protected Class loadClassNotInDatabaseJar(String name) throws ClassNotFoundException {

        Class foundClass;

        // We may have two problems with calling  getContextClassLoader()
        // when trying to find our own classes for aggregates.
        // 1) If using the URLClassLoader a ClassNotFoundException may be
        //    thrown (Beetle 5002).
        // 2) If Derby is loaded with JNI, getContextClassLoader()
        //    may return null. (Beetle 5171)
        //
        // If this happens we need to user the class loader of this object
        // (the classLoader that loaded Derby).
        // So we call Class.forName to ensure that we find the class.
        try {
            ClassLoader cl= java.security.AccessController.doPrivileged(threadLoader);
            foundClass = (cl != null) ?  cl.loadClass(name)
                    :Class.forName(name);
        } catch (ClassNotFoundException cnfe) {
            foundClass = Class.forName(name);
        }
        return foundClass;
    }
}
