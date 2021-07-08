package com.splicemachine.derby.utils;

import com.splicemachine.db.iapi.services.compiler.ClassBuilder;
import com.splicemachine.db.iapi.services.compiler.JavaFactory;
import com.splicemachine.db.iapi.services.loader.ClassFactory;

import com.splicemachine.db.impl.services.bytecode.BCJava;
import com.splicemachine.db.impl.sql.compile.CompilerContextImpl;
import com.splicemachine.derby.impl.services.reflect.SpliceReflectClasses;
import org.apache.commons.lang.RandomStringUtils;

class MyCompilerContext extends CompilerContextImpl {

    public MyCompilerContext() {
        super();
    }

    public static class MyJavaFactory implements JavaFactory {
        BCJava b = new BCJava();
        @Override
        public ClassBuilder newClassBuilder(ClassFactory cf, String packageName,
                                            int modifiers, String className, String superClass) {

            return b.newClassBuilder(cf, packageName, modifiers, className, superClass);
        }
    }

    MyJavaFactory javaFactory = new MyJavaFactory();
    SpliceReflectClasses classFactory = new SpliceReflectClasses();

    @Override
    public ClassFactory getClassFactory() {
        return classFactory;
    }

    @Override
    public JavaFactory getJavaFactory() {
        return javaFactory;
    }

    @Override
    public String getUniqueClassName() {
        return "Class_" + RandomStringUtils.randomAlphabetic(10);
    }
}
