/*
 * Copyright (c) 2012 - 2021 Splice Machine, Inc.
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

package com.splicemachine.derby.utils;

import com.splicemachine.db.iapi.services.classfile.VMOpcode;
import com.splicemachine.db.iapi.services.compiler.ClassBuilder;
import com.splicemachine.db.iapi.services.compiler.LocalField;
import com.splicemachine.db.iapi.services.compiler.MethodBuilder;
import com.splicemachine.db.iapi.services.loader.GeneratedClass;
import com.splicemachine.db.impl.services.bytecode.BCJava;
import com.splicemachine.derby.impl.services.reflect.SpliceReflectClasses;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.commons.lang.RandomStringUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.stream.Collectors;

public class CodeGenUnitTest {

    SpliceReflectClasses cf = new SpliceReflectClasses();
    BCJava b = new BCJava();

    ClassBuilder createClass(String parent) {
        String packageName = "Package_";
        String className = "Class_" + RandomStringUtils.randomAlphabetic(10);
        return b.newClassBuilder(cf, packageName, Modifier.PUBLIC, className, parent);
    }

    ClassBuilder createClassEmptyConstructor(String parent)
    {
        ClassBuilder cb = createClass(parent);
        MethodBuilder constructor = cb.newConstructorBuilder(Modifier.PUBLIC);
        constructor.callSuper();
        constructor.methodReturn();
        constructor.complete();
        return cb;
    }

    @Test
    public void testSimple() throws Exception {
        ClassBuilder cb = createClassEmptyConstructor(null);
        MethodBuilder mb = cb.newMethodBuilder(Modifier.PUBLIC, "int", "t1");
        mb.push(24);
        mb.methodReturn();
        mb.complete();

        GeneratedClass gc = cb.getGeneratedClass();
        Object o = gc.newInstance();

        Assert.assertEquals(getMethodList(cb.getFullName(), o),
                "public int t1()");

        Assert.assertEquals(gc.getMethod("t1").invoke(o), 24);

        checkDecompiled(cb,
                "public class %CLASSNAME% {\n" +
                "  public %CLASSNAME%();\n" +
                "    Code:\n" +
                "       0: aload_0\n" +
                "       1: invokespecial #8                  // Method java/lang/Object.\"<init>\":()V\n" +
                "       4: return\n" +
                "\n" +
                "  public int t1();\n" +
                "    Code:\n" +
                "       0: bipush        24\n" +
                "       2: ireturn\n" +
                "}\n");
    }

    @Test
    public void testVariables() throws Exception {
        String superclass = CSuperClassExample.class.getName();
        ClassBuilder cb = createClassEmptyConstructor(superclass);

        // create variable "Integer intVal;" in new class
        LocalField intVal = cb.addField(Integer.class.getName(), "intVal", Modifier.PUBLIC);

        // t1
        {
            // Integer t1() { return this.a; }
            MethodBuilder mb = cb.newMethodBuilder(Modifier.PUBLIC, Integer.class.getName(), "t1");
            mb.pushThis();
            mb.getField(superclass, "a", Integer.class.getName());
            mb.methodReturn();
            mb.complete();
        }

        // t2
        {
            // int t2() {
            //  CSuperClassExample.staticFunc(1, 2, 3);
            //  return 2; }
            MethodBuilder mb = cb.newMethodBuilder(Modifier.PUBLIC, "int", "t2");
            mb.pushThis();
            mb.push(1);
            mb.push(2);
            mb.push(3);
            mb.callMethod(VMOpcode.INVOKEVIRTUAL, CSuperClassExample.class.getName(), "staticFunc", "void", 3);
            mb.push(2);
            mb.methodReturn();
            mb.complete();
        }

        // t3
        {
            // int t3() {
            //  CSuperClassExample.staticFunc2(1, this.a, 3);
            //  return 2; }
            MethodBuilder mb = cb.newMethodBuilder(Modifier.PUBLIC, "int", "t3");
            mb.pushThis();
            mb.push(1);
            mb.pushThis();
            mb.getField(superclass, "a", Integer.class.getName());
            mb.push(3);
            mb.callMethod(VMOpcode.INVOKEVIRTUAL, CSuperClassExample.class.getName(), "staticFunc2", "void", 3);
            mb.push(2);
            mb.methodReturn();
            mb.complete();
        }

        // set_val
        {
            // void set_val() { this.intVal = this.a; }
            MethodBuilder mb = cb.newMethodBuilder(Modifier.PUBLIC, "void", "set_val");
            mb.pushThis();
            mb.getField(superclass, "a", Integer.class.getName());
            mb.setField(intVal);
            mb.methodReturn();
            mb.complete();
        }

        // get_val
        {
            // Integer get_val() { return this.intVal; }
            MethodBuilder mb = cb.newMethodBuilder(Modifier.PUBLIC, Integer.class.getName(), "get_val");
            mb.getField(intVal);
            mb.methodReturn();
            mb.complete();
        }


        GeneratedClass gc = cb.getGeneratedClass();
        Object o = gc.newInstance();
        CSuperClassExample b = (CSuperClassExample) o;

        Assert.assertEquals(getMethodList(cb.getFullName(), o),
                "public int t2()\n" +
                "public int t3()\n" +
                "public java.lang.Integer get_val()\n" +
                "public java.lang.Integer t1()\n" +
                "public void set_val()");

        b.a = 24;
        Assert.assertEquals(gc.getMethod("t1").invoke(o), 24);

        b.a = 32;
        Assert.assertEquals(gc.getMethod("t1").invoke(o), 32);

        Assert.assertEquals(gc.getMethod("t2").invoke(o), 2);
        Assert.assertEquals(b.a, Integer.valueOf(6) );

        b.a = 30;
        Assert.assertEquals(gc.getMethod("t3").invoke(o), 2);
        Assert.assertEquals(b.a, Integer.valueOf(34) );

        b.a = 123;
        gc.getMethod("set_val").invoke(o);
        Assert.assertEquals(gc.getMethod("get_val").invoke(o), 123);

        checkDecompiled(cb,
                "public class %CLASSNAME% extends com.splicemachine.derby.utils.CSuperClassExample {\n" +
                "  public java.lang.Integer intVal;\n" +
                "\n" +
                "  public %CLASSNAME%();\n" +
                "    Code:\n" +
                "       0: aload_0\n" +
                "       1: invokespecial #8                  // Method com/splicemachine/derby/utils/CSuperClassExample.\"<init>\":()V\n" +
                "       4: return\n" +
                "\n" +
                "  public java.lang.Integer t1();\n" +
                "    Code:\n" +
                "       0: aload_0\n" +
                "       1: getfield      #18                 // Field com/splicemachine/derby/utils/CSuperClassExample.a:Ljava/lang/Integer;\n" +
                "       4: areturn\n" +
                "\n" +
                "  public int t2();\n" +
                "    Code:\n" +
                "       0: aload_0\n" +
                "       1: iconst_1\n" +
                "       2: iconst_2\n" +
                "       3: iconst_3\n" +
                "       4: invokevirtual #24                 // Method com/splicemachine/derby/utils/CSuperClassExample.staticFunc:(III)V\n" +
                "       7: iconst_2\n" +
                "       8: ireturn\n" +
                "\n" +
                "  public int t3();\n" +
                "    Code:\n" +
                "       0: aload_0\n" +
                "       1: iconst_1\n" +
                "       2: aload_0\n" +
                "       3: getfield      #18                 // Field com/splicemachine/derby/utils/CSuperClassExample.a:Ljava/lang/Integer;\n" +
                "       6: iconst_3\n" +
                "       7: invokevirtual #29                 // Method com/splicemachine/derby/utils/CSuperClassExample.staticFunc2:(ILjava/lang/Integer;I)V\n" +
                "      10: iconst_2\n" +
                "      11: ireturn\n" +
                "\n" +
                "  public void set_val();\n" +
                "    Code:\n" +
                "       0: aload_0\n" +
                "       1: getfield      #18                 // Field com/splicemachine/derby/utils/CSuperClassExample.a:Ljava/lang/Integer;\n" +
                "       4: aload_0\n" +
                "       5: swap\n" +
                "       6: putfield      #13                 // Field intVal:Ljava/lang/Integer;\n" +
                "       9: return\n" +
                "\n" +
                "  public java.lang.Integer get_val();\n" +
                "    Code:\n" +
                "       0: aload_0\n" +
                "       1: getfield      #13                 // Field intVal:Ljava/lang/Integer;\n" +
                "       4: areturn\n" +
                "}\n");
    }


    @Test
    public void testIfElse() throws Exception {
        String superclass = CSuperClassExample.class.getName();
        ClassBuilder cb = createClassEmptyConstructor(superclass);

        // String t1() {
        //  if( (boolean) this.a.compareTo(this.intVal) )
        //      return "different"";
        //  else
        //      return "same";

        MethodBuilder mb = cb.newMethodBuilder(Modifier.PUBLIC, String.class.getName(), "t1");
        mb.pushThis();
        mb.getField(superclass, "a", Integer.class.getName());
        mb.pushThis();
        mb.getField(superclass, "b", Integer.class.getName());
        mb.callMethod(VMOpcode.INVOKEVIRTUAL, Integer.class.getName(), "compareTo", "int", 1);

        mb.cast("boolean");
        mb.conditionalIf();
            mb.push("different");
        mb.startElseCode();
            mb.push("same");
        mb.completeConditional();
        mb.methodReturn();
        mb.complete();
        // }

        GeneratedClass gc = cb.getGeneratedClass();
        Object o = gc.newInstance();
        CSuperClassExample b = (CSuperClassExample) o;

        Assert.assertEquals(getMethodList(cb.getFullName(), o),
                "public java.lang.String t1()");

        b.a = 24;
        b.b = 44;
        Assert.assertEquals(gc.invokeMethod(o, "t1"), "different");

        b.a = 34;
        b.b = 34;
        Assert.assertEquals(gc.invokeMethod(o, "t1"), "same");

        checkDecompiled(cb,
                "public class %CLASSNAME% extends com.splicemachine.derby.utils.CSuperClassExample {\n" +
                "  public %CLASSNAME%();\n" +
                "    Code:\n" +
                "       0: aload_0\n" +
                "       1: invokespecial #8                  // Method com/splicemachine/derby/utils/CSuperClassExample.\"<init>\":()V\n" +
                "       4: return\n" +
                "\n" +
                "  public java.lang.String t1();\n" +
                "    Code:\n" +
                "       0: aload_0\n" +
                "       1: getfield      #15                 // Field com/splicemachine/derby/utils/CSuperClassExample.a:Ljava/lang/Integer;\n" +
                "       4: aload_0\n" +
                "       5: getfield      #18                 // Field com/splicemachine/derby/utils/CSuperClassExample.b:Ljava/lang/Integer;\n" +
                "       8: invokevirtual #24                 // Method java/lang/Integer.compareTo:(Ljava/lang/Integer;)I\n" +
                "      11: ifeq          19\n" +
                "      14: ldc           #26                 // String different\n" +
                "      16: goto          21\n" +
                "      19: ldc           #28                 // String same\n" +
                "      21: areturn\n" +
                "}\n");
    }

    @Test
    public void testFunctionWithParameters() throws Exception {
        String superclass = CSuperClassExample.class.getName();
        ClassBuilder cb = createClassEmptyConstructor(superclass);

        // t1
        {
            // Integer t1(Integer x) { return x; }
            MethodBuilder mb = cb.newMethodBuilder(Modifier.PUBLIC, Integer.class.getName(), "t1", new String[]{Integer.class.getName()});
            mb.getParameter(0);
            mb.methodReturn();
            mb.complete();
        }

        // t2
        {
            // int t2(Integer x, Integer y) { return x.compareTo(y); }

            MethodBuilder mb = cb.newMethodBuilder(Modifier.PUBLIC, "int", "compare",
                    new String[]{Integer.class.getName(), Integer.class.getName()});

            mb.getParameter(0);
            mb.getParameter(1);
            mb.callMethod(VMOpcode.INVOKEVIRTUAL, Integer.class.getName(), "compareTo", "int", 1);
            mb.methodReturn();
            mb.complete();
        }

        GeneratedClass gc = cb.getGeneratedClass();
        Object o = gc.newInstance();

        Assert.assertEquals(getMethodList(cb.getFullName(), o),
                "public int compare(java.lang.Integer,java.lang.Integer)\n" +
                "public java.lang.Integer t1(java.lang.Integer)");

        Assert.assertEquals(gc.invokeMethod(o, "t1", 44), 44);

        Assert.assertEquals(gc.invokeMethod(o, "compare", 20, 22), -1);
        Assert.assertEquals(gc.invokeMethod(o, "compare", 20, 20), 0);
        Assert.assertEquals(gc.invokeMethod(o, "compare", 26, 22), 1);

        checkDecompiled(cb,
                "public class %CLASSNAME% extends com.splicemachine.derby.utils.CSuperClassExample {\n" +
                "  public %CLASSNAME%();\n" +
                "    Code:\n" +
                "       0: aload_0\n" +
                "       1: invokespecial #8                  // Method com/splicemachine/derby/utils/CSuperClassExample.\"<init>\":()V\n" +
                "       4: return\n" +
                "\n" +
                "  public java.lang.Integer t1(java.lang.Integer);\n" +
                "    Code:\n" +
                "       0: aload_1\n" +
                "       1: areturn\n" +
                "\n" +
                "  public int compare(java.lang.Integer, java.lang.Integer);\n" +
                "    Code:\n" +
                "       0: aload_1\n" +
                "       1: aload_2\n" +
                "       2: invokevirtual #19                 // Method java/lang/Integer.compareTo:(Ljava/lang/Integer;)I\n" +
                "       5: ireturn\n" +
                "}\n");
    }

    @Test
    public void testPrimitive() throws Exception {
        ClassBuilder cb = createClassEmptyConstructor(null);
        // t1
        {
            // int t1(int x) { return x; }
            MethodBuilder mb = cb.newMethodBuilder(Modifier.PUBLIC, int.class.getName(), "t1", new String[]{int.class.getName()});
            mb.getParameter(0);
            mb.methodReturn();
            mb.complete();
        }

        GeneratedClass gc = cb.getGeneratedClass();
        Object o = gc.newInstance();

        Assert.assertEquals(getMethodList(cb.getFullName(), o),
                "public int t1(int)");

        Assert.assertEquals(o.getClass().getMethod("t1", int.class).invoke(o, 23), 23);
    }

    @Test
    public void testArray() throws Exception {
        String superclass = CSuperClassExample.class.getName();
        ClassBuilder cb = createClassEmptyConstructor(superclass);

        // t1
        {
            // Integer getArr() { return this.sarr[2]; }
            MethodBuilder mb = cb.newMethodBuilder(Modifier.PUBLIC, Integer.class.getName(), "getArr");
            mb.pushThis();
            mb.getField(superclass, "sarr", Integer.class.getName() + "[]");
            mb.getArrayElement(2);
            mb.methodReturn();
            mb.complete();
        }

        // t2
        {
            // void setArr(Integer x) { this.sarr[2] = x; }
            MethodBuilder mb = cb.newMethodBuilder(Modifier.PUBLIC, "void", "setArr",
                    new String[]{Integer.class.getName()});

            mb.pushThis();
            mb.getField(superclass, "sarr", Integer.class.getName() + "[]");
            mb.getParameter(0);
            mb.setArrayElement(2);
            mb.methodReturn();
            mb.complete();
        }

        GeneratedClass gc = cb.getGeneratedClass();
        Object o = gc.newInstance();

        Assert.assertEquals(getMethodList(cb.getFullName(), o),
                "public java.lang.Integer getArr()\n" +
                "public void setArr(java.lang.Integer)");

        Assert.assertEquals(gc.invokeMethod(o, "getArr"), 20);
        gc.invokeMethod(o, "setArr", 22);
        Assert.assertEquals(gc.invokeMethod(o, "getArr"), 22);

        checkDecompiled(cb,
                "public class %CLASSNAME% extends com.splicemachine.derby.utils.CSuperClassExample {\n" +
                "  public %CLASSNAME%();\n" +
                "    Code:\n" +
                "       0: aload_0\n" +
                "       1: invokespecial #8                  // Method com/splicemachine/derby/utils/CSuperClassExample.\"<init>\":()V\n" +
                "       4: return\n" +
                "\n" +
                "  public java.lang.Integer getArr();\n" +
                "    Code:\n" +
                "       0: aload_0\n" +
                "       1: getfield      #15                 // Field com/splicemachine/derby/utils/CSuperClassExample.sarr:[Ljava/lang/Integer;\n" +
                "       4: iconst_2\n" +
                "       5: aaload\n" +
                "       6: areturn\n" +
                "\n" +
                "  public void setArr(java.lang.Integer);\n" +
                "    Code:\n" +
                "       0: aload_0\n" +
                "       1: getfield      #15                 // Field com/splicemachine/derby/utils/CSuperClassExample.sarr:[Ljava/lang/Integer;\n" +
                "       4: aload_1\n" +
                "       5: iconst_2\n" +
                "       6: swap\n" +
                "       7: aastore\n" +
                "       8: return\n" +
                "}\n");
    }

    public static void checkDecompiled(ClassBuilder cb, String expected) throws Exception {
        checkDecompiled(cb, expected, "-c");
    }

    @SuppressFBWarnings("RV_RETURN_VALUE_IGNORED_BAD_PRACTICE")
    public static void checkDecompiled(ClassBuilder cb, String expected, String options)
            throws Exception {
        try {
            execSysCommand("javap -version");
        } catch(Exception e) {
            System.out.println("WARNING: can't run javap.");
            return;
        }
        String path = "/tmp/";
        String filename = path + cb.getName() + ".class";
        try {
            cb.writeClassFile(path, false, null);
            String output = execSysCommand("javap " + options + " " + filename);
            output = output.replace(cb.getFullName(), "%CLASSNAME%");
            Assert.assertEquals(expected, output);
        } finally {
            //new File(filename).delete();
        }
    }

    public static String execSysCommand(String command) throws IOException, InterruptedException {
        Runtime r = Runtime.getRuntime();
        Process p = r.exec(command);
        p.waitFor();
        StringBuilder res = new StringBuilder();
        BufferedReader b = new BufferedReader(new InputStreamReader(p.getInputStream(), "UTF-8"));
        String line = "";

        while ((line = b.readLine()) != null) {
            res.append(line);
            res.append("\n");
        }
        b.close();
        return res.toString();
    }

    public static String getMethodList(String fullName, Object o) {

        return Arrays.stream(o.getClass().getMethods()).map(Method::toString).filter(
                s-> s.contains(fullName)).map( s -> s.replace(fullName+".", ""))
                .sorted()
                .collect(Collectors.joining("\n"));
    }
}
