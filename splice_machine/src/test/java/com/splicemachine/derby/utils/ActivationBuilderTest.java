package com.splicemachine.derby.utils;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.ClassName;
import com.splicemachine.db.iapi.services.compiler.MethodBuilder;
import com.splicemachine.db.iapi.services.loader.GeneratedClass;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.compile.*;
import com.splicemachine.db.impl.sql.compile.ActivationClassBuilder;
import com.splicemachine.db.impl.sql.execute.CurrentDatetime;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Modifier;
import java.util.Date;

public class ActivationBuilderTest {

    Activation generateAndConstruct(ActivationClassBuilder acb) throws Exception {
        // try to instantiate an object of the generated code
        GeneratedClass gc = acb.getClassBuilder().getGeneratedClass();
        Object o = gc.newInstance();
        // postConstructor will initialize all objects, so needs to be callable
        gc.invokeMethod(o, "postConstructor");
        return (Activation) o;
    }

    @Test
    public void testSimpleActivation() throws Exception {
        CompilerContext cc = new MyCompilerContext();
        // start codegen
        ActivationClassBuilder acb = new ActivationClassBuilder(ClassName.CursorActivation, cc);

        acb.setDataSetProcessorType(DataSetProcessorType.DEFAULT_OLTP);
        acb.setSparkExecutionType(SparkExecutionType.UNSPECIFIED);

        createSimpleFillResult(acb);
        finishAcb(acb);
        // finish codegen

        Activation o = generateAndConstruct(acb);

        Assert.assertEquals(CodeGenUnitTest.getMethodList(acb.getClassBuilder().getFullName(), o),
                "public com.splicemachine.db.iapi.sql.ResultSet execute() throws com.splicemachine.db.iapi.error.StandardException\n" +
                "public com.splicemachine.db.iapi.sql.ResultSet fillResultSet() throws com.splicemachine.db.iapi.error.StandardException\n" +
                "public java.util.Vector getSubqueryResultSets() throws com.splicemachine.db.iapi.error.StandardException\n" +
                "public void materialize() throws com.splicemachine.db.iapi.error.StandardException\n" +
                "public void postConstructor() throws com.splicemachine.db.iapi.error.StandardException");


        // check interface of new class
        CodeGenUnitTest.checkDecompiled(acb.getClassBuilder(),
                "public final class %CLASSNAME% extends com.splicemachine.db.impl.sql.execute.CursorActivation {\n" +
                "  public %CLASSNAME%();\n" +
                "  public void postConstructor() throws com.splicemachine.db.iapi.error.StandardException;\n" +
                "  public com.splicemachine.db.iapi.sql.ResultSet execute() throws com.splicemachine.db.iapi.error.StandardException;\n" +
                "  public void materialize() throws com.splicemachine.db.iapi.error.StandardException;\n" +
                "  public java.util.Vector getSubqueryResultSets() throws com.splicemachine.db.iapi.error.StandardException;\n" +
                "  public com.splicemachine.db.iapi.sql.ResultSet fillResultSet() throws com.splicemachine.db.iapi.error.StandardException;\n" +
                "  protected final int getExecutionCount();\n" +
                "  protected final void setExecutionCount(int);\n" +
                "  protected final java.util.Vector getRowCountCheckVector();\n" +
                "  protected final void setRowCountCheckVector(java.util.Vector);\n" +
                "  protected final int getStalePlanCheckInterval();\n" +
                "  protected final void setStalePlanCheckInterval(int);\n" +
                "}\n", "");

        // check actual code (this is a bit cement)
        CodeGenUnitTest.checkDecompiled(acb.getClassBuilder(),
                "public final class %CLASSNAME% extends com.splicemachine.db.impl.sql.execute.CursorActivation {\n" +
                "  public %CLASSNAME%();\n" +
                "    Code:\n" +
                "       0: aload_0\n" +
                "       1: invokespecial #8                  // Method com/splicemachine/db/impl/sql/execute/CursorActivation.\"<init>\":()V\n" +
                "       4: return\n" +
                "\n" +
                "  public void postConstructor() throws com.splicemachine.db.iapi.error.StandardException;\n" +
                "    Code:\n" +
                "       0: aload_0\n" +
                "       1: iconst_0\n" +
                "       2: dup_x1\n" +
                "       3: putfield      #34                 // Field com/splicemachine/db/impl/sql/execute/BaseActivation.datasetProcessorType:I\n" +
                "       6: pop\n" +
                "       7: aload_0\n" +
                "       8: iconst_0\n" +
                "       9: dup_x1\n" +
                "      10: putfield      #37                 // Field com/splicemachine/db/impl/sql/execute/BaseActivation.sparkExecutionType:I\n" +
                "      13: pop\n" +
                "      14: return\n" +
                "\n" +
                "  public com.splicemachine.db.iapi.sql.ResultSet execute() throws com.splicemachine.db.iapi.error.StandardException;\n" +
                "    Code:\n" +
                "       0: aload_0\n" +
                "       1: ldc           #13                 // String execute\n" +
                "       3: invokevirtual #19                 // Method com/splicemachine/db/impl/sql/execute/BaseActivation.throwIfClosed:(Ljava/lang/String;)V\n" +
                "       6: aload_0\n" +
                "       7: invokevirtual #22                 // Method com/splicemachine/db/impl/sql/execute/BaseActivation.startExecution:()V\n" +
                "      10: aload_0\n" +
                "      11: getfield      #45                 // Field com/splicemachine/db/impl/sql/execute/BaseActivation.resultSet:Lcom/splicemachine/db/iapi/sql/ResultSet;\n" +
                "      14: ifnonnull     30\n" +
                "      17: aload_0\n" +
                "      18: invokevirtual #47                 // Method fillResultSet:()Lcom/splicemachine/db/iapi/sql/ResultSet;\n" +
                "      21: aload_0\n" +
                "      22: swap\n" +
                "      23: dup_x1\n" +
                "      24: putfield      #45                 // Field com/splicemachine/db/impl/sql/execute/BaseActivation.resultSet:Lcom/splicemachine/db/iapi/sql/ResultSet;\n" +
                "      27: goto          34\n" +
                "      30: aload_0\n" +
                "      31: getfield      #45                 // Field com/splicemachine/db/impl/sql/execute/BaseActivation.resultSet:Lcom/splicemachine/db/iapi/sql/ResultSet;\n" +
                "      34: areturn\n" +
                "\n" +
                "  public void materialize() throws com.splicemachine.db.iapi.error.StandardException;\n" +
                "    Code:\n" +
                "       0: aload_0\n" +
                "       1: ldc           #24                 // String materialize\n" +
                "       3: invokevirtual #19                 // Method com/splicemachine/db/impl/sql/execute/BaseActivation.throwIfClosed:(Ljava/lang/String;)V\n" +
                "       6: return\n" +
                "\n" +
                "  public java.util.Vector getSubqueryResultSets() throws com.splicemachine.db.iapi.error.StandardException;\n" +
                "    Code:\n" +
                "       0: aload_0\n" +
                "       1: ldc           #27                 // String getSubqueryResultSets\n" +
                "       3: invokevirtual #19                 // Method com/splicemachine/db/impl/sql/execute/BaseActivation.throwIfClosed:(Ljava/lang/String;)V\n" +
                "       6: new           #29                 // class java/util/Vector\n" +
                "       9: dup\n" +
                "      10: invokespecial #30                 // Method java/util/Vector.\"<init>\":()V\n" +
                "      13: areturn\n" +
                "\n" +
                "  public com.splicemachine.db.iapi.sql.ResultSet fillResultSet() throws com.splicemachine.db.iapi.error.StandardException;\n" +
                "    Code:\n" +
                "       0: aconst_null\n" +
                "       1: areturn\n" +
                "\n" +
                "  protected final int getExecutionCount();\n" +
                "    Code:\n" +
                "       0: getstatic     #50                 // Field getExecutionCount:I\n" +
                "       3: ireturn\n" +
                "\n" +
                "  protected final void setExecutionCount(int);\n" +
                "    Code:\n" +
                "       0: iload_1\n" +
                "       1: putstatic     #50                 // Field getExecutionCount:I\n" +
                "       4: return\n" +
                "\n" +
                "  protected final java.util.Vector getRowCountCheckVector();\n" +
                "    Code:\n" +
                "       0: getstatic     #57                 // Field getRowCountCheckVector:Ljava/util/Vector;\n" +
                "       3: areturn\n" +
                "\n" +
                "  protected final void setRowCountCheckVector(java.util.Vector);\n" +
                "    Code:\n" +
                "       0: aload_1\n" +
                "       1: putstatic     #57                 // Field getRowCountCheckVector:Ljava/util/Vector;\n" +
                "       4: return\n" +
                "\n" +
                "  protected final int getStalePlanCheckInterval();\n" +
                "    Code:\n" +
                "       0: getstatic     #62                 // Field getStalePlanCheckInterval:I\n" +
                "       3: ireturn\n" +
                "\n" +
                "  protected final void setStalePlanCheckInterval(int);\n" +
                "    Code:\n" +
                "       0: iload_1\n" +
                "       1: putstatic     #62                 // Field getStalePlanCheckInterval:I\n" +
                "       4: return\n" +
                "}\n");
    }

    @Test
    public void testCurrentDate() throws Exception {
        CompilerContext cc = new MyCompilerContext();
        ActivationClassBuilder acb = new ActivationClassBuilder(ClassName.CursorActivation, cc);

        createSimpleFillResult(acb);

        MethodBuilder mb = acb.getClassBuilder().newMethodBuilder(Modifier.PUBLIC, "java.sql.Date",
                "myFunc");
        acb.getCurrentDateExpression(mb);
        mb.methodReturn();
        mb.complete();

        finishAcb(acb);
        Activation o = generateAndConstruct(acb);

        Date d = (Date) o.getClass().getMethod("myFunc").invoke(o);
        if(d.compareTo(new CurrentDatetime().getCurrentDate() ) != 0)
        {
            // in the super rare case of the first call happening on a different date,
            // the next call has 24 hours to complete
            d = (Date) o.getClass().getMethod("myFunc").invoke(o);
            Assert.assertEquals(d, new CurrentDatetime().getCurrentDate());
        }
    }

    private void createSimpleFillResult(ActivationClassBuilder acb) {
        MethodBuilder mbWorker=acb.getClassBuilder().newMethodBuilder(
                Modifier.PUBLIC, // normally PRIVATE, but setting to PUBLIC so we see in generated code and it's not inlined
                ClassName.ResultSet,
                "fillResultSet");
        mbWorker.addThrownException(ClassName.StandardException);
        // normally generate(...)
        mbWorker.pushNull(ClassName.ResultSet);
        mbWorker.methodReturn();
        mbWorker.complete();
    }
    void finishAcb(ActivationClassBuilder acb) throws StandardException {
        acb.finishExecuteCode(false);
        acb.addFields();
        acb.finishActivationMethod();

        // wrap up the constructor by putting a return at the end of it
        acb.finishConstructor();
        acb.finishMaterializationMethod();
        acb.finishSubqueryResultSetMethod();
    }

}
