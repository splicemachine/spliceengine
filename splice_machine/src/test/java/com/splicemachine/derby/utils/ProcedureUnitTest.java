package com.splicemachine.derby.utils;

import com.splicemachine.db.catalog.types.RoutineAliasInfo;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.GlobalDBProperties;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;
import com.splicemachine.db.impl.sql.catalog.DefaultSystemProcedureGenerator;
import com.splicemachine.db.impl.sql.catalog.Procedure;
import com.splicemachine.derby.impl.sql.catalog.SpliceSystemProcedures;
import com.splicemachine.derby.procedures.SpliceAdmin;
import com.splicemachine.derby.procedures.StatisticsProcedures;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

public class ProcedureUnitTest {
    final static String CLASS_NAME = ProcedureUnitTest.class.getCanonicalName();
    @Test
    public void testAddProcNoParam() throws Exception {
        Procedure.newBuilder().name("SYSCS_GET_VERSION_INFO")
                .numOutputParams(0)
                .numResultSets(1)
                .ownerClass(SpliceAdmin.class.getCanonicalName())
                .buildCheck();
    }

    @Test
    public void testAddProcParamResultSet() throws Exception {
        Procedure.newBuilder().name("COLLECT_TABLE_STATISTICS")
                .numOutputParams(0)
                .numResultSets(1)
                .varchar("schema",128)
                .varchar("table",1024)
                .arg("staleOnly", DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BOOLEAN).getCatalogType())
                .ownerClass(StatisticsProcedures.class.getCanonicalName())
                .buildCheck();
    }

    @Test
    public void testAddProcParamNoResult() throws Exception {
        Procedure.newBuilder().name("SYSCS_EMPTY_GLOBAL_STORED_STATEMENT_CACHE")
                .numOutputParams(0)
                .numResultSets(0)
                .ownerClass(SpliceAdmin.class.getCanonicalName())
                .buildCheck();
    }

    @Test
    public void testAddProcFailsNumParameters() throws Exception {
        try {
            Procedure.newBuilder().name("COLLECT_TABLE_STATISTICS")
                    .numOutputParams(0)
                    .numResultSets(1)
                    .varchar("schema", 128)
                    .ownerClass(StatisticsProcedures.class.getCanonicalName())
                    .buildCheck();
            Assert.fail();
        }catch(Exception e) {
            Assert.assertEquals("could not find correct function with signature for com.splicemachine.derby.procedures.StatisticsProcedures.COLLECT_TABLE_STATISTICS:\n" +
                            " public static void com.splicemachine.derby.procedures.StatisticsProcedures.COLLECT_TABLE_STATISTICS(java.lang.String,java.lang.String,boolean,java.sql.ResultSet[]) throws java.sql.SQLException:\n" +
                            "  parameter count doesn't match: expected 2, but actual 4\n",
                    e.getMessage());
        }
    }

    @Test
    public void testAddProcFailsParameterTypes() throws Exception {
        try {
            Procedure.newBuilder().name("COLLECT_TABLE_STATISTICS")
                    .numOutputParams(0)
                    .numResultSets(1)
                    .varchar("schema", 128)
                    .integer("WRONG_TYPE")
                    .arg("staleOnly", DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BOOLEAN).getCatalogType())
                    .ownerClass(StatisticsProcedures.class.getCanonicalName())
                    .buildCheck();
            Assert.fail();
        }catch(Exception e) {
            Assert.assertEquals("could not find correct function with signature for com.splicemachine.derby.procedures.StatisticsProcedures.COLLECT_TABLE_STATISTICS:\n" +
                            " public static void com.splicemachine.derby.procedures.StatisticsProcedures.COLLECT_TABLE_STATISTICS(java.lang.String,java.lang.String,boolean,java.sql.ResultSet[]) throws java.sql.SQLException:\n" +
                            "  parameter 1 has wrong type: expected type is INTEGER, but actual type is VARCHAR\n",
                    e.getMessage());
        }
    }
    public void toRegisterFunc(String s) {
    }


    @Test
    public void testAddProcParamResultSizeWrong() throws Exception {
        try {
            Procedure.newBuilder().name("toRegisterFunc")
                    .numOutputParams(0)
                    .numResultSets(1)
                    .ownerClass(CLASS_NAME)
                    .buildCheck();
        }catch(Exception e) {
            Assert.assertEquals("could not find correct function with signature for com.splicemachine.derby.utils.ProcedureUnitTest.toRegisterFunc:\n" +
                            " public void com.splicemachine.derby.utils.ProcedureUnitTest.toRegisterFunc(java.lang.String):\n" +
                            "  parameter 0 needs to be java.sql.ResultSet, but is java.lang.String\n",
                    e.getMessage());
        }
        Procedure.newBuilder().name("toRegisterFunc")
                .numOutputParams(0)
                .numResultSets(0)
                .varchar("s", 10)
                .ownerClass(CLASS_NAME)
                .buildCheck();
    }

    @Test
    public void testClassNameWrong() throws Exception {
        try {
            Procedure.newBuilder().name("toRegisterFunc")
                    .numOutputParams(0)
                    .numResultSets(1)
                    .ownerClass(CLASS_NAME + "_WRONG")
                    .buildCheck();
        }catch(Exception e) {
            Assert.assertEquals("java.lang.ClassNotFoundException: " + CLASS_NAME + "_WRONG",
                    e.toString());
        }
    }

    @Test
    public void testFunctionNameWrong() throws Exception {
        try {
            Procedure.newBuilder().name("notExistingMethod")
                    .numOutputParams(0)
                    .numResultSets(1)
                    .ownerClass(CLASS_NAME)
                    .buildCheck();
        }catch(Exception e) {
            Assert.assertEquals("couldn't find function with name " + CLASS_NAME + ".notExistingMethod",
                    e.getMessage());
        }
    }

    void testProcedures(List<Procedure> procedures)
    {
        for( Procedure p : procedures) {
            try {
                p.check();
            } catch (Exception e) {
                Assert.fail("error :" + e.toString());
            }
        }
    }

    @Test
    public void testAllSysUtilProcedures()
    {
        List<Procedure> procedures = new ArrayList<>();
        SpliceSystemProcedures.createSysUtilProcedures(procedures);
        testProcedures(procedures);
    }

    @Test
    public void testSYSFUN_PROCEDURES()
    {
        List<Procedure> procedures = new ArrayList<>();
        SpliceSystemProcedures.getSYSFUN_PROCEDURES(procedures);
        testProcedures(procedures);
    }

    @Test
    public void testSYSIBM() throws StandardException {
        testProcedures(DefaultSystemProcedureGenerator.getSYSIBMProcedures());
    }

    @Test
    public void testSQL() throws StandardException {
        testProcedures(SpliceSystemProcedures.getSQLProcedures());
    }

    @Test
    public void testSYSCSM() throws StandardException {
        testProcedures(SpliceSystemProcedures.getSYSCSMProcedures());
    }

    short getRoutineControl(List<Procedure> procedures, String name)
    {
        return procedures.stream().filter(p -> p.getName().equals(name)).findAny().get().getRoutineSqlControl();
    }

    @Test
    public void testAllSysUtilProceduresExecutable()
    {
        List<Procedure> p = new ArrayList<>();
        SpliceSystemProcedures.createSysUtilProcedures(p);
        Assert.assertEquals(getRoutineControl(p, "SYSCS_GET_REGION_SERVER_CONFIG_INFO"), RoutineAliasInfo.NO_SQL);
        Assert.assertEquals(getRoutineControl(p, "SYSCS_SET_LOGGER_LEVEL"),              RoutineAliasInfo.NO_SQL);
        Assert.assertEquals(getRoutineControl(p, "SYSCS_SET_LOGGER_LEVEL_LOCAL"),        RoutineAliasInfo.NO_SQL);
        Assert.assertEquals(getRoutineControl(p, "SYSCS_GET_GLOBAL_DATABASE_PROPERTY"),  RoutineAliasInfo.READS_SQL_DATA);
        Assert.assertEquals(getRoutineControl(p, "SYSCS_SET_GLOBAL_DATABASE_PROPERTY"),  RoutineAliasInfo.MODIFIES_SQL_DATA);
        Assert.assertEquals(getRoutineControl(p, "SYSCS_RESTORE_DATABASE_OWNER"),        RoutineAliasInfo.NO_SQL);
        Assert.assertEquals(getRoutineControl(p, "SYSCS_SET_GLOBAL_DATABASE_PROPERTY"),  RoutineAliasInfo.MODIFIES_SQL_DATA);
        testProcedures(p);
    }

    @Test
    public void testCheckSpliceSystemProcedures() {
        List<Procedure> proc = new ArrayList<>();
        SpliceSystemProcedures.createSysUtilProcedures(proc);
        // note: this value changes if you add new system procedures
        // this is here to help in refactoring methods, move them around
        // and be sure that there's still the same procedures afterwards
        Assert.assertEquals(158, proc.stream().count());
        Assert.assertEquals(-2146922843, proc.stream().map( procedure -> procedure.getName() ).sorted()
                .map( s -> s.hashCode()).reduce(0, (subtotal, element) -> subtotal + element).longValue() );
    }

    @Test
    public void testValidateGlobalOptions() {
        Assert.assertEquals(
                "Error parsing '2' for option splice.function.preserveLineEndings: " +
                        "java.lang.RuntimeException: Expected either TRUE or FALSE.",
                GlobalDBProperties.PRESERVE_LINE_ENDINGS.validate("2") );

        Assert.assertEquals("", GlobalDBProperties.SPLICE_TIMESTAMP_FORMAT.validate("hh:mm:ss a") );
        Assert.assertEquals("Error parsing 'h:mm:ss a' for option splice.function.timestampFormat: " +
                        "java.lang.IllegalArgumentException: not supported format \"h:mm:ss a\": 'h' can't be repeated 1 times",
                GlobalDBProperties.SPLICE_TIMESTAMP_FORMAT.validate("h:mm:ss a") );

        Assert.assertEquals("", GlobalDBProperties.SPLICE_CURRENT_TIMESTAMP_PRECISION.validate("1234") );
        Assert.assertEquals("Error parsing 'abcd' for option splice.function.currentTimestampPrecision: " +
                        "java.lang.NumberFormatException: For input string: \"abcd\"",
                GlobalDBProperties.SPLICE_CURRENT_TIMESTAMP_PRECISION.validate("abcd") );

        Assert.assertEquals("", GlobalDBProperties.FLOATING_POINT_NOTATION.validate("plain") );
        Assert.assertEquals("Error parsing 'abcd' for option splice.function.floatingPointNotation: " +
                        "java.lang.RuntimeException: Supported values are [plain, normalized, default].",
                GlobalDBProperties.FLOATING_POINT_NOTATION.validate("abcd") );


    }
}
