package com.splicemachine.derby.utils;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;
import com.splicemachine.db.impl.sql.catalog.DefaultSystemProcedureGenerator;
import com.splicemachine.db.impl.sql.catalog.Procedure;
import com.splicemachine.derby.impl.sql.catalog.SpliceSystemProcedures;
import com.splicemachine.derby.impl.storage.TableSplit;
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
                .ownerClass(StatisticsAdmin.class.getCanonicalName())
                .buildCheck();
    }

    @Test
    public void testAddProcParamNoResult() throws Exception {
        Procedure.newBuilder().name("testAddProcParamNoResult")
                .numOutputParams(0)
                .numResultSets(0)
                .ownerClass(ProcedureUnitTest.class.getCanonicalName())
                .buildCheck();
    }

    @Test
    public void testAddProcFailsNumParameters() throws Exception {
        try {
            Procedure.newBuilder().name("COLLECT_TABLE_STATISTICS")
                    .numOutputParams(0)
                    .numResultSets(1)
                    .varchar("schema", 128)
                    .ownerClass(StatisticsAdmin.class.getCanonicalName())
                    .buildCheck();
            Assert.fail();
        }catch(Exception e) {
            Assert.assertEquals("could not find correct function with signature for com.splicemachine.derby.utils.StatisticsAdmin.COLLECT_TABLE_STATISTICS:\n" +
                            " public static void com.splicemachine.derby.utils.StatisticsAdmin.COLLECT_TABLE_STATISTICS(java.lang.String,java.lang.String,boolean,java.sql.ResultSet[]) throws java.sql.SQLException:\n" +
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
                    .ownerClass(StatisticsAdmin.class.getCanonicalName())
                    .buildCheck();
            Assert.fail();
        }catch(Exception e) {
            Assert.assertEquals("could not find correct function with signature for com.splicemachine.derby.utils.StatisticsAdmin.COLLECT_TABLE_STATISTICS:\n" +
                            " public static void com.splicemachine.derby.utils.StatisticsAdmin.COLLECT_TABLE_STATISTICS(java.lang.String,java.lang.String,boolean,java.sql.ResultSet[]) throws java.sql.SQLException:\n" +
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

}
