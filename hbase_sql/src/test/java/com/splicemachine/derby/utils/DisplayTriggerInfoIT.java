package com.splicemachine.derby.utils;

import com.splicemachine.db.iapi.tools.i18n.LocalizedInput;
import com.splicemachine.db.iapi.tools.i18n.LocalizedOutput;
import com.splicemachine.db.iapi.tools.i18n.LocalizedResource;
import com.splicemachine.db.impl.tools.ij.Main;
import com.splicemachine.db.impl.tools.ij.ijCommands;
import com.splicemachine.derby.test.framework.SpliceNetConnection;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.test.SerialTest;
import org.apache.commons.io.IOUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;

public class DisplayTriggerInfoIT implements SerialTest {
    static final LocalizedResource langUtil = LocalizedResource.getInstance();
    static final String zeroRowsUpdated = "0 rows inserted/updated/deleted\n";
    static Main me;
    static String SCHEMA_NAME = "DisplayTriggerInfoIT".toUpperCase();
    static File tempDir;
    static ByteArrayOutputStream baos = new ByteArrayOutputStream();

    @BeforeClass
    public static void startup() throws IOException {
        tempDir = SpliceUnitTest.createTempDirectory(SCHEMA_NAME);
        me = createMain();

        execute("elapsedtime off;\n"); // ignore output, can be 0ms or 1ms
        execute("connect '" + SpliceNetConnection.getDefaultLocalURL() + "';\n", "");
        try {
            execute("DROP SCHEMA " + SCHEMA_NAME + " CASCADE;\n");
        } catch(Exception e) {
            e.printStackTrace();
        }
        execute("CREATE SCHEMA " + SCHEMA_NAME + ";\n", zeroRowsUpdated);
        execute("SET SCHEMA " + SCHEMA_NAME + ";\n", zeroRowsUpdated);
    }

    @AfterClass
    public static void shutdown() throws Exception {
        execute("DROP SCHEMA " + SCHEMA_NAME + " CASCADE;\n");
        SpliceUnitTest.deleteTempDirectory(tempDir);
    }

    static String execute(String in) {
        baos.reset();
        LocalizedInput input = langUtil.getNewInput(IOUtils.toInputStream(in));
        me.goGuts(input);
        try {
            return baos.toString(StandardCharsets.UTF_8.name());
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
            return "UnsupportedEncodingException";
        }
    }

    static void execute(String in, String expectedOut) {
        Assert.assertEquals("splice> " + in + expectedOut + "splice> ", execute(in));
    }

    // execute, expect output to match regular expression
    static void executeR(String in, String expectedOutRegex) {
        expectedOutRegex = "splice\\> " + SpliceUnitTest.escapeRegexp(in)
                + expectedOutRegex + "splice\\> \n";
        String o = execute(in);
        SpliceUnitTest.matchMultipleLines(o, expectedOutRegex);
    }

    static Main createMain() {
        ByteArrayOutputStream baos1 = new ByteArrayOutputStream();
        LocalizedOutput out1 = langUtil.getNewOutput(baos1);
        Main me = new Main(out1);
        me.init(langUtil.getNewOutput(baos));
        return me;
    }

    private final static String uuidReg = "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}[ ]*";
    /*
      This testcase essentially test whether the function executes without error.
      It can also perform correctness test to some extent, e.g., number of trigger records returned should match expectation
      and trigger name returned should be reasonable.
      One should manually check the correctness in the returned trigger info according to the specific comments in the testcase below.
      THIS TEST CAN ONLY BE RUN ON ITS OWN (SINGLE THREADED WITHOUT CONCURRENT TRANSACTIONS),
      AS SYSTEM CALL GETTRIGGEREXEC WILL GET THE CURRENT STATE OF TRIGGER INFO, THEN CLEAR EVERYTHING UP FOR THE NEXT ROUND
     */
    @Test
    public void testCallGetTriggerExec() {
        execute("CREATE TABLE TABLE1 (COL1 INT);\n");
        execute("CREATE TRIGGER TR1 BEFORE INSERT ON TABLE1 REFERENCING NEW AS NEW FOR EACH ROW SET NEW.COL1=NEW.COL1*100;\n");
        execute("CREATE TRIGGER TR2 BEFORE INSERT ON TABLE1 REFERENCING NEW AS NEW FOR EACH ROW SET NEW.COL1=NEW.COL1*20;\n");
        execute("INSERT INTO TABLE1 VALUES(1), (2);");

        String header =
                "triggerName [ ]*\\|triggerId [ ]*\\|txnId [ ]*\\|queryId [ ]*\\|parentQueryId  [ ]*\\|timeSpent [ ]*\\|numRowsModified [ ]*\n" +
                 "--------------------------------------------------------------------------------------------------[-]*\n";
        // variable columns:              triggerId   |   txnId  |   queryId       |  parentQueryId  | timeSpent
        String variableColumns = "\\|" + uuidReg + "\\|\\d+\\s*\\|" + uuidReg + "\\|" + uuidReg + "\\|\\d+\\s*\\|";

        // One single event triggering two triggers; they should have same parentQueryId and txnId but different queryId and elapsedTime
         // There will be a row for each query executed during trigger execution, meaning, trigger1 and trigger 2 should each have two queries/rows displayed
        String row12 = "TR[12] [ ]*" + variableColumns + "0[ ]*\n";
        executeR("CALL SYSCS_UTIL.SYSCS_GET_TRIGGER_EXEC();\n",
                 header + row12 + row12 + row12 + row12 +
                "\n" +
                "4 rows selected\n");


        // Any statement that does not trigger a trigger should reset the trigger info
        executeR("CALL SYSCS_UTIL.SYSCS_GET_TRIGGER_EXEC();\n",
                header +
                "\n" +
                "0 rows selected\n");

        execute("CREATE TABLE TABLE2 (COL2 INT);\n");
        execute("CREATE TRIGGER TR3 AFTER INSERT ON TABLE1 FOR EACH STATEMENT INSERT INTO TABLE2 VALUES(2);\n");
        execute("CREATE TRIGGER TR4 BEFORE INSERT ON TABLE2 REFERENCING NEW AS NEW FOR EACH ROW SET NEW.COL2=NEW.COL2*20;\n");
        execute("INSERT INTO TABLE1 VALUES(3);\n");

        // Single level nested trigger: Tr1-3 should be triggered by the original insert statement; tr4 should be triggered by tr3
        String row14 = "TR[1-4] [ ]*" + variableColumns + "[10] [ ]*\n";;
        executeR("CALL SYSCS_UTIL.SYSCS_GET_TRIGGER_EXEC();\n",
                header +
                row14 + row14 + row14 + row14 +
                "\n" +
                "4 rows selected\n");


        execute("CREATE TABLE TABLE3 (COL3 INT);\n");
        execute("CREATE TABLE TABLE4 (COL4 INT);\n");
        execute("CREATE TRIGGER TR5 AFTER INSERT ON TABLE3 REFERENCING NEW AS NEW FOR EACH ROW WHEN (NEW.COL3=5) INSERT INTO TABLE4 VALUES(4);\n");
        execute("CREATE TRIGGER TR6 AFTER INSERT ON TABLE4 REFERENCING NEW AS NEW FOR EACH ROW INSERT INTO TABLE2 VALUES (NEW.COL4*40);\n");
        execute("INSERT INTO TABLE3 VALUES(5);\n");

        // Double level nested trigger: Tr6 should be triggered by tr5; tr4 should be triggered by tr6. Two queries of tr5 are expected
        String row46 = "TR[4-6] [ ]*" + variableColumns + "[10] [ ]*\n";
        executeR("CALL SYSCS_UTIL.SYSCS_GET_TRIGGER_EXEC();\n",
                header + row46 + row46 + row46 + row46 +
                "\n" +
                "4 rows selected\n");

        execute("INSERT INTO TABLE3 VALUES(7);\n");

        // Only tr5 is triggered; since the when clause is not satisfied, tr5 won't do the insertion or trigger tr6
        String row5 = "TR5 [ ]*" + variableColumns + "0 [ ]*\n";
        executeR("CALL SYSCS_UTIL.SYSCS_GET_TRIGGER_EXEC();\n",
                header + row5 +
                "\n" +
                "1 row selected\n");

        execute("CREATE TABLE TABLE5 (COL5 INT);\n");
        execute("CREATE TRIGGER TR7 BEFORE INSERT ON TABLE5 FOR EACH STATEMENT CALL SYSCS_UTIL.SYSCS_GET_VERSION_INFO();\n");
        execute("INSERT INTO TABLE5 VALUES(9);\n");

        // Only tr7 is triggered; test before statement trigger
        String row7 = "TR7 [ ]*" + variableColumns + "0 [ ]*\n";
        executeR("CALL SYSCS_UTIL.SYSCS_GET_TRIGGER_EXEC();\n",
                header + row7 +
                "\n" +
                "1 row selected\n");

        // Drop all tables and triggers created
        for (int i = 0; i < 7; i++) {
            execute("DROP TRIGGER TR" + i + ";\n");
        }
        for (int i = 0; i < 5; i++) {
            execute("DROP TABLE TABLE" + i + ";\n");
        }
    }
}
