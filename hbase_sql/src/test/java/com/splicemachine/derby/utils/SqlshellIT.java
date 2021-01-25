package com.splicemachine.derby.utils;

import com.splicemachine.db.iapi.tools.i18n.LocalizedInput;
import com.splicemachine.db.iapi.tools.i18n.LocalizedOutput;
import com.splicemachine.db.iapi.tools.i18n.LocalizedResource;
import com.splicemachine.db.impl.tools.ij.Main;
import com.splicemachine.db.impl.tools.ij.ProgressThread;
import com.splicemachine.db.impl.tools.ij.ijCommands;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.commons.io.IOUtils;
import org.junit.*;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;

public class SqlshellIT {
    static final LocalizedResource langUtil = LocalizedResource.getInstance();
    static final String zeroRowsUpdated = "0 rows inserted/updated/deleted\n";
    static Main me;
    static String SCHEMA_NAME = SqlshellIT.class.getName().toUpperCase();
    static File tempDir;
    static ByteArrayOutputStream baos = new ByteArrayOutputStream();

    @BeforeClass
    public static void startup() throws IOException {
        tempDir = SpliceUnitTest.createTempDirectory(SCHEMA_NAME);
        me = createMain();

        execute("elapsedtime off;\n"); // ignore output, can be 0ms or 1ms
        execute("connect 'jdbc:splice://localhost:1527/splicedb;user=splice;password=admin';\n", "");
        try {
            execute("DROP SCHEMA SQLSHELLIT CASCADE;\n");
        } catch(Exception e) {
            e.printStackTrace();
        }
        execute("CREATE SCHEMA SQLSHELLIT;\n", zeroRowsUpdated);
        execute("SET SCHEMA SQLSHELLIT;\n", zeroRowsUpdated);

        execute("CREATE TABLE ABC (DEF INTEGER);\n", zeroRowsUpdated);
        execute("CREATE TABLE TX2 (ABC INTEGER);\n", zeroRowsUpdated); // check this is not output when querying T_2
        execute("CREATE TABLE T_2 (COL_1 INTEGER NOT NULL PRIMARY KEY, COL_2 INTEGER, COL_3 DOUBLE, COL_4 VARCHAR(255));\n", zeroRowsUpdated);

        execute("CREATE VIEW VX1 AS SELECT * FROM ABC;\n", zeroRowsUpdated);
        execute("CREATE VIEW V_1 AS SELECT * FROM T_2;\n", zeroRowsUpdated);

        execute("CREATE SYNONYM SVX1 FOR VX1;\n", zeroRowsUpdated);
        execute("CREATE SYNONYM SV_1 FOR V_1;\n", zeroRowsUpdated);
        execute("CREATE SYNONYM STX2 FOR TX2;\n", zeroRowsUpdated);
        execute("CREATE SYNONYM ST_2 FOR T_2;\n", zeroRowsUpdated);
    }

    @AfterClass
    public static void shutdown() throws Exception {
        execute("DROP SCHEMA SQLSHELLIT CASCADE;\n", zeroRowsUpdated);
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

    public static String getJavaRegexpFilterFromAsterixFilter(String asterixFilter)
    {
        String filter = asterixFilter;
        String toEscape[] = {"<", "(", "[", "{", "\\", "^", "-", "=", "$", "!", "|", "]", "}", ")", "+", ".", ">"};
        for(String s : toEscape) {
            filter = filter.replaceAll("\\" + s, "\\\\" + s);
        }

        filter = filter.replaceAll("\\*", ".*");
        return filter.replaceAll("\\?", ".");
    }

    // execute, expect output to match regular expression
    static void executeR(String in, String expectedOutRegex) {
        expectedOutRegex = "splice\\> " + getJavaRegexpFilterFromAsterixFilter(in)
                + expectedOutRegex + "splice\\> \n";
        String o = execute(in);
        String[] o2 = o.split("\n");
        String[] ex2 = expectedOutRegex.split("\n");
        Assert.assertEquals(o + "\n---\n" + expectedOutRegex, o2.length, ex2.length);
        for(int i =0; i<o2.length; i++) {
            Assert.assertTrue(o2[i] + "doesn't match " + ex2[i], o2[i].matches(ex2[i]));
        }
    }

    static Main createMain() {
        ByteArrayOutputStream baos1 = new ByteArrayOutputStream();
        LocalizedOutput out1 = langUtil.getNewOutput(baos1);
        Main me = new Main(out1);
        me.init(langUtil.getNewOutput(baos));
        return me;
    }

    @Test
    public void testDescribeTable() {
        execute("show create table abc;\n",
                    "DDL\n" +
                    "---\n" +
                    "CREATE TABLE \"SQLSHELLIT\".\"ABC\" (\n" +
                    "\"DEF\" INTEGER\n" +
                    ") ;\n" +
                    "\n" +
                    "1 row selected\n");

        execute("describe abc;\n",
                    "COLUMN_NAME                             |TYPE_NAME|DEC&|NUM&|COLUMN_SI&|COLUMN_DEF|CHAR_OCTE&|IS_NU&\n" +
                    "----------------------------------------------------------------------------------------------------\n" +
                    "DEF                                     |INTEGER  |0   |10  |10        |NULL      |NULL      |YES   \n" +
                    "\n" +
                    "1 row selected\n");

        execute( "show primarykeys from abc;\n",
                "TABLE_NAME                    |COLUMN_NAME                   |KEY_SEQ   |PK_NAME                       \n" +
                        "-------------------------------------------------------------------------------------------------------\n" +
                        "\n" +
                        "0 rows selected\n");
    }

    @Test
    public void testDescribeTablePKs() {
        executeR("show create table T_2;\n",
                "DDL\n" +
                        "---\n" +
                        "CREATE TABLE \"SQLSHELLIT\".\"T_2\" \\(\n" +
                        "\"COL_1\" INTEGER NOT NULL\n" +
                        ",\"COL_2\" INTEGER\n" +
                        ",\"COL_3\" DOUBLE\n" +
                        ",\"COL_4\" VARCHAR\\(255\\)\n" +
                        ", CONSTRAINT .* PRIMARY KEY\\(\"COL_1\"\\)\\) ;\n" +
                        "\n" +
                        "1 row selected\n");

        execute("describe T_2;\n",
                "COLUMN_NAME                             |TYPE_NAME|DEC&|NUM&|COLUMN_SI&|COLUMN_DEF|CHAR_OCTE&|IS_NU&\n" +
                        "----------------------------------------------------------------------------------------------------\n" +
                        "COL_1                                   |INTEGER  |0   |10  |10        |NULL      |NULL      |NO    \n" +
                        "COL_2                                   |INTEGER  |0   |10  |10        |NULL      |NULL      |YES   \n" +
                        "COL_3                                   |DOUBLE   |NULL|2   |52        |NULL      |NULL      |YES   \n" +
                        "COL_4                                   |VARCHAR  |NULL|NULL|255       |NULL      |510       |YES   \n" +
                        "\n" +
                        "4 rows selected\n");

        executeR( "show primarykeys from T_2;\n",
                "TABLE_NAME                    \\|COLUMN_NAME                   \\|KEY_SEQ   \\|PK_NAME                       \n" +
                        "-------------------------------------------------------------------------------------------------------\n" +
                        "T_2                           \\|COL_1                         \\|1         \\|.*\n" +
                        "\n" +
                        "1 row selected\n");
    }

    @Test
    public void testShowTables() {
        executeR("show tables in SQLSHELLIT;\n",
                "TABLE_SCHEM[ ]*\\|TABLE_NAME[ ]*\\|CONGLOM_ID[ ]*\\|REMARKS[ ]*\n" +
                        "[-]*\n" +
                        "SQLSHELLIT [ ]*\\|ABC [ ]*\\|\\d* [ ]*\\|[ ]+\n" +
                        "SQLSHELLIT [ ]*\\|TX2 [ ]*\\|\\d* [ ]*\\|[ ]+\n" +
                        "SQLSHELLIT [ ]*\\|T_2 [ ]*\\|\\d* [ ]*\\|[ ]+\n" +
                        "\n" +
                        "3 rows selected\n");
    }

    @Test
    public void testShowViews() {
        executeR("show views in SQLSHELLIT;\n",
                "TABLE_SCHEM[ ]*\\|TABLE_NAME[ ]*\\|CONGLOM_ID[ ]*\\|REMARKS[ ]*\n" +
                        "[-]*\n" +
                        "SQLSHELLIT [ ]*\\|VX1 [ ]*\\|NULL [ ]*\\|[ ]+\n" +
                        "SQLSHELLIT [ ]*\\|V_1 [ ]*\\|NULL [ ]*\\|[ ]+\n" +
                        "\n" +
                        "2 rows selected\n");
    }


    @Test
    public void testShowProcedures() {
        execute("show procedures in SQLJ;\n",
                "PROCEDURE_SCHEM     |PROCEDURE_NAME                                              |REMARKS                                                                                             \n" +
                        "--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------\n" +
                        "SQLJ                |INSTALL_JAR                                                 |com.splicemachine.db.catalog.SystemProcedures.INSTALL_JAR                                           \n" +
                        "SQLJ                |REMOVE_JAR                                                  |com.splicemachine.db.catalog.SystemProcedures.REMOVE_JAR                                            \n" +
                        "SQLJ                |REPLACE_JAR                                                 |com.splicemachine.db.catalog.SystemProcedures.REPLACE_JAR                                           \n" +
                        "\n" +
                        "3 rows selected\n");
    }

    @Test
    public void testDescribeProcedure() {
        execute("describe procedure SQLJ.INSTALL_JAR;\n",
                        "COLUMN_NAME                     |TYPE_NAME                       |ORDINAL_POSITION\n" +
                        "----------------------------------------------------------------------------------\n" +
                        "URL                             |VARCHAR                         |1               \n" +
                        "JAR                             |VARCHAR                         |2               \n" +
                        "DEPLOY                          |INTEGER                         |3               \n" +
                        "\n" +
                        "3 rows selected\n");
    }

    @Test
    public void testShowFunctions() {
        execute("show functions in SYSCS_UTIL;\n",
                "FUNCTION_SCHEM|FUNCTION_NAME                      |REMARKS                                                                         \n" +
                        "-----------------------------------------------------------------------------------------------------------------------------------\n" +
                        "SYSCS_UTIL    |SYSCS_GET_DATABASE_PROPERTY        |com.splicemachine.db.catalog.SystemProcedures.SYSCS_GET_DATABASE_PROPERTY       \n" +
                        "SYSCS_UTIL    |SYSCS_PEEK_AT_SEQUENCE             |com.splicemachine.db.catalog.SystemProcedures.SYSCS_PEEK_AT_SEQUENCE            \n" +
                        "\n" +
                        "2 rows selected\n");
    }

    @Test
    public void testShowSynonyms() {
        execute("show synonyms in SQLSHELLIT;\n",
                "TABLE_SCHEM         |TABLE_NAME                                        |CONGLOM_ID|REMARKS             \n" +
                        "-------------------------------------------------------------------------------------------------------\n" +
                        "SQLSHELLIT          |STX2                                              |NULL      |                    \n" +
                        "SQLSHELLIT          |ST_2                                              |NULL      |                    \n" +
                        "SQLSHELLIT          |SVX1                                              |NULL      |                    \n" +
                        "SQLSHELLIT          |SV_1                                              |NULL      |                    \n" +
                        "\n" +
                        "4 rows selected\n");
    }

    @Test // DB-10049
    public void testTerminator() {
        try {
            execute("SET TERMINATOR '#';\n", "");

            execute("values 1#\n",
                    "1          \n" +
                            "-----------\n" +
                            "1          \n" +
                            "\n" +
                            "1 row selected\n");
        }
        finally {
            // reset option for other tests
            execute("SET TERMINATOR ';'#\n", "");
        }
    }

    @Test // DB-10089
    public void testWithHeader() {
        try {
            execute("WITH HEADER;\n", "");
            String v = "1          \n" +
                    "\n" +
                    "1 row selected\n";
            execute("values 1;\n",
                    "1          \n" +
                            "-----------\n" + v);

            execute("WITHOUT HEADER;\n", "");
            execute("values 1;\n", v);
        }
        finally {
            // reset option for other tests
            execute("WITH HEADER;\n", "");
        }
    }

    @Test
    public void testSpool() throws IOException {
        try {
            String path = tempDir + "/out.tmp";
            execute("SPOOL '" + path + "';\n");

            String expected = "splice> values 23;\n" +
                    "1          \n" +
                    "-----------\n" +
                    "23         \n" +
                    "\n" +
                    "1 row selected\n" +
                    "splice> null;\n";
            Assert.assertEquals(expected, execute("values 23;\n"));
            String fileContent = new String(Files.readAllBytes(Paths.get(path)), StandardCharsets.UTF_8);
            Assert.assertEquals("splice> null;\n" + expected, fileContent);
        }
        finally {
            execute("SPOOL STOP");
        }
    }

    @Test
    public void testRunScript() throws IOException {
        String path = tempDir + "/script.sql";
        FileWriter f = new FileWriter(path);
        f.write("-- this is a comment\n" +
                "values 3;\n" +
                "-- another comment!\n" +
                "values 5;\n"
        );
        f.flush();
        f.close();

        execute("run '" + path + "';\n",
                "splice> -- this is a comment\n" +
                        "values 3;\n" +
                        "1          \n" +
                        "-----------\n" +
                        "3          \n" +
                        "\n" +
                        "1 row selected\n" +
                        "splice> -- another comment!\n" +
                        "values 5;\n" +
                        "1          \n" +
                        "-----------\n" +
                        "5          \n" +
                        "\n" +
                        "1 row selected\n");
    }

    @Test // DB-10373
    public void testMaximumDisplayWidth()
    {
        try {
            execute("CREATE TABLE TABLE_WITH_A_VERY_VERY_RIDICULOUS_SUPER_MUCH_TOO_LONG_NAME (COLUMN_WITH_A_VERY_VERY_RIDICULOUS_SUPER_MUCH_TOO_LONG_NAME INTEGER );\n", zeroRowsUpdated);

            executeR("show tables in SQLSHELLIT;\n",
                    "TABLE_SCHEM[ ]*\\|TABLE_NAME[ ]*\\|CONGLOM_ID[ ]*\\|REMARKS[ ]*\n" +
                            "[-]*\n" +
                            "SQLSHELLIT [ ]*\\|ABC [ ]*\\|\\d* [ ]*\\|[ ]+\n" +
                            "SQLSHELLIT [ ]*\\|TABLE_WITH_A_VERY_VERY_RIDICULOUS_SUPER_MUCH_TOO_&\\|\\d* [ ]*\\|[ ]+\n" +
                            "SQLSHELLIT [ ]*\\|TX2 [ ]*\\|\\d* [ ]*\\|[ ]+\n" +
                            "SQLSHELLIT [ ]*\\|T_2 [ ]*\\|\\d* [ ]*\\|[ ]+\n" +
                            "\n" +
                            "4 rows selected\n");

            execute("maximumdisplaywidth 0;\n", "");
            executeR( "show tables in SQLSHELLIT;\n",
                    "TABLE_SCHEM[ ]*\\|TABLE_NAME[ ]*\\|CONGLOM_ID[ ]*\\|REMARKS[ ]*\n" +
                            "[-]*\n" +
                            "SQLSHELLIT\\|ABC\\|\\d*\\|\n" +
                            "SQLSHELLIT\\|TABLE_WITH_A_VERY_VERY_RIDICULOUS_SUPER_MUCH_TOO_LONG_NAME\\|\\d*\\|\n" +
                            "SQLSHELLIT\\|TX2\\|\\d*\\|\n" +
                            "SQLSHELLIT\\|T_2\\|\\d*\\|\n" +
                            "\n" +
                            "4 rows selected\n");

            execute("maximumdisplaywidth 8;\n", "");
            executeR( "show tables in SQLSHELLIT;\n",
                    "TABLE_S&\\|TABLE_N&\\|CONGLOM&\\|REMARKS \n" +
                            "-----------------------------------\n" +
                            "SQLSHEL&\\|ABC     \\|\\d*[ ]*\\|        \n" +
                            "SQLSHEL&\\|TABLE_W&\\|\\d*[ ]*\\|        \n" +
                            "SQLSHEL&\\|TX2     \\|\\d*[ ]*\\|        \n" +
                            "SQLSHEL&\\|T_2     \\|\\d*[ ]*\\|        \n" +
                            "\n" +
                            "4 rows selected\n");
        }
        finally {
            execute("maximumdisplaywidth 256;\n", "");
            execute("DROP TABLE TABLE_WITH_A_VERY_VERY_RIDICULOUS_SUPER_MUCH_TOO_LONG_NAME IF EXISTS");
        }
    }

    @Test
    public void testHasServerLikeFix() {
        Assert.assertEquals(Arrays.toString(ijCommands.parseVersion("3.2.0.1992")), "[3, 2, 0, 1992]");
        Assert.assertEquals(Arrays.toString(ijCommands.parseVersion("2.8.0.1973-SNAPSHOT")), "[2, 8, 0, 1973]");
    }
}
