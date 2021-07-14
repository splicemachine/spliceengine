package com.splicemachine.derby.utils;

import com.splicemachine.db.iapi.tools.i18n.LocalizedInput;
import com.splicemachine.db.iapi.tools.i18n.LocalizedOutput;
import com.splicemachine.db.iapi.tools.i18n.LocalizedResource;
import com.splicemachine.db.impl.tools.ij.Main;
import com.splicemachine.db.impl.tools.ij.ijCommands;
import com.splicemachine.derby.test.framework.SpliceNetConnection;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
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

public class SqlshellIT {
    static final LocalizedResource langUtil = LocalizedResource.getInstance();
    static final String zeroRowsUpdated = "0 rows inserted/updated/deleted\n";
    static Main me;
    static String SCHEMA_NAME = SqlshellIT.class.getName().toUpperCase();
    static File tempDir;
    static ByteArrayOutputStream baos = new ByteArrayOutputStream();

    String rowsSelected(int num) {
        return "\n" + num + " row" + (num == 1 ? "" : "s") + " selected\n";
    }

    @BeforeClass
    public static void startup() throws IOException {
        tempDir = SpliceUnitTest.createTempDirectory(SCHEMA_NAME);
        me = createMain();

        execute("elapsedtime off;\n"); // ignore output, can be 0ms or 1ms
        execute("connect '" + SpliceNetConnection.getDefaultLocalURL() + "';\n", "");
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

        execute("CREATE ROLE FRED;\n", zeroRowsUpdated);

        execute("CREATE INDEX idx1 ON TX2(ABC);\n", zeroRowsUpdated);
        execute("CREATE INDEX id_1 ON T_2(COL_1);\n", zeroRowsUpdated);

        execute("CREATE VIEW V_2(BLA) AS SELECT DEF FROM ABC;\n", zeroRowsUpdated);
    }

    @AfterClass
    public static void shutdown() throws Exception {
        execute("DROP SCHEMA SQLSHELLIT CASCADE;\n");
        execute("DROP ROLE FRED;\n");
        SpliceUnitTest.deleteTempDirectory(tempDir);
    }

    static String execute(String in) {
        baos.reset();
        LocalizedInput input = langUtil.getNewInput(IOUtils.toInputStream(in));
        me.goGuts(input, false);
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

    @Test
    public void testDescribeTable() {
        execute("show create table abc;\n",
                    "DDL\n" +
                    "---\n" +
                    "CREATE TABLE \"SQLSHELLIT\".\"ABC\" (\n" +
                    "\"DEF\" INTEGER\n" +
                    ") ;\n" + rowsSelected(1));

        execute("describe abc;\n",
                    "COLUMN_NAME                             |TYPE_NAME|DEC&|NUM&|COLUMN_SI&|COLUMN_DEF|CHAR_OCTE&|IS_NU&\n" +
                    "----------------------------------------------------------------------------------------------------\n" +
                    "DEF                                     |INTEGER  |0   |10  |10        |NULL      |NULL      |YES   \n" +
                    rowsSelected(1));

        execute( "show primarykeys from abc;\n",
                    "TABLE_NAME                    |COLUMN_NAME                   |KEY_SEQ   |PK_NAME                       \n" +
                    "-------------------------------------------------------------------------------------------------------\n" +
                    rowsSelected(0));
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
                        rowsSelected(1));

        execute("describe T_2;\n",
                "COLUMN_NAME                             |TYPE_NAME|DEC&|NUM&|COLUMN_SI&|COLUMN_DEF|CHAR_OCTE&|IS_NU&\n" +
                        "----------------------------------------------------------------------------------------------------\n" +
                        "COL_1                                   |INTEGER  |0   |10  |10        |NULL      |NULL      |NO    \n" +
                        "COL_2                                   |INTEGER  |0   |10  |10        |NULL      |NULL      |YES   \n" +
                        "COL_3                                   |DOUBLE   |NULL|2   |52        |NULL      |NULL      |YES   \n" +
                        "COL_4                                   |VARCHAR  |NULL|NULL|255       |NULL      |510       |YES   \n" +
                        rowsSelected(4));

        executeR( "show primarykeys from T_2;\n", SpliceUnitTest.escapeRegexp(
                        "TABLE_NAME                    |COLUMN_NAME                   |KEY_SEQ   |PK_NAME                       \n" +
                        "-------------------------------------------------------------------------------------------------------\n" +
                        "T_2                           |COL_1                         |1         |§\n" +
                        rowsSelected(1)) );
    }

    @Test
    public void testShowView() {
        execute("show create view VX1;\n",
                 "DDL\n" +
                         "---\n" +
                         "CREATE VIEW VX1 AS SELECT * FROM ABC;\n" +
                         "\n" +
                         "1 row selected\n");

        execute("show create view V_2;\n",
                "DDL\n" +
                        "---\n" +
                        "CREATE VIEW V_2(BLA) AS SELECT DEF FROM ABC;\n" +
                        "\n" +
                        "1 row selected\n");
    }

    @Test
    public void testShowTables() {
        executeR("show tables in SQLSHELLIT;\n", SpliceUnitTest.escapeRegexp(
            "TABLE_SCHEM         |TABLE_NAME                                        |CONGLOM_ID|REMARKS             \n" +
            "-------------------------------------------------------------------------------------------------------\n" +
            "SQLSHELLIT          |ABC                                               |§|                    \n" +
            "SQLSHELLIT          |TX2                                               |§|                    \n" +
            "SQLSHELLIT          |T_2                                               |§|                    \n" +
            rowsSelected(3)) );
    }

    @Test
    public void testShowViews() {
        execute("show views in SQLSHELLIT;\n",
            "TABLE_SCHEM         |TABLE_NAME                                        |CONGLOM_ID|REMARKS             \n" +
            "-------------------------------------------------------------------------------------------------------\n" +
            "SQLSHELLIT          |VX1                                               |NULL      |                    \n" +
            "SQLSHELLIT          |V_1                                               |NULL      |                    \n" +
            "SQLSHELLIT          |V_2                                               |NULL      |                    \n" +
            rowsSelected(3));
    }


    @Test
    public void testShowProcedures() {
        execute("show procedures in SQLJ;\n",
                "PROCEDURE_SCHEM     |PROCEDURE_NAME                                              |REMARKS                                                                                             \n" +
                        "--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------\n" +
                        "SQLJ                |INSTALL_JAR                                                 |com.splicemachine.db.catalog.SystemProcedures.INSTALL_JAR                                           \n" +
                        "SQLJ                |REMOVE_JAR                                                  |com.splicemachine.db.catalog.SystemProcedures.REMOVE_JAR                                            \n" +
                        "SQLJ                |REPLACE_JAR                                                 |com.splicemachine.db.catalog.SystemProcedures.REPLACE_JAR                                           \n" +
                        rowsSelected(3));
    }

    @Test
    public void testDescribeProcedure() {
        String res =
                "COLUMN_NAME                     |TYPE_NAME                       |ORDINAL_POSITION\n" +
                "----------------------------------------------------------------------------------\n" +
                "URL                             |VARCHAR                         |1               \n" +
                "JAR                             |VARCHAR                         |2               \n" +
                "DEPLOY                          |INTEGER                         |3               \n" +
                rowsSelected(3);

        execute("show procedurecols in SQLJ FROM INSTALL_JAR;\n", res);
        execute("show procedurecols FROM SQLJ.INSTALL_JAR;\n", res);
        execute("describe procedure SQLJ.INSTALL_JAR;\n", res);
    }

    @Test
    public void testShowFunctions() {
        execute("show functions in SYSCS_UTIL;\n",
                "FUNCTION_SCHEM|FUNCTION_NAME                      |REMARKS                                                                         \n" +
                        "-----------------------------------------------------------------------------------------------------------------------------------\n" +
                        "SYSCS_UTIL    |SYSCS_GET_DATABASE_PROPERTY        |com.splicemachine.db.catalog.SystemProcedures.SYSCS_GET_DATABASE_PROPERTY       \n" +
                        "SYSCS_UTIL    |SYSCS_PEEK_AT_SEQUENCE             |com.splicemachine.db.catalog.SystemProcedures.SYSCS_PEEK_AT_SEQUENCE            \n" +
                        rowsSelected(2));
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
                        rowsSelected(4));
    }

    @Test // DB-10049
    public void testTerminator() {
        try {
            execute("SET TERMINATOR '#';\n", "");

            execute("values 1#\n",
                    "1          \n" +
                    "-----------\n" +
                    "1          \n" +
                    rowsSelected(1));
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
            String v =  "1          \n" +
                       rowsSelected(1);
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
                    rowsSelected(1) +
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
                        rowsSelected(1) +
                        "splice> -- another comment!\n" +
                        "values 5;\n" +
                        "1          \n" +
                        "-----------\n" +
                        "5          \n" +
                        rowsSelected(1));
    }

    @Test // DB-10373
    public void testMaximumDisplayWidth()
    {
        try {
            execute("CREATE TABLE TABLE_WITH_A_VERY_VERY_RIDICULOUS_SUPER_MUCH_TOO_LONG_NAME (COLUMN_WITH_A_VERY_VERY_RIDICULOUS_SUPER_MUCH_TOO_LONG_NAME INTEGER );\n", zeroRowsUpdated);

            executeR("show tables in SQLSHELLIT;\n", SpliceUnitTest.escapeRegexp(
                    "TABLE_SCHEM         |TABLE_NAME                                        |CONGLOM_ID|REMARKS             \n" +
                    "-------------------------------------------------------------------------------------------------------\n" +
                    "SQLSHELLIT          |ABC                                               |§|                    \n" +
                    "SQLSHELLIT          |TABLE_WITH_A_VERY_VERY_RIDICULOUS_SUPER_MUCH_TOO_&|§|                    \n" +
                    "SQLSHELLIT          |TX2                                               |§|                    \n" +
                    "SQLSHELLIT          |T_2                                               |§|                    \n" +
                    rowsSelected(4)) );

            execute("maximumdisplaywidth 0;\n", "");
            executeR( "show tables in SQLSHELLIT;\n", SpliceUnitTest.escapeRegexp(
                    "TABLE_SCHEM|TABLE_NAME|CONGLOM_ID|REMARKS\n" +
                    "-----------------------------------------\n" +
                    "SQLSHELLIT|ABC|§|\n" +
                    "SQLSHELLIT|TABLE_WITH_A_VERY_VERY_RIDICULOUS_SUPER_MUCH_TOO_LONG_NAME|§|\n" +
                    "SQLSHELLIT|TX2|§|\n" +
                    "SQLSHELLIT|T_2|§|\n" +
                    rowsSelected(4)) );

            execute("maximumdisplaywidth 8;\n", "");
            executeR( "show tables in SQLSHELLIT;\n", SpliceUnitTest.escapeRegexp(
                    "TABLE_S&|TABLE_N&|CONGLOM&|REMARKS \n" +
                    "-----------------------------------\n" +
                    "SQLSHEL&|ABC     |§|        \n" +
                    "SQLSHEL&|TABLE_W&|§|        \n" +
                    "SQLSHEL&|TX2     |§|        \n" +
                    "SQLSHEL&|T_2     |§|        \n" +
                    rowsSelected(4)) );
        }
        finally {
            execute("maximumdisplaywidth 256;\n", "");
            execute("DROP TABLE TABLE_WITH_A_VERY_VERY_RIDICULOUS_SUPER_MUCH_TOO_LONG_NAME IF EXISTS");
        }
    }

    @Test
    public void testShowIndices()
    {
        try {
            executeR("show indexes from SQLSHELLIT.TX2;\n", SpliceUnitTest.escapeRegexp(
                "TABLE_NAME                                        |INDEX_NAME                                        |COLUMN_NAME         |ORDINAL&|NON_UNIQUE|TYPE |ASC&|CONGLOM_NO\n" +
                "--------------------------------------------------------------------------------------------------------------------------------------------------------------------\n" +
                "TX2                                               |IDX1                                              |ABC                 |1       |true      |BTREE|A   |§\n" +
                "\n" +
                "1 row selected\n" ) );

            execute("maximumdisplaywidth 10;\n");

            executeR("show indexes from SQLSHELLIT.T_2;\n",
                    SpliceUnitTest.escapeRegexp(   "TABLE_NAME|INDEX_NAME|COLUMN_NA&|ORDINAL_P&|NON_UNIQUE|TYPE      |ASC_OR_DE&|CONGLOM_NO\n" +
                                    "---------------------------------------------------------------------------------------\n" +
                                    "T_2       |ID_1      |COL_1     |1         |true      |BTREE     |A         |§\n" +
                                    rowsSelected(1)) );

            execute("maximumdisplaywidth 0;\n", "");
            executeR("show indexes in SQLSHELLIT;\n", SpliceUnitTest.escapeRegexp(
                    "TABLE_NAME|INDEX_NAME|COLUMN_NAME|ORDINAL_POSITION|NON_UNIQUE|TYPE|ASC_OR_DESC|CONGLOM_NO\n" +
                    "-----------------------------------------------------------------------------------------\n" +
                    "TX2|IDX1|ABC|1|true|BTREE|A|§\n" +
                    "T_2|ID_1|COL_1|1|true|BTREE|A|§\n" +
                     rowsSelected(2) ) );
        }
        finally {
            execute("maximumdisplaywidth 256;\n", "");
        }
    }

    @Test // DB-11155
    public void testShowRoles() {
        String result = execute("show roles;\n");
        // some other tests might have added users, so the output might differ a bit
        Assert.assertTrue( result.contains("ROLEID") );
        Assert.assertTrue( result.contains("------") );
        Assert.assertTrue( result.contains("FRED") );
        // show settable_roles should print the same as show roles
        String result2 = execute("show settable_roles;\n");
        // ignore first line (differs because different SQL command is printed there)
        String[] res = result.split("\n");
        String[] res2 = result2.split("\n");
        res = Arrays.copyOfRange( res, 1, res.length);
        res2 = Arrays.copyOfRange( res2, 1, res2.length);
        Assert.assertArrayEquals( res, res2 );
    }

    @Test
    public void testShowVersion() throws Exception {
        String contain;
        if( SpliceUnitTest.isMemPlatform(new SpliceWatcher()) ) {
            contain = "UNKNOWN";
        }
        else {
            contain = "http://www.splicemachine.com";
        }
        Assert.assertTrue(execute("show version local;\n").contains(contain));
    }

    @Test
    public void testHasServerLikeFix() {
        Assert.assertEquals(Arrays.toString(ijCommands.parseVersion("3.2.0.1992")), "[3, 2, 0, 1992]");
        Assert.assertEquals(Arrays.toString(ijCommands.parseVersion("2.8.0.1973-SNAPSHOT")), "[2, 8, 0, 1973]");
    }

    public void testSyntaxErrorMessage() {
        execute("CREATE TABLE MY_TABLE (\n" +
                "L_ORDERKEY BIGINT NOT NULL,\n" +
                "L_PARTKEY INTEGER NOT DECIMAL,\n" +
                "L_SUPPKEY INTEGER NOT NULL,\n" +
                "L_LINENUMBER INTEGER NOT NULL,\n" +
                "L_QUANTITY DECIMAL(15,2),\n" +
                "L_EXTENDEDPRICE DECIMAL(15,2),\n" +
                "L_DISCOUNT DECIMAL(15,2)\n" +
                ");\n",

                "ERROR 42X01: Syntax error: Encountered \"DECIMAL\" at line 3, column 23.\n" +
                "Was expecting:\n" +
                "    \"null\" ...\n" +
                "    \n" +
                "0:\tCREATE TABLE MY_TABLE (\n" +
                "1:\tL_ORDERKEY BIGINT NOT NULL,\n" +
                "2:\tL_PARTKEY INTEGER NOT DECIMAL,\n" +
                "   \t                      ^^^^^^^---------\n" +
                "3:\tL_SUPPKEY INTEGER NOT NULL,\n" +
                "4:\tL_LINENUMBER INTEGER NOT NULL,\n" +
                "5:\tL_QUANTITY DECIMAL(15,2),\n" +
                ".\n" +
                "Issue the 'help' command for general information on Splice command syntax.\n" +
                "Any unrecognized commands are treated as potential SQL commands and executed directly.\n" +
                "Consult your DBMS server reference documentation for details of the SQL syntax supported by your server.\n");
    }

    @Test
    public void testLexerErrorMessage() {
        execute("CREATE TABLE MY_TABLE (\n" +
                "L_ORDERKEY BIGINT NOT NULL,\n" +
                "L_PARTKEY # INTEGER NOT NULL,\n" +
                "L_SUPPKEY INTEGER NOT NULL,\n" +
                "L_LINENUMBER INTEGER NOT NULL,\n" +
                "L_QUANTITY DECIMAL(15,2),\n" +
                "L_EXTENDEDPRICE DECIMAL(15,2),\n" +
                "L_DISCOUNT DECIMAL(15,2)\n" +
                ");\n",

                "ERROR 42X02: Lexical error at line 3, column 11.  Encountered: \"#\" (35), after : \"\"\n" +
                "\n" +
                "0:\tCREATE TABLE MY_TABLE (\n" +
                "1:\tL_ORDERKEY BIGINT NOT NULL,\n" +
                "2:\tL_PARTKEY # INTEGER NOT NULL,\n" +
                "   \t         ^^---------\n" +
                "3:\tL_SUPPKEY INTEGER NOT NULL,\n" +
                "4:\tL_LINENUMBER INTEGER NOT NULL,\n" +
                "5:\tL_QUANTITY DECIMAL(15,2),\n" +
                ".\n");
    }

    @Test
    public void testPromptClockElapsedTime()
    {
        try {
            execute("without header");
            execute("prompt clock on");
            execute("elapsedtime on");

            String command = "values 1;\n";
            // e.g. 2021-07-06 12:43:06+0200
            String timestamp = "\\d\\d\\d\\d-\\d\\d-\\d\\d \\d\\d:\\d\\d:.*";
            SpliceUnitTest.matchMultipleLines(execute(command),
                    "splice " + timestamp + "\\> " + command +
                    "1          \n" +
                    rowsSelected(1) +
                    "ELAPSED TIME = \\d* milliseconds\n" +
                    "splice " + timestamp + "\\> \n");
        } finally {
            execute("with header");
            execute("prompt clock off");
            execute("elapsedtime off");
        }
    }

    @Test
    public void testCustomFormatsOption() {
        try {
            execute("without header");

            // eg. 15:36:51
            String timeFormat = "\\d\\d:\\d\\d:\\d\\d";
            executeR("values current_time;\n",
                    timeFormat + "\n" +
                     rowsSelected(1));

            // e.g. 2021-07-06
            String dateFormat = "\\d\\d\\d\\d-\\d\\d-\\d\\d";
            executeR("values current_date;\n",
                    dateFormat + "\n" +
                            rowsSelected(1));

            // e.g. 2021-07-09 15:34:36.474536
            String timestampFormat = dateFormat + " " + timeFormat + ".\\d\\d\\d\\d\\d\\d";
            executeR("values current_timestamp;\n",
                     timestampFormat + "[ ]*\n" +
                     rowsSelected(1));

            // custom

            // eg. 15.36.39
            execute("set timeformat 'HH.mm.ss'");
            executeR("values current_time;\n",
                    "\\d\\d.\\d\\d.\\d\\d" + "\n" +
                    rowsSelected(1));

            execute("set dateformat 'dd.MM.YYYY'");
            executeR("values current_date;\n",
                    "\\d\\d.\\d\\d.\\d\\d\\d\\d\n" + // e.g. 06.07.2021
                            rowsSelected(1));

            execute("set timestampformat 'HH.mm.ss'");
            executeR("values current_timestamp;\n",
                    "\\d\\d.\\d\\d.\\d\\d[ ]*" + "\n" +
                            rowsSelected(1));

            execute("CREATE TABLE T_NULL_DATE_CUSTOM (I INTEGER, D DATE);\n", zeroRowsUpdated);
            execute("INSERT INTO T_NULL_DATE_CUSTOM (I) VALUES 1;\n");
            execute("SELECT * FROM T_NULL_DATE_CUSTOM;\n",
                        "1          |NULL      \n" +
                       rowsSelected(1));

        }
        finally {
            execute("set dateformat 'YYYY-MM-dd'");
            execute("set timeformat 'HH:mm:ss'");
            execute("set timestampformat 'yyyy-MM-dd T HH:mm:ss.SSS';");
            execute("with header");
            execute("DROP TABLE T_NULL_DATE_CUSTOM IF EXISTS");
        }
    }
}
