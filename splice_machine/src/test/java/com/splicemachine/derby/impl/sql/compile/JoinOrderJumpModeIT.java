package com.splicemachine.derby.impl.sql.compile;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.test_tools.TableCreator;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.sql.Connection;

/**
 * Created by zli on 01/20/21.
 */
public class JoinOrderJumpModeIT extends SpliceUnitTest {
    public static final String CLASS_NAME = JoinOrderJumpModeIT.class.getSimpleName().toUpperCase();
    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher(CLASS_NAME);
    protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(spliceSchemaWatcher);
    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(CLASS_NAME);

    public static void createData(Connection conn, String schemaName) throws Exception {

        new TableCreator(conn)
                .withCreate("CREATE TABLE \"TABLE_1\" (\n" +
                        "\"GA#\" CHAR(36) NOT NULL\n" +
                        ",\"ACTIVITY\" CHAR(5) NOT NULL DEFAULT ''\n" +
                        ",\"AFGZP\" TIMESTAMP NOT NULL\n" +
                        ",\"USERID\" CHAR(8) NOT NULL DEFAULT ''\n" +
                        ",\"TAKEOVERTIME\" TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP\n" +
                        ",\"LOGDAT\" DATE NOT NULL\n" +
                        ",\"VALID\" CHAR(1) NOT NULL DEFAULT 'J'\n" +
                        ",\"PLATFORM\" CHAR(10) NOT NULL DEFAULT ''\n" +
                        ",\"GF#\" CHAR(36) NOT NULL\n" +
                        ",PRIMARY KEY(\"GA#\"))")
                .withIndex("create index IDX_1 on \"TABLE_1\" (\"GF#\", \"ACTIVITY\", \"GA#\", \"AFGZP\", \"TAKEOVERTIME\")")
                .create();

        new TableCreator(conn)
                .withCreate("CREATE TABLE \"TABLE_2\" (\n" +
                        "\"MA#\" CHAR(36) NOT NULL\n" +
                        ",\"USERID\" CHAR(8) NOT NULL DEFAULT ''\n" +
                        ",\"TYPE\" CHAR(1) NOT NULL DEFAULT ''\n" +
                        ",\"STATUS\" CHAR(2) NOT NULL DEFAULT ''\n" +
                        ",\"PN\" CHAR(8) NOT NULL DEFAULT ''\n" +
                        ",\"LNAME\" VARCHAR(128) NOT NULL DEFAULT ''\n" +
                        ",\"FNAME\" VARCHAR(65) NOT NULL DEFAULT ''\n" +
                        ",\"DEGREE\" CHAR(4) NOT NULL DEFAULT ''\n" +
                        ",\"GD\" CHAR(1) NOT NULL DEFAULT ''\n" +
                        ",\"IDATE\" DATE NOT NULL\n" +
                        ",\"ODATE\" DATE NOT NULL\n" +
                        ",\"MEMOID\" CHAR(8) NOT NULL DEFAULT ''\n" +
                        ",\"SP\" CHAR(3) NOT NULL DEFAULT ''\n" +
                        ",\"OF\" CHAR(10) NOT NULL DEFAULT ''\n" +
                        ",\"EMAIL\" VARCHAR(100) NOT NULL DEFAULT ''\n" +
                        ",\"MAST\" VARCHAR(65) NOT NULL DEFAULT ''\n" +
                        ",\"CMT\" VARCHAR(254) NOT NULL DEFAULT ''\n" +
                        ",\"NAMEZ\" CHAR(6) NOT NULL DEFAULT ''\n" +
                        ",\"AT\" CHAR(2) NOT NULL DEFAULT ''\n" +
                        ",\"ZN\" VARCHAR(128) NOT NULL DEFAULT ''\n" +
                        ",\"MAF\" CHAR(3) NOT NULL DEFAULT ''\n" +
                        ",\"BSTG\" DECIMAL(3,0) NOT NULL DEFAULT 0.0\n" +
                        ",\"ADB\" CHAR(3) NOT NULL DEFAULT ''\n" +
                        ",\"ADT\" CHAR(3) NOT NULL DEFAULT ''\n" +
                        ",PRIMARY KEY(\"MA#\"))")
                .withIndex("create index IDX_2 on \"TABLE_2\" (\"USERID\", \"MA#\")")
                .create();

        new TableCreator(conn)
                .withCreate("CREATE TABLE \"TABLE_3\" (\n" +
                        "\"MAP#\" CHAR(36) NOT NULL\n" +
                        ",\"MA#\" CHAR(36) NOT NULL\n" +
                        ",\"STLOK#\" CHAR(36) NOT NULL\n" +
                        ",\"TELDW\" CHAR(5) NOT NULL DEFAULT ''\n" +
                        ",\"MAPA\" CHAR(1) NOT NULL DEFAULT ''\n" +
                        ",\"GT\" DATE NOT NULL\n" +
                        ",\"GB\" DATE NOT NULL\n" +
                        ",\"FN\" CHAR(30) NOT NULL DEFAULT ''\n" +
                        ",\"TNS\" CHAR(20) NOT NULL DEFAULT ''\n" +
                        ",\"TDS\" CHAR(5) NOT NULL DEFAULT ''\n" +
                        ",\"ISB\" VARCHAR(65) NOT NULL DEFAULT ''\n" +
                        ",\"KZBO\" CHAR(1) NOT NULL DEFAULT ''\n" +
                        ",\"MTN\" VARCHAR(50) NOT NULL DEFAULT ''\n" +
                        ",PRIMARY KEY(\"MAP#\"))")
                .withIndex("create index IDX_3 on \"TABLE_3\" (\"MA#\")")
                .create();

        new TableCreator(conn)
                .withCreate("CREATE TABLE \"TABLE_4\" (\n" +
                        "\"STLOK#\" CHAR(36) NOT NULL\n" +
                        ",\"ST#\" CHAR(36) NOT NULL\n" +
                        ",\"LK#\" CHAR(36) NOT NULL\n" +
                        ",PRIMARY KEY(\"STLOK#\"))")
                .create();

        new TableCreator(conn)
                .withCreate("CREATE TABLE \"TABLE_5\" (\n" +
                        "\"ST#\" CHAR(36) NOT NULL\n" +
                        ",\"GT\" DATE NOT NULL\n" +
                        ",\"GB\" DATE NOT NULL\n" +
                        ",\"SKB\" CHAR(8) NOT NULL DEFAULT ''\n" +
                        ",\"SA\" CHAR(1) NOT NULL DEFAULT ''\n" +
                        ",\"FS\" CHAR(4) NOT NULL DEFAULT ''\n" +
                        ",\"SBA\" CHAR(8) NOT NULL DEFAULT ''\n" +
                        ",\"SF\" CHAR(4) NOT NULL DEFAULT ''\n" +
                        ",\"OE#\" CHAR(36) NOT NULL\n" +
                        ",\"UEB_ST#\" CHAR(36)\n" +
                        ",\"RGP#\" CHAR(36)\n" +
                        ",\"SB\" VARCHAR(65) NOT NULL DEFAULT ''\n" +
                        ",\"AG\" CHAR(2) NOT NULL DEFAULT ''\n" +
                        ",PRIMARY KEY(\"ST#\"))")
                .create();

        new TableCreator(conn)
                .withCreate("CREATE TABLE \"TABLE_6\" (\n" +
                        "\"LK#\" CHAR(36) NOT NULL\n" +
                        ",\"LKN\" CHAR(5) NOT NULL DEFAULT ''\n" +
                        ",\"LKKN\" CHAR(8) NOT NULL DEFAULT ''\n" +
                        ",\"LKKN2\" CHAR(16) NOT NULL DEFAULT ''\n" +
                        ",\"LKTN\" CHAR(10) NOT NULL DEFAULT ''\n" +
                        ",\"LKT\" CHAR(1) NOT NULL DEFAULT ''\n" +
                        ",\"KZLKW\" CHAR(1) NOT NULL DEFAULT ''\n" +
                        ",\"SE\" CHAR(3) NOT NULL DEFAULT ''\n" +
                        ",\"GT\" DATE NOT NULL\n" +
                        ",\"GB\" DATE NOT NULL\n" +
                        ",\"UEB_LK#\" CHAR(36)\n" +
                        ",\"LKNA\" VARCHAR(65) NOT NULL DEFAULT ''\n" +
                        ",PRIMARY KEY(\"LK#\"))")
                .withIndex("create index IDX_6 on \"TABLE_6\" (\"LKKN\", \"LK#\")")
                .create();

        conn.createStatement().executeQuery(format(
                "CALL SYSCS_UTIL.FAKE_TABLE_STATISTICS('%s', 'TABLE_1', 5822681, 124, 2)", schemaName));
        conn.createStatement().executeQuery(format(
                "call syscs_util.fake_column_statistics('%s', 'TABLE_1', 'GF#', 0, 1)", schemaName));
        conn.createStatement().executeQuery(format(
                "call syscs_util.fake_column_statistics('%s', 'TABLE_1', 'USERID', 0, 1)", schemaName));

        conn.createStatement().executeQuery(format(
                "CALL SYSCS_UTIL.FAKE_TABLE_STATISTICS('%s', 'TABLE_2', 34609, 189, 1)", schemaName));
        conn.createStatement().executeQuery(format(
                "call syscs_util.fake_column_statistics('%s', 'TABLE_2', 'MA#', 0, 1)", schemaName));
        conn.createStatement().executeQuery(format(
                "call syscs_util.fake_column_statistics('%s', 'TABLE_2', 'USERID', 0, 1)", schemaName));

        conn.createStatement().executeQuery(format(
                "CALL SYSCS_UTIL.FAKE_TABLE_STATISTICS('%s', 'TABLE_3', 85085, 178, 1)", schemaName));
        conn.createStatement().executeQuery(format(
                "call syscs_util.fake_column_statistics('%s', 'TABLE_3', 'MA#', 0, 1)", schemaName));
        conn.createStatement().executeQuery(format(
                "call syscs_util.fake_column_statistics('%s', 'TABLE_3', 'STLOK#', 0, 1)", schemaName));

        conn.createStatement().executeQuery(format(
                "CALL SYSCS_UTIL.FAKE_TABLE_STATISTICS('%s', 'TABLE_3', 85085, 178, 1)", schemaName));
        conn.createStatement().executeQuery(format(
                "call syscs_util.fake_column_statistics('%s', 'TABLE_3', 'MA#', 0, 1)", schemaName));
        conn.createStatement().executeQuery(format(
                "call syscs_util.fake_column_statistics('%s', 'TABLE_3', 'STLOK#', 0, 1)", schemaName));

        conn.createStatement().executeQuery(format(
                "CALL SYSCS_UTIL.FAKE_TABLE_STATISTICS('%s', 'TABLE_4', 20991, 108, 1)", schemaName));
        conn.createStatement().executeQuery(format(
                "call syscs_util.fake_column_statistics('%s', 'TABLE_4', 'STLOK#', 0, 1)", schemaName));
        conn.createStatement().executeQuery(format(
                "call syscs_util.fake_column_statistics('%s', 'TABLE_4', 'LK#', 0, 1)", schemaName));

        conn.createStatement().executeQuery(format(
                "CALL SYSCS_UTIL.FAKE_TABLE_STATISTICS('%s', 'TABLE_5', 800, 199, 1)", schemaName));
        conn.createStatement().executeQuery(format(
                "call syscs_util.fake_column_statistics('%s', 'TABLE_5', 'ST#', 0, 1)", schemaName));

        conn.createStatement().executeQuery(format(
                "CALL SYSCS_UTIL.FAKE_TABLE_STATISTICS('%s', 'TABLE_6', 20024, 156, 1)", schemaName));
        conn.createStatement().executeQuery(format(
                "call syscs_util.fake_column_statistics('%s', 'TABLE_6', 'LK#', 0, 1)", schemaName));

        conn.commit();
    }

    @BeforeClass
    public static void createDataSet() throws Exception {
        createData(spliceClassWatcher.getOrCreateConnection(), spliceSchemaWatcher.toString());
    }

    @Test
    public void testResetJumpModeForNextRound() throws Exception {
        String sqlText = "explain SELECT GA.LOGDAT,GA.USERID,ST.SKB \n" +
                "FROM\n" +
                "     TABLE_1 GA --splice-properties useSpark=false\n" +
                "   , TABLE_2 MD\n" +
                "   , TABLE_3 MP\n" +
                "   , TABLE_4 SL\n" +
                "   , TABLE_5 ST\n" +
                "   , TABLE_6 L\n" +
                "WHERE GF#='0c21cd04-9277-11e0-aff0-00247e126b6c'\n" +
                "  AND GA.USERID=MD.USERID\n" +
                "  AND MD.MA#=MP.MA#\n" +
                "  AND MP.MAPA='H'\n" +
                "  AND MP.GB=(\n" +
                "        SELECT MAX(GB)\n" +
                "        FROM TABLE_3 \n" +
                "        WHERE MA#=MP.MA#\n" +
                "          AND MAPA=MP.MAPA\n" +
                "      )\n" +
                "  AND MP.STLOK#=SL.STLOK#\n" +
                "  AND SL.ST#=ST.ST#\n" +
                "  AND SL.LK#=L.LK#\n" +
                "  AND L.LKKN NOT LIKE 'TCCCSM%' \n" +
                "  AND (L.LKKN='SCCCC' OR L.LKKN LIKE 'TCCC%' OR L.LKKN LIKE 'SL2%' OR 0<>0)\n" +
                "  AND TAKEOVERTIME>='2011-06-09 11:04:13.423203'\n" +
                "  AND TAKEOVERTIME<'2020-04-13 20:11:45.069000'\n" +
                "ORDER BY TAKEOVERTIME DESC WITH UR";

        /* expected join order (I=IndexScan, LK=IndexLookup):
                                      nestedloop
                                      /        \
                                  broadcast    Group - TABLE_3 (I, LK)
                                  /       \
                              broadcast   TABLE_6 (I)
                              /       \
                         nestedloop   TABLE_5
                         /        \
                    nestedloop    TABLE_4
                    /        \
               nestedloop    TABLE_3 (I, LK)
               /        \
           TABLE_1    TABLE_2
            (I, LK)            (I)

           The plan above is obtained with real data. Since this test only mock statistics, it could be
           different on join strategies or access paths. However, the join order must be the same.
        */

        rowContainsQuery(new int[]{6,9,12,13,14,16,17,18,19,20,21,23,24,25,27,28,29}, sqlText, methodWatcher,
                new String[] {"NestedLoopJoin"},                                              // 6
                new String[] {"GroupBy"},                                                     // 9
                new String[] {"IndexLookup", "outputRows=1"},                                 // 12
                new String[] {"IndexScan[IDX_3", "outputRows=1"},                             // 13
                new String[] {"Join"},                                                        // 14
                new String[] {"Scan["},                                                       // 16, IndexScan on mem but TableScan on cdh
                new String[] {"Join"},                                                        // 17
                new String[] {"TableScan[TABLE_5", "scannedRows=800,outputRows=800"},         // 18
                new String[] {"Join"},                                                        // 19
                new String[] {"TableScan[TABLE_4", "scannedRows=1,outputRows=1"},             // 20
                new String[] {"Join"},                                                        // 21
                new String[] {"IndexLookup"},                                                 // 23
                new String[] {"IndexScan[IDX_3", "scannedRows=1,outputRows=1"},               // 24
                new String[] {"Join"},                                                        // 25
                new String[] {"IndexScan[IDX_2", "scannedRows=34609,outputRows=34609"},       // 27
                new String[] {"IndexLookup"},                                                 // 28
                new String[] {"IndexScan[IDX_1", "scannedRows=1,outputRows=1"}                // 29
                );
    }
}
