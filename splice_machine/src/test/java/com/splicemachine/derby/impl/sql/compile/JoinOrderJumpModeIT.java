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
                .withCreate("CREATE TABLE \"TGSCHFAKT\" (\n" +
                        "\"GA#\" CHAR(36) NOT NULL\n" +
                        ",\"GFAKTIVITAET\" CHAR(5) NOT NULL DEFAULT ''\n" +
                        ",\"AUSFBEGZP\" TIMESTAMP NOT NULL\n" +
                        ",\"USERID\" CHAR(8) NOT NULL DEFAULT ''\n" +
                        ",\"UEBERNAHMEZEIT\" TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP\n" +
                        ",\"LOGDAT\" DATE NOT NULL\n" +
                        ",\"KZGUELTIG\" CHAR(1) NOT NULL DEFAULT 'J'\n" +
                        ",\"AKTPLATTFORM\" CHAR(10) NOT NULL DEFAULT ''\n" +
                        ",\"GF#\" CHAR(36) NOT NULL\n" +
                        ",PRIMARY KEY(\"GA#\"))")
                .withIndex("create index XFKGAGF on \"TGSCHFAKT\" (\"GF#\", \"GFAKTIVITAET\", \"GA#\", \"AUSFBEGZP\", \"UEBERNAHMEZEIT\")")
                .create();

        new TableCreator(conn)
                .withCreate("CREATE TABLE \"TMITARBEITER_DEF\" (\n" +
                        "\"MA#\" CHAR(36) NOT NULL\n" +
                        ",\"USERID\" CHAR(8) NOT NULL DEFAULT ''\n" +
                        ",\"MATYP\" CHAR(1) NOT NULL DEFAULT ''\n" +
                        ",\"MASTATUS\" CHAR(2) NOT NULL DEFAULT ''\n" +
                        ",\"PERSONALNUMMER\" CHAR(8) NOT NULL DEFAULT ''\n" +
                        ",\"FAMNAME\" VARCHAR(128) NOT NULL DEFAULT ''\n" +
                        ",\"VORNAME\" VARCHAR(65) NOT NULL DEFAULT ''\n" +
                        ",\"AKADGRAD\" CHAR(4) NOT NULL DEFAULT ''\n" +
                        ",\"GESCHLECHT\" CHAR(1) NOT NULL DEFAULT ''\n" +
                        ",\"EINTRDAT\" DATE NOT NULL\n" +
                        ",\"AUSTRDAT\" DATE NOT NULL\n" +
                        ",\"MEMOID\" CHAR(8) NOT NULL DEFAULT ''\n" +
                        ",\"SPRACHE\" CHAR(3) NOT NULL DEFAULT ''\n" +
                        ",\"BUERO\" CHAR(10) NOT NULL DEFAULT ''\n" +
                        ",\"EMAIL\" VARCHAR(100) NOT NULL DEFAULT ''\n" +
                        ",\"MAKURZKOM\" VARCHAR(65) NOT NULL DEFAULT ''\n" +
                        ",\"MABEMERKUNG\" VARCHAR(254) NOT NULL DEFAULT ''\n" +
                        ",\"NAMZUSATZ\" CHAR(6) NOT NULL DEFAULT ''\n" +
                        ",\"ANRTITEL\" CHAR(2) NOT NULL DEFAULT ''\n" +
                        ",\"ZWNAME\" VARCHAR(128) NOT NULL DEFAULT ''\n" +
                        ",\"MAFUNKTION\" CHAR(3) NOT NULL DEFAULT ''\n" +
                        ",\"BESCHAEFTGRAD\" DECIMAL(3,0) NOT NULL DEFAULT 0.0\n" +
                        ",\"ADELSBEZ\" CHAR(3) NOT NULL DEFAULT ''\n" +
                        ",\"ADELSTITEL\" CHAR(3) NOT NULL DEFAULT ''\n" +
                        ",PRIMARY KEY(\"MA#\"))")
                .withIndex("create index XPFUSERIDMA on \"TMITARBEITER_DEF\" (\"USERID\", \"MA#\")")
                .create();

        new TableCreator(conn)
                .withCreate("CREATE TABLE \"TMAPOS\" (\n" +
                        "\"MAP#\" CHAR(36) NOT NULL\n" +
                        ",\"MA#\" CHAR(36) NOT NULL\n" +
                        ",\"STLOK#\" CHAR(36) NOT NULL\n" +
                        ",\"TELDW\" CHAR(5) NOT NULL DEFAULT ''\n" +
                        ",\"MAPOSART\" CHAR(1) NOT NULL DEFAULT ''\n" +
                        ",\"GILTAB\" DATE NOT NULL\n" +
                        ",\"GILTBIS\" DATE NOT NULL\n" +
                        ",\"FAXNR\" CHAR(30) NOT NULL DEFAULT ''\n" +
                        ",\"TELNR_SPEZIAL\" CHAR(20) NOT NULL DEFAULT ''\n" +
                        ",\"TELDW_SPEZIAL\" CHAR(5) NOT NULL DEFAULT ''\n" +
                        ",\"IND_STELLENBEZ\" VARCHAR(65) NOT NULL DEFAULT ''\n" +
                        ",\"KZBERECHT_OBJ\" CHAR(1) NOT NULL DEFAULT ''\n" +
                        ",\"MOBILTELNR\" VARCHAR(50) NOT NULL DEFAULT ''\n" +
                        ",PRIMARY KEY(\"MAP#\"))")
                .withIndex("create index XFKMAPMA on \"TMAPOS\" (\"MA#\")")
                .create();

        new TableCreator(conn)
                .withCreate("CREATE TABLE \"TSTELLENLOK\" (\n" +
                        "\"STLOK#\" CHAR(36) NOT NULL\n" +
                        ",\"ST#\" CHAR(36) NOT NULL\n" +
                        ",\"LOKATION#\" CHAR(36) NOT NULL\n" +
                        ",PRIMARY KEY(\"STLOK#\"))")
                .create();

        new TableCreator(conn)
                .withCreate("CREATE TABLE \"TSTELLE\" (\n" +
                        "\"ST#\" CHAR(36) NOT NULL\n" +
                        ",\"GILTAB\" DATE NOT NULL\n" +
                        ",\"GILTBIS\" DATE NOT NULL\n" +
                        ",\"STELLEKURZBEZ\" CHAR(8) NOT NULL DEFAULT ''\n" +
                        ",\"STELLEART\" CHAR(1) NOT NULL DEFAULT ''\n" +
                        ",\"FKTSCHL\" CHAR(4) NOT NULL DEFAULT ''\n" +
                        ",\"STELLEKBEZ_ANZ\" CHAR(8) NOT NULL DEFAULT ''\n" +
                        ",\"SAP_FSCHL\" CHAR(4) NOT NULL DEFAULT ''\n" +
                        ",\"OE#\" CHAR(36) NOT NULL\n" +
                        ",\"UEB_ST#\" CHAR(36)\n" +
                        ",\"RGP#\" CHAR(36)\n" +
                        ",\"STELLENBEZ\" VARCHAR(65) NOT NULL DEFAULT ''\n" +
                        ",\"ANWENDGR\" CHAR(2) NOT NULL DEFAULT ''\n" +
                        ",PRIMARY KEY(\"ST#\"))")
                .create();

        new TableCreator(conn)
                .withCreate("CREATE TABLE \"TLOKATION\" (\n" +
                        "\"LOKATION#\" CHAR(36) NOT NULL\n" +
                        ",\"LOKNR\" CHAR(5) NOT NULL DEFAULT ''\n" +
                        ",\"LOKKURZNAME\" CHAR(8) NOT NULL DEFAULT ''\n" +
                        ",\"LOKKURZNAME2\" CHAR(16) NOT NULL DEFAULT ''\n" +
                        ",\"LOKTECHNAME\" CHAR(10) NOT NULL DEFAULT ''\n" +
                        ",\"LOKTYP\" CHAR(1) NOT NULL DEFAULT ''\n" +
                        ",\"KZLOKWERTE\" CHAR(1) NOT NULL DEFAULT ''\n" +
                        ",\"STRUKTEBENE\" CHAR(3) NOT NULL DEFAULT ''\n" +
                        ",\"GILTAB\" DATE NOT NULL\n" +
                        ",\"GILTBIS\" DATE NOT NULL\n" +
                        ",\"UEB_LOKATION#\" CHAR(36)\n" +
                        ",\"LOKNAME\" VARCHAR(65) NOT NULL DEFAULT ''\n" +
                        ",PRIMARY KEY(\"LOKATION#\"))")
                .withIndex("create index XPFLOKKURZNAME on \"TLOKATION\" (\"LOKKURZNAME\", \"LOKATION#\")")
                .create();

        conn.createStatement().executeQuery(format(
                "CALL SYSCS_UTIL.FAKE_TABLE_STATISTICS('%s', 'TGSCHFAKT', 5822681, 124, 2)", schemaName));
        conn.createStatement().executeQuery(format(
                "call syscs_util.fake_column_statistics('%s', 'TGSCHFAKT', 'GF#', 0, 1)", schemaName));
        conn.createStatement().executeQuery(format(
                "call syscs_util.fake_column_statistics('%s', 'TGSCHFAKT', 'USERID', 0, 1)", schemaName));

        conn.createStatement().executeQuery(format(
                "CALL SYSCS_UTIL.FAKE_TABLE_STATISTICS('%s', 'TMITARBEITER_DEF', 34609, 189, 1)", schemaName));
        conn.createStatement().executeQuery(format(
                "call syscs_util.fake_column_statistics('%s', 'TMITARBEITER_DEF', 'MA#', 0, 1)", schemaName));
        conn.createStatement().executeQuery(format(
                "call syscs_util.fake_column_statistics('%s', 'TMITARBEITER_DEF', 'USERID', 0, 1)", schemaName));

        conn.createStatement().executeQuery(format(
                "CALL SYSCS_UTIL.FAKE_TABLE_STATISTICS('%s', 'TMAPOS', 85085, 178, 1)", schemaName));
        conn.createStatement().executeQuery(format(
                "call syscs_util.fake_column_statistics('%s', 'TMAPOS', 'MA#', 0, 1)", schemaName));
        conn.createStatement().executeQuery(format(
                "call syscs_util.fake_column_statistics('%s', 'TMAPOS', 'STLOK#', 0, 1)", schemaName));

        conn.createStatement().executeQuery(format(
                "CALL SYSCS_UTIL.FAKE_TABLE_STATISTICS('%s', 'TMAPOS', 85085, 178, 1)", schemaName));
        conn.createStatement().executeQuery(format(
                "call syscs_util.fake_column_statistics('%s', 'TMAPOS', 'MA#', 0, 1)", schemaName));
        conn.createStatement().executeQuery(format(
                "call syscs_util.fake_column_statistics('%s', 'TMAPOS', 'STLOK#', 0, 1)", schemaName));

        conn.createStatement().executeQuery(format(
                "CALL SYSCS_UTIL.FAKE_TABLE_STATISTICS('%s', 'TSTELLENLOK', 20991, 108, 1)", schemaName));
        conn.createStatement().executeQuery(format(
                "call syscs_util.fake_column_statistics('%s', 'TSTELLENLOK', 'STLOK#', 0, 1)", schemaName));
        conn.createStatement().executeQuery(format(
                "call syscs_util.fake_column_statistics('%s', 'TSTELLENLOK', 'LOKATION#', 0, 1)", schemaName));

        conn.createStatement().executeQuery(format(
                "CALL SYSCS_UTIL.FAKE_TABLE_STATISTICS('%s', 'TSTELLE', 800, 199, 1)", schemaName));
        conn.createStatement().executeQuery(format(
                "call syscs_util.fake_column_statistics('%s', 'TSTELLE', 'ST#', 0, 1)", schemaName));

        conn.createStatement().executeQuery(format(
                "CALL SYSCS_UTIL.FAKE_TABLE_STATISTICS('%s', 'TLOKATION', 20024, 156, 1)", schemaName));
        conn.createStatement().executeQuery(format(
                "call syscs_util.fake_column_statistics('%s', 'TLOKATION', 'LOKATION#', 0, 1)", schemaName));

        conn.commit();
    }

    @BeforeClass
    public static void createDataSet() throws Exception {
        createData(spliceClassWatcher.getOrCreateConnection(), spliceSchemaWatcher.toString());
    }

    @Test
    public void testResetJumpModeForNextRound() throws Exception {
        String sqlText = "explain SELECT GA.LOGDAT,GA.USERID,ST.STELLEKURZBEZ \n" +
                "FROM\n" +
                "     TGSCHFAKT GA\n" +
                "   , TMITARBEITER_DEF MD\n" +
                "   , TMAPOS MP\n" +
                "   , TSTELLENLOK SL\n" +
                "   , TSTELLE ST\n" +
                "   , TLOKATION L\n" +
                "WHERE GF#='0c21cd04-9277-11e0-aff0-00247e126b6c'\n" +
                "  AND GA.USERID=MD.USERID\n" +
                "  AND MD.MA#=MP.MA#\n" +
                "  AND MP.MAPOSART='H'\n" +
                "  AND MP.GILTBIS=(\n" +
                "        SELECT MAX(GILTBIS)\n" +
                "        FROM TMAPOS \n" +
                "        WHERE MA#=MP.MA#\n" +
                "          AND MAPOSART=MP.MAPOSART\n" +
                "      )\n" +
                "  AND MP.STLOK#=SL.STLOK#\n" +
                "  AND SL.ST#=ST.ST#\n" +
                "  AND SL.LOKATION#=L.LOKATION#\n" +
                "  AND L.LOKKURZNAME NOT LIKE 'TCCCSM%' \n" +
                "  AND (L.LOKKURZNAME='SCCCC' OR L.LOKKURZNAME LIKE 'TCCC%' OR L.LOKKURZNAME LIKE 'SL2%' OR 0<>0)\n" +
                "  AND UEBERNAHMEZEIT>='2011-06-09 11:04:13.423203'\n" +
                "  AND UEBERNAHMEZEIT<'2020-04-13 20:11:45.069000'\n" +
                "ORDER BY UEBERNAHMEZEIT DESC WITH UR";

        /* expected join order (I=IndexScan, LK=IndexLookup):
                                      nestedloop
                                      /        \
                                  broadcast    Group - TMAPOS (I, LK)
                                  /       \
                              broadcast   TLOKATION (I)
                              /       \
                         nestedloop   TSTELLE
                         /        \
                    nestedloop    TSTELLENLOK
                    /        \
               nestedloop    TMAPOS (I, LK)
               /        \
           TGSCHFAKT    TMITARBEITER_DEF
            (I, LK)            (I)

           The plan above is obtained with real data. Since this test only mock statistics, it could be
           different on join strategies or access paths. However, the join order must be the same.
        */

        rowContainsQuery(new int[]{6,9,12,13,14,16,17,18,19,20,21,23,24,25,27,28,29}, sqlText, methodWatcher,
                new String[] {"NestedLoopJoin"},                                                 // 6
                new String[] {"GroupBy"},                                                        // 9
                new String[] {"IndexLookup", "outputRows=1"},                                    // 12
                new String[] {"IndexScan[XFKMAPMA", "outputRows=1"},                             // 13
                new String[] {"NestedLoopJoin"},                                                 // 14
                new String[] {"TableScan[TLOKATION", "scannedRows=1,outputRows=1"},              // 16
                new String[] {"BroadcastJoin"},                                                  // 17
                new String[] {"TableScan[TSTELLE", "scannedRows=800,outputRows=800"},            // 18
                new String[] {"NestedLoopJoin"},                                                 // 19
                new String[] {"TableScan[TSTELLENLOK", "scannedRows=1,outputRows=1"},            // 20
                new String[] {"NestedLoopJoin"},                                                 // 21
                new String[] {"IndexLookup"},                                                    // 23
                new String[] {"IndexScan[XFKMAPMA", "scannedRows=1,outputRows=1"},               // 24
                new String[] {"MergeSortJoin"},                                                  // 25
                new String[] {"IndexScan[XPFUSERIDMA", "scannedRows=34609,outputRows=34609"},    // 27
                new String[] {"IndexLookup"},                                                    // 28
                new String[] {"IndexScan[XFKGAGF", "scannedRows=1,outputRows=1"}                 // 29
                );
    }
}
