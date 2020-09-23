/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
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

package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceTableWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.homeless.TestUtils;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import java.io.File;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import static org.junit.Assert.assertNotNull;

/**
 *
 * Test class for validating S3 connectivity with test credentials.
 *
 */
@Ignore
public class S3IT extends SpliceUnitTest{
    public static final String LOCATION = "s3a://spliceintegrationtest/nation.tbl";
    public static File BADDIR;
    public static final String NATION_FULL_RESPONSE = "N_NATIONKEY |    N_NAME     | N_REGIONKEY |                                                    N_COMMENT                                                     |\n" +
            "---------------------------------------------------------------------------------------------------------------------------------------------------------------\n" +
            "      0      |    ALGERIA    |      0      |                               haggle. carefully final deposits detect slyly agai                                 |\n" +
            "      1      |   ARGENTINA   |      1      |                  al foxes promise slyly according to the regular accounts. bold requests alon                    |\n" +
            "     10      |     IRAN      |      4      |                                efully alongside of the slyly final dependencies.                                 |\n" +
            "     11      |     IRAQ      |      4      |                       nic deposits boost atop the quickly final requests? quickly regula                         |\n" +
            "     12      |     JAPAN     |      2      |                                      ously. final, express gifts cajole a                                        |\n" +
            "     13      |    JORDAN     |      4      |                             ic deposits are blithely about the carefully regular pa                              |\n" +
            "     14      |     KENYA     |      0      |          pending excuses haggle furiously deposits. pending, express pinto beans wake fluffily past t            |\n" +
            "     15      |    MOROCCO    |      0      |           rns. blithely bold courts among the closely regular packages use furiously bold platelets?             |\n" +
            "     16      |  MOZAMBIQUE   |      0      |                                  s. ironic, unusual asymptotes wake blithely r                                   |\n" +
            "     17      |     PERU      |      1      |   platelets. blithely pending dependencies use fluffily across the even pinto beans. carefully silent accoun     |\n" +
            "     18      |     CHINA     |      2      |           c dependencies. furiously express notornis sleep slyly regular accounts. ideas sleep. depos            |\n" +
            "     19      |    ROMANIA    |      3      | ular asymptotes are about the furious multipliers. express dependencies nag above the ironically ironic account  |\n" +
            "      2      |    BRAZIL     |      1      |   y alongside of the pending deposits. carefully special packages are about the ironic forges. slyly special     |\n" +
            "     20      | SAUDI ARABIA  |      4      |                 ts. silent requests haggle. closely express packages sleep across the blithely                   |\n" +
            "     21      |    VIETNAM    |      2      |                                  hely enticingly express accounts. even, final                                   |\n" +
            "     22      |    RUSSIA     |      3      |                 requests against the platelets use never according to the quickly regular pint                   |\n" +
            "     23      |UNITED KINGDOM |      3      |                          eans boost carefully special requests. accounts are. carefull                           |\n" +
            "     24      | UNITED STATES |      1      | y final packages. slow foxes cajole quickly. quickly silent platelets breach ironic accounts. unusual pinto be   |\n" +
            "      3      |    CANADA     |      1      |      eas hang ironic, silent packages. slyly regular packages are furiously over the tithes. fluffily bold       |\n" +
            "      4      |     EGYPT     |      4      |       y above the carefully unusual theodolites. final dugouts are quickly across the furiously regular d        |\n" +
            "      5      |   ETHIOPIA    |      0      |                                         ven packages wake quickly. regu                                          |\n" +
            "      6      |    FRANCE     |      3      |                                     refully final requests. regular, ironi                                       |\n" +
            "      7      |    GERMANY    |      3      |                           l platelets. regular accounts x-ray: unusual, regular acco                             |\n" +
            "      8      |     INDIA     |      2      |                        ss excuses cajole slyly across the packages. deposits print aroun                         |\n" +
            "      9      |   INDONESIA   |      2      |slyly express asymptotes. regular deposits haggle slyly. carefully ironic hockey players sleep blithely. carefull |";
    private static final String SCHEMA_NAME = S3IT.class.getSimpleName().toUpperCase();
    private static final SpliceWatcher spliceClassWatcher = new SpliceWatcher(SCHEMA_NAME);
    private static final SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(SCHEMA_NAME);
    private static final SpliceTableWatcher spliceTableWatcher = new SpliceTableWatcher("NATION2",SCHEMA_NAME,"( " +
            "N_NATIONKEY INTEGER NOT NULL, " +
            "N_NAME VARCHAR(25), " +
            "N_REGIONKEY INTEGER NOT NULL, " +
            "N_COMMENT VARCHAR(152))");
    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(SCHEMA_NAME);

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(spliceSchemaWatcher).
            around(spliceTableWatcher);

    @BeforeClass
    public static void beforeClass() {
        BADDIR = SpliceUnitTest.createBadLogDirectory(spliceSchemaWatcher.schemaName);
        assertNotNull(BADDIR);
    }

    @Test
    public void testReadFromS3File() throws Exception{
        methodWatcher.executeUpdate(String.format("CREATE EXTERNAL TABLE NATION (" +
                "                N_NATIONKEY INTEGER NOT NULL,\n" +
                "                N_NAME VARCHAR(25),\n" +
                "                N_REGIONKEY INTEGER NOT NULL,\n" +
                "                N_COMMENT VARCHAR(152)\n" +
                "        ) ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' " +
                "stored as %s location '%s'", "TEXTFILE",LOCATION));
        ResultSet rs = methodWatcher.executeQuery("select * from nation");
        Assert.assertEquals(NATION_FULL_RESPONSE, TestUtils.FormattedResult.ResultFactory.toString(rs));
    }


    @Test
    public void testImportFromS3() throws Exception {

            PreparedStatement ps = methodWatcher.prepareStatement(format("call SYSCS_UTIL.IMPORT_DATA(" +
                            "'%s'," +  // schema name
                            "'%s'," +  // table name
                            "null," +  // insert column list
                            "'%s'," +  // file path
                            "'|'," +   // column delimiter
                            "null," +  // character delimiter
                            "null," +  // timestamp format
                            "null," +  // date format
                            "null," +  // time format
                            "%d," +    // max bad records
                            "'%s'," +  // bad record dir
                            "null," +  // has one line records
                            "null)",   // char set
                    spliceSchemaWatcher.schemaName, "NATION2",
                    LOCATION,
                    0,
                    BADDIR.getCanonicalPath()));
            ps.execute();
        ResultSet rs = methodWatcher.executeQuery("select * from nation2");
        Assert.assertEquals(NATION_FULL_RESPONSE, TestUtils.FormattedResult.ResultFactory.toString(rs));
    }

    @Test
    public void testPinS3File() throws Exception{
        methodWatcher.executeUpdate(String.format("CREATE EXTERNAL TABLE NATION3 (" +
                "                N_NATIONKEY INTEGER NOT NULL,\n" +
                "                N_NAME VARCHAR(25),\n" +
                "                N_REGIONKEY INTEGER NOT NULL,\n" +
                "                N_COMMENT VARCHAR(152)\n" +
                "        ) ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' " +
                "stored as %s location '%s'", "TEXTFILE",LOCATION));
        ResultSet rs = methodWatcher.executeQuery("select * from nation3");
        Assert.assertEquals(NATION_FULL_RESPONSE, TestUtils.FormattedResult.ResultFactory.toString(rs));
        methodWatcher.executeUpdate("create pin table nation3");
        ResultSet rs2 = methodWatcher.executeQuery("select * from nation3 --splice-properties pin=true");
        Assert.assertEquals(NATION_FULL_RESPONSE, TestUtils.FormattedResult.ResultFactory.toString(rs2));
    }


}
