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

package com.splicemachine.db.impl.sql.compile;


import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.homeless.TestUtils;
import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import splice.com.google.common.collect.Lists;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test predicate simplification.
 * First pass of predicate simplification involves only the transformation
 * of a predicate 'p', OR'ed or AND'ed with boolean TRUE and FALSE:
 * 	(p OR FALSE)  ==> (p)
 * 	(p OR TRUE)   ==> TRUE
 * 	(p AND TRUE)  ==> (p)
 *  (p AND FALSE) ==> (FALSE)
 */
@RunWith(Parameterized.class)
public class PredicateSimplificationIT  extends SpliceUnitTest {
    
    private Boolean useSpark;
    
    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        Collection<Object[]> params = Lists.newArrayListWithCapacity(2);
        params.add(new Object[]{true});
        params.add(new Object[]{false});
        return params;
    }
    private static final String SCHEMA = PredicateSimplificationIT.class.getSimpleName();

    @ClassRule
    public static SpliceSchemaWatcher schemaWatcher = new SpliceSchemaWatcher(SCHEMA);

    @ClassRule
    public static SpliceWatcher classWatcher = new SpliceWatcher(SCHEMA);

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(SCHEMA);

    @BeforeClass
    public static void createSharedTables() throws Exception {
        TestUtils.executeSqlFile(classWatcher.getOrCreateConnection(), "subquery/PredSimplTestTables.sql", "");
    }

    @AfterClass
    public static void dropUDF() throws Exception {
        try {
            TestUtils.executeSqlFile(classWatcher.getOrCreateConnection(), "subquery/PredSimplTestCleanup.sql", "");
        }
        catch (Exception e) {
            // Don't error out if the UDF we want to drop does not exist.
        }
    }

    public PredicateSimplificationIT(Boolean useSpark) {
        this.useSpark = useSpark;
    }


    private void testQuery(String sqlText, String expected) throws Exception {
        ResultSet rs = null;
        try {
            rs = methodWatcher.executeQuery(sqlText);
            assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        }
        finally {
            if (rs != null)
                rs.close();
        }
    }

    private void testPreparedQuery(String sqlText, String expected, List<Integer> paramList) throws Exception {
        ResultSet rs = null;
        try {
            PreparedStatement ps = methodWatcher.prepareStatement(sqlText);
            int i = 1;
            for (int param : paramList) {
                ps.setInt(i++, param);
            }

            rs = ps.executeQuery();
            assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        }
        finally {
            if (rs != null)
                rs.close();
        }
    }
    private void testParameterizedExplainContains(String sqlText,
                                     List<String> containedStrings,
                                     List<String> notContainedStrings,
                                     List<Integer> paramList) throws Exception {
        ResultSet rs = null;
        try {
            String explainQuery = "explain " + sqlText;
            PreparedStatement ps = methodWatcher.prepareStatement(explainQuery);
            int i = 1;
            for (int param : paramList) {
                ps.setInt(i++, param);
            }
            rs = ps.executeQuery();
            String explainText = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
            if (containedStrings != null)
                for (String containedString : containedStrings) {
                    assertTrue(format("\n" + explainQuery + "\n\n" + "Expected to contain: %s\n", containedString),
                        explainText.contains(containedString));
                }
            if (notContainedStrings != null)
                for (String notContainedString : notContainedStrings) {
                    assertTrue(format("\n" + explainQuery + "\n\n" + "Expected not to contain: %s\n", notContainedString),
                        !explainText.contains(notContainedString));
                }
        }
        finally {
            if (rs != null)
                rs.close();
        }
    }

    private void testExplainContains(String sqlText,
                                     List<String> containedStrings,
                                     List<String> notContainedStrings) throws Exception {
        ResultSet rs = null;
        try {
            String explainQuery = "explain " + sqlText;
            rs = methodWatcher.executeQuery(explainQuery);
            String explainText = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
            if (containedStrings != null)
            for (String containedString : containedStrings) {
                assertTrue(format("\n" + explainQuery + "\n\n" + "Expected to contain: %s\n", containedString),
                    explainText.contains(containedString));
            }
            if (notContainedStrings != null)
            for (String notContainedString : notContainedStrings) {
                assertTrue(format("\n" + explainQuery + "\n\n" + "Expected not to contain: %s\n", notContainedString),
                    !explainText.contains(notContainedString));
            }
        }
        finally {
            if (rs != null)
                rs.close();
        }
    }


    @Test
    public void testSingleTableScan() throws Exception {
        String expected =
            "1 |\n" +
                "----\n" +
                " 1 |\n" +
                " 1 |\n" +
                " 1 |\n" +
                " 1 |\n" +
                " 1 |\n" +
                " 1 |";

        String query = format("select 1 from A --SPLICE-PROPERTIES useSpark=%s\n" +
            "where (a1 in (1,2,3) or false) or (a1 in (4,5,6) and (true or false)) ", useSpark);

        List<String> containedStrings = Arrays.asList("MultiProbeIndexScan", "(A1[0:1] IN (1,2,3,4,5,6)");
        List<String> notContainedStrings = null;
        testQuery(query, expected);
        testExplainContains(query, containedStrings, notContainedStrings);

        query = format("select 1 from A --SPLICE-PROPERTIES useSpark=%s\n" +
            "where (a1 in (1,2,3) and false) or (a1 in (4,5,6) and (true or false and 5=5 or 4=5)) ", useSpark);

        expected =
            "1 |\n" +
                "----\n" +
                " 1 |\n" +
                " 1 |\n" +
                " 1 |";

        containedStrings = Arrays.asList("MultiProbeIndexScan", "(A1[0:1] IN (4,5,6)");
        testQuery(query, expected);
        testExplainContains(query, containedStrings, notContainedStrings);

        query = format("select 1 from A --SPLICE-PROPERTIES useSpark=%s\n" +
            "where (a1 in (1,2,3) and (a2 = 3 and (a3 =5 and (false or (true and (false and true))) or (a1 in (4,5,6) and (false or true and 5=4 or 5=8))))) ", useSpark);

        expected =
            "";

        containedStrings = Arrays.asList("Values(");
        notContainedStrings = Arrays.asList("TableScan");
        testQuery(query, expected);
        testExplainContains(query, containedStrings, notContainedStrings);

        query = format("select 1 from A --SPLICE-PROPERTIES useSpark=%s\n" +
            "where (a1 in (1,2,3) and a2 in (10,20,30) and a3 in (100,200,300) and false) ", useSpark);

        testQuery(query, expected);
        testExplainContains(query, containedStrings, notContainedStrings);

        query = format("select 1 from A --SPLICE-PROPERTIES useSpark=%s\n" +
            "where (false and a1 in (1,2,3) and a2 in (10,20,30) and a3 in (100,200,300)) ", useSpark);


        testQuery(query, expected);
        testExplainContains(query, containedStrings, notContainedStrings);

        query = format("select 1 from A --SPLICE-PROPERTIES useSpark=%s\n" +
            "where (a1 in (1,2,3) and a2 in (10,20,30) and a3 in (100,200,300) or true) ", useSpark);

        expected =
            "1 |\n" +
                "----\n" +
                " 1 |\n" +
                " 1 |\n" +
                " 1 |\n" +
                " 1 |\n" +
                " 1 |\n" +
                " 1 |\n" +
                " 1 |\n" +
                " 1 |\n" +
                " 1 |\n" +
                " 1 |\n" +
                " 1 |\n" +
                " 1 |\n" +
                " 1 |\n" +
                " 1 |";

        containedStrings = Arrays.asList("");
        notContainedStrings = Arrays.asList("MultiProbeIndexScan", "preds");
        testQuery(query, expected);
        testExplainContains(query, containedStrings, notContainedStrings);

        query = format("select 1 from A --SPLICE-PROPERTIES useSpark=%s\n" +
            "where (true or a1 in (1,2,3) and a2 in (10,20,30) and a3 in (100,200,300)) ", useSpark);

        testQuery(query, expected);
        testExplainContains(query, containedStrings, notContainedStrings);

        query = format("select 1 from A --SPLICE-PROPERTIES useSpark=%s\n" +
            "where (a1 in (4,5,6) or (false and true)) and a2 in (40,50,60) and (true or false) ", useSpark);

        expected =
            "1 |\n" +
                "----\n" +
                " 1 |\n" +
                " 1 |\n" +
                " 1 |";

        if (useSpark)
            Arrays.asList("MultiProbeIndexScan", "[(A1[0:1] IN (4,5,6))]");
        else
            containedStrings = Arrays.asList("MultiProbeIndexScan", "[((A1[0:1],A2[0:2]) IN ((4,40),(4,50),(4,60),(5,40),(5,50),(5,60),(6,40),(6,50),(6,60)))]");
        notContainedStrings = null;
        testQuery(query, expected);
        testExplainContains(query, containedStrings, notContainedStrings);
    }

    @Test
    public void testJoin() throws Exception {
        String expected =
            "1 |\n" +
                    "----\n" +
                    " 1 |\n" +
                    " 1 |\n" +
                    " 1 |\n" +
                    " 1 |\n" +
                    " 1 |\n" +
                    " 1 |\n" +
                    " 1 |\n" +
                    " 1 |";

        // Simplifying predicates should allow Merge join to be picked.
        String query = format("select 1 from A, B --SPLICE-PROPERTIES joinStrategy=MERGE, useSpark=%s\n" +
            "where a1 = b1 or false ", useSpark);

        List<String> containedStrings = Arrays.asList("IndexScan");
        List<String> notContainedStrings = null;
        testQuery(query, expected);
        testExplainContains(query, containedStrings, notContainedStrings);

        query = format("select count(*) from A, B --SPLICE-PROPERTIES useSpark=%s\n" +
            "where a1 = b1 or true ", useSpark);
        expected =
            "1  |\n" +
                "-----\n" +
                "168 |";

        containedStrings = Arrays.asList("");
        notContainedStrings = Arrays.asList("preds");;
        testQuery(query, expected);
        testExplainContains(query, containedStrings, notContainedStrings);

        query = format("select count(*) from A --SPLICE-PROPERTIES useSpark=%s\n" +
            "where a1 in (select b1 from B) and false ", useSpark);
        expected =
            "1 |\n" +
                "----\n" +
                " 0 |";

        containedStrings = Arrays.asList("Values(");
        notContainedStrings = Arrays.asList("Join");
        testQuery(query, expected);
        testExplainContains(query, containedStrings, notContainedStrings);

        query = format("select count(*) from A --SPLICE-PROPERTIES useSpark=%s\n" +
            "where a1 in (select b1 from B) and a1 in (select c1 from C) and " +
            "a1 in (select d1 from D) and false ", useSpark);
        expected =
            "1 |\n" +
                "----\n" +
                " 0 |";

        testQuery(query, expected);
        testExplainContains(query, containedStrings, notContainedStrings);

        query = format("select count(*) from A --SPLICE-PROPERTIES useSpark=%s\n" +
            "where false and a1 in (select b1 from B) and a1 in (select c1 from C) and " +
            "a1 in (select d1 from D) ", useSpark);
        expected =
            "1 |\n" +
                "----\n" +
                " 0 |";

        testQuery(query, expected);
        testExplainContains(query, containedStrings, notContainedStrings);

        query = format("select count(*) from A --SPLICE-PROPERTIES useSpark=%s\n" +
            "where a1 in (select b1 from B) and a1 in (select c1 from C) and " +
            "a1 in (select d1 from D) or true ", useSpark);
        expected =
            "1 |\n" +
                "----\n" +
                "14 |";
        containedStrings = Arrays.asList("");
        notContainedStrings = Arrays.asList("preds");;
        testQuery(query, expected);
        testExplainContains(query, containedStrings, notContainedStrings);

        query = format("select count(*) from A --SPLICE-PROPERTIES useSpark=%s\n" +
            "where true or a1 in (select b1 from B) and a1 in (select c1 from C) and " +
            "a1 in (select d1 from D) ", useSpark);

        testQuery(query, expected);
        testExplainContains(query, containedStrings, notContainedStrings);
    }


    @Test
    public void testParametersAndSubqueries() throws Exception {
        String expected =
            "1 |\n" +
                "----\n" +
                " 1 |\n" +
                " 1 |\n" +
                " 1 |\n" +
                " 1 |\n" +
                " 1 |\n" +
                " 1 |";
        String query = format("select 1 from A --SPLICE-PROPERTIES useSpark=%s\n" +
            "where (a1 in (?,?,?) or false) or (a1 in (?,?,?) and (true or false)) ", useSpark);

        List<String> containedStrings = Arrays.asList("MultiProbeIndexScan", "(A1[0:1] IN (dataTypeServices: INTEGER ,dataTypeServices: INTEGER ,dataTypeServices: INTEGER ,dataTypeServices: INTEGER ,dataTypeServices: INTEGER ,dataTypeServices: INTEGER )");
        List<String> notContainedStrings = null;
        List<Integer> paramList = Arrays.asList(1,2,3,4,5,6);
        testPreparedQuery(query, expected, paramList);
        testParameterizedExplainContains(query, containedStrings, notContainedStrings, paramList);

        containedStrings = Arrays.asList("Values(");
        notContainedStrings = Arrays.asList("Join");
        paramList = Arrays.asList(0,0,0);

        query = format("select count(*) from A --SPLICE-PROPERTIES useSpark=%s\n" +
            "where a1 in (select b1 from B where b2 > ?) and a1 in (select c1 from C where c2 > ?) and " +
            "a1 in (select d1 from D where d2 > ?) and false ", useSpark);
        expected =
            "1 |\n" +
                "----\n" +
                " 0 |";

        testPreparedQuery(query, expected, paramList);
        testParameterizedExplainContains(query, containedStrings, notContainedStrings, paramList);

        query = format("select count(*) from A --SPLICE-PROPERTIES useSpark=%s\n" +
            "where false and a1 in (select b1 from B where b2 > ?) and a1 in (select c1 from C where c2 > ?) and " +
            "a1 in (select d1 from D where d2 > ?) ", useSpark);

        testPreparedQuery(query, expected, paramList);
        testParameterizedExplainContains(query, containedStrings, notContainedStrings, paramList);

        paramList = Arrays.asList(0);
        query = format("select count(*) from A --SPLICE-PROPERTIES useSpark=%s\n" +
            "where true or TO_DEGREES(a1) > ?", useSpark);
        expected =
            "1 |\n" +
                "----\n" +
                "14 |";

        containedStrings = Arrays.asList("");
        notContainedStrings = Arrays.asList("Values", "preds");
        testPreparedQuery(query, expected, paramList);
        testParameterizedExplainContains(query, containedStrings, notContainedStrings, paramList);

        query = format("select count(*) from A --SPLICE-PROPERTIES useSpark=%s\n" +
            "where false and TO_DEGREES(a1) > ?", useSpark);
        expected =
            "1 |\n" +
                "----\n" +
                " 0 |";

        containedStrings = Arrays.asList("Values(");
        notContainedStrings = Arrays.asList("Scan");
        testPreparedQuery(query, expected, paramList);
        testParameterizedExplainContains(query, containedStrings, notContainedStrings, paramList);

        query = format("select count(*) from A --SPLICE-PROPERTIES useSpark=%s\n" +
            "where true or TO_DEGREES(?) > a1", useSpark);
        expected =
            "1 |\n" +
                "----\n" +
                "14 |";

        containedStrings = Arrays.asList("");
        notContainedStrings = Arrays.asList("Values", "preds");
        testPreparedQuery(query, expected, paramList);
        testParameterizedExplainContains(query, containedStrings, notContainedStrings, paramList);

        query = format("select count(*) from A --SPLICE-PROPERTIES useSpark=%s\n" +
            "where false and TO_DEGREES(?) > a1", useSpark);
        expected =
            "1 |\n" +
                "----\n" +
                " 0 |";

        containedStrings = Arrays.asList("Values(");
        notContainedStrings = Arrays.asList("Scan");
        testPreparedQuery(query, expected, paramList);
        testParameterizedExplainContains(query, containedStrings, notContainedStrings, paramList);

        query = format("select count(*) from A --SPLICE-PROPERTIES useSpark=%s\n" +
            "where a1 in (select b1 from B where b2 = a2) and a1 in (select c1 from C where c2=a2) and " +
            "a1 in (select d1 from D where d2 = a2) or true or a1=?", useSpark);
        expected =
            "1 |\n" +
                "----\n" +
                "14 |";

        containedStrings = Arrays.asList("");
        notContainedStrings = Arrays.asList("Values", "preds");
        testPreparedQuery(query, expected, paramList);
        testParameterizedExplainContains(query, containedStrings, notContainedStrings, paramList);

        query = format("select count(*) from A --SPLICE-PROPERTIES useSpark=%s, index=pred_simpl_a1_a2\n" +
            "where a1 in (select b1 from B --SPLICE-PROPERTIES index=pred_simpl_b1\n" +
            "where b2 = a2 and b1 in (?,?,?) and (1=1 or (3=3 and (1=2 or 6=6)))) and a1 in (select c1 from C where c2=a2) and " +
            "a1 in (select d1 from D where d2 = a2) or false and a1>?", useSpark);
        expected =
            "1 |\n" +
                "----\n" +
                " 1 |";

        paramList = Arrays.asList(4,5,6,0);
        containedStrings = Arrays.asList("IndexLookup");
        notContainedStrings = Arrays.asList("Values");
        testPreparedQuery(query, expected, paramList);
        testParameterizedExplainContains(query, containedStrings, notContainedStrings, paramList);
    }

    @Test
    public void test_DB_8081() throws Exception {
        String expected =
        "NAME  |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               DESCRIPTION                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                KEYWORDS                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |\n" +
        "-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------\n" +
        "Harry |Harry works in the Redundancy Automation Division of the Materials Blasting Laboratory in the National Cattle Acceleration Project of lower Michigan.  His job is to document the trajectory of cattle and correlate the loft and acceleration versus the quality of materials used in the trebuchet.  He served ten years as the vice-president in charge of marketing in the now defunct milk trust of the Pennsylvania Coalition of All Things Bovine.  Prior to that he established himself as a world-class graffiti artist and source of all good bits related to channel dredging in poor weather.  He is author of over ten thousand paperback novels, including such titles as \"How Many Pumpkins will Fit on the Head of a Pin,\" \"A Whole Bunch of Useless Things that you Don't Want to Know,\" and \"How to Lift Heavy Things Over your Head without Hurting Yourself or Dropping Them.\"  He attends ANSI and ISO standards meetings in his copious free time and funds the development of test suites with his pocket change. |aardvark albatross nutmeg redundancy automation materials blasting cattle acceleration trebuchet catapult loft coffee java sendmail SMTP FTP HTTP censorship expletive senility extortion distortion conformity conformance nachos chicks goslings ducklings honk quack melatonin tie noose circulation column default ionic doric chlorine guanine Guam invasions rubicon helmet plastics recycle HDPE nylon ceramics plumbing parachute zeppelin carbon hydrogen vinegar sludge asphalt adhesives tensile magnetic Ellesmere Greenland Knud Rasmussen precession navigation positioning orbit altitude resistance radiation levitation yoga demiurge election violence collapsed fusion cryogenics gravity sincerity idiocy budget accounting auditing titanium torque pressure fragile hernia muffler cartilage graphics deblurring headache eyestrain interlace bandwidth resolution determination steroids barrel oak wine ferment yeast brewing bock siphon clarity impurities SQL RBAC data warehouse security integrity feedback |";

        String query = format("SELECT * FROM CONTACTS --SPLICE-PROPERTIES useSpark=%s\n" +
        "WHERE DESCRIPTION = 'Harry works in the Redundancy Automation Division of the ' || 'Materials ' || 'Blasting Laboratory in the National Cattle Acceleration ' || 'Project of ' || 'lower Michigan.  His job is to document the trajectory of ' || 'cattle and ' || 'correlate the loft and acceleration versus the quality of ' || 'materials ' || 'used in the trebuchet.  He served ten years as the ' || 'vice-president in ' || 'charge of marketing in the now defunct milk trust of the ' || 'Pennsylvania ' || 'Coalition of All Things Bovine.  Prior to that he ' || 'established himself ' || 'as a world-class graffiti artist and source of all good ' || 'bits related ' || 'to channel dredging in poor weather.  He is author of over ' || 'ten thousand ' || 'paperback novels, including such titles as \"How Many ' || 'Pumpkins will Fit ' || 'on the Head of a Pin,\" \"A Whole Bunch of Useless Things ' || 'that you Don''t ' || 'Want to Know,\" and \"How to Lift Heavy Things Over your ' || 'Head without ' || 'Hurting Yourself or Dropping Them.\"  He attends ANSI and ' || 'ISO standards ' || 'meetings in his copious free time and funds the development ' || 'of test ' || 'suites with his pocket change.' AND KEYWORDS = 'aardvark albatross nutmeg redundancy ' || 'automation materials blasting ' || 'cattle acceleration trebuchet catapult ' || 'loft coffee java sendmail SMTP ' || 'FTP HTTP censorship expletive senility ' || 'extortion distortion conformity ' || 'conformance nachos chicks goslings ' || 'ducklings honk quack melatonin tie ' || 'noose circulation column default ' || 'ionic doric chlorine guanine Guam ' || 'invasions rubicon helmet plastics ' || 'recycle HDPE nylon ceramics plumbing ' || 'parachute zeppelin carbon hydrogen ' || 'vinegar sludge asphalt adhesives ' || 'tensile magnetic Ellesmere Greenland ' || 'Knud Rasmussen precession ' || 'navigation positioning orbit altitude ' || 'resistance radiation levitation ' || 'yoga demiurge election violence ' || 'collapsed fusion cryogenics gravity ' || 'sincerity idiocy budget accounting ' || 'auditing titanium torque pressure ' || 'fragile hernia muffler cartilage ' || 'graphics deblurring headache eyestrain ' || 'interlace bandwidth resolution ' || 'determination steroids barrel oak wine ' || 'ferment yeast brewing bock siphon ' || 'clarity impurities SQL RBAC data ' || 'warehouse security integrity feedback'"
         , useSpark);

        testQuery(query, expected);
    }

}
