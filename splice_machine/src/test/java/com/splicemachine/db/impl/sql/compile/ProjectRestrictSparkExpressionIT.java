/*
 * Copyright (c) 2012 - 2019 Splice Machine, Inc.
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
import org.spark_project.guava.collect.Lists;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test SQL expressions, which may now be evaluated native SparkSQL,
 * and verify the results using Spark match non-Spark evaluation.
 */
@RunWith(Parameterized.class)
public class ProjectRestrictSparkExpressionIT  extends SpliceUnitTest {
    
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
        TestUtils.executeSqlFile(classWatcher.getOrCreateConnection(), "expressions/SparkExpressionTables.sql", "");
    }

    public ProjectRestrictSparkExpressionIT(Boolean useSpark) {
        this.useSpark = useSpark;
    }

    @Test
    public void testLike() throws Exception {
        String expected =
        "A |\n" +
        "----\n" +
        "a_ |";

        String query = format("select * from t1_like --SPLICE-PROPERTIES useSpark=%s\n" +
        "where a LIKE 'a=_' ESCAPE '='", useSpark);

        testQuery(query, expected, methodWatcher);

        query = format("select * from t1_like --SPLICE-PROPERTIES useSpark=%s\n" +
        "where a LIKE 'a%%' ", useSpark);

        expected =
        "A |\n" +
        "----\n" +
        " a |\n" +
        " a |\n" +
        "a_ |";

        testQuery(query, expected, methodWatcher);

        query = format("select * from t2_like --SPLICE-PROPERTIES useSpark=%s\n" +
        "WHERE REGEXP_LIKE(a, '^Ste.*') ", useSpark);

        expected =
        "A        |\n" +
        "-----------------\n" +
        "Stephen Tuvesco |\n" +
        " Steve Mossely  |\n" +
        " Steve Raster   |";

        testQuery(query, expected, methodWatcher);

    }

    @Test
    public void testBetween() throws Exception {
        String expected =
        "1                |\n" +
        "----------------------------------\n" +
        "0.024691357802469135780246913578 |\n" +
        "2.000000000000000000000000000000 |";

        String query = format("select tab1.a+tab2.a from t1 tab1, t1 tab2 --SPLICE-PROPERTIES useSpark=%s, joinStrategy=SORTMERGE\n" +
        "where tab1.c=tab2.c and tab1.a+tab2.a between 0 and 1000", useSpark);

        testQuery(query, expected, methodWatcher);

    }

    @Test
    public void testIn() throws Exception {
        String expected =
        "1                |\n" +
        "----------------------------------\n" +
        "0.024691357802469135780246913578 |\n" +
        "2.000000000000000000000000000000 |";

        String query = format("select tab1.a+tab2.a from t1 tab1, t1 tab2 --SPLICE-PROPERTIES useSpark=%s, joinStrategy=SORTMERGE\n" +
        "where tab1.c=tab2.c and tab1.a+tab2.a IN (0.024691357802469135780246913578, 2.0)", useSpark);

        testQuery(query, expected, methodWatcher);

    }

    @Test
    public void testFunctions() throws Exception {
        String expected[] = {
            "1  |\n" +
            "------\n" +
            "2001 |\n" +
            "2001 |",
            "1  |\n" +
            "------\n" +
            "2001 |\n" +
            "2001 |\n" +
            "2001 |",
            "1 |\n" +
            "----\n" +
            " 1 |\n" +
            " 1 |\n" +
            " 1 |",
            "1 |\n" +
            "----\n" +
            " 1 |\n" +
            " 1 |\n" +
            " 1 |",
            "1    |\n" +
            "---------\n" +
            "January |\n" +
            "January |\n" +
            "January |",
            "1 |\n" +
            "----\n" +
            " 1 |\n" +
            " 1 |\n" +
            " 1 |",
            "1 |\n" +
            "----\n" +
            " 1 |\n" +
            " 1 |\n" +
            " 1 |",
            "1   |\n" +
            "--------\n" +
            "Monday |\n" +
            "Monday |\n" +
            "Monday |",
            "1 |\n" +
            "----\n" +
            " 1 |\n" +
            " 1 |\n" +
            " 1 |",
            "1 |\n" +
            "----\n" +
            " 1 |\n" +
            " 1 |\n" +
            " 1 |",
            "1 |\n" +
            "----\n" +
            "12 |\n" +
            "12 |\n" +
            "12 |",
            "1 |\n" +
            "----\n" +
            " 0 |\n" +
            " 0 |\n" +
            " 0 |",
            "1    |\n" +
            "----------\n" +
            "0.123456 |\n" +
            "0.123456 |\n" +
            "0.123456 |",
            "1 |\n" +
            "----\n" +
            "17 |\n" +
            "51 |\n" +
            "52 |",
            "1  |\n" +
            "-----\n" +
            "-11 |\n" +
            " 3  |\n" +
            " 5  |",
            "1   |\n" +
            "--------\n" +
            "SPLICE |\n" +
            "SPLICE |\n" +
            "SPLICE |",
            "1  |\n" +
            "-----\n" +
            "ABC |\n" +
            "ABC |\n" +
            "ABC |",
            "1  |\n" +
            "-----\n" +
            "ABC |\n" +
            "ABC |\n" +
            "ABC |",
            "1     |\n" +
            "------------\n" +
            "2016-12-31 |\n" +
            "2016-12-31 |\n" +
            "2016-12-31 |",
            "1     |\n" +
            "------------\n" +
            "04/25/1990 |\n" +
            "12/25/0001 |\n" +
            "12/25/1988 |",
            "1 |\n" +
            "----\n" +
            "37 |",
            "1             |             2             |\n" +
            "--------------------------------------------------------\n" +
            "2001-01-01 12:00:00.123456 |2001-03-01 12:00:00.123456 |\n" +
            "2001-01-01 12:00:00.123456 |2001-03-01 12:00:00.123456 |\n" +
            "2001-01-01 12:00:00.123456 |2001-03-01 12:00:00.123456 |",
            "1 |\n" +
            "----\n" +
            " 1 |\n" +
            " 1 |\n" +
            " 1 |",
            "1           |\n" +
            "-----------------------\n" +
            "2015-11-12 19:02:43.0 |\n" +
            "2015-11-12 19:02:43.0 |\n" +
            "2015-11-12 19:02:43.0 |",
            "1          |\n" +
            "---------------------\n" +
            "0.08876575249514286 |\n" +
            "0.08876575249514286 |\n" +
            "0.08876575249514286 |",
            "1          |\n" +
            "---------------------\n" +
            "0.08923573660270123 |\n" +
            "0.08923573660270123 |\n" +
            "0.08923573660270123 |",
            "1          |\n" +
            "---------------------\n" +
            "-1849594.5501649815 |\n" +
            "-1849594.5501649815 |\n" +
            "-1849594.5501649815 |",
            "1  |\n" +
            "------\n" +
            "abcd |",
            "1  |\n" +
            "-----\n" +
            "132 |\n" +
            "132 |\n" +
            "132 |",
            "1          |\n" +
            "----------------------\n" +
            "-0.30452029344714265 |\n" +
            "0.012345992516353266 |\n" +
            " 1.1752011936438016  |",
            "1          |\n" +
            "----------------------\n" +
            "-0.29552020666133955 |\n" +
            "0.012345365290895811 |\n" +
            " 0.8414709848078965  |",
            "1      |\n" +
            "-------------\n" +
            "18000000000 |\n" +
            "18000000000 |\n" +
            "18000000000 |",
            "1 | 2 | 3 |  4  |\n" +
            "------------------\n" +
            " 1 |-1 | 0 |NULL |\n" +
            " 1 |-1 | 0 |NULL |\n" +
            " 1 |-1 | 0 |NULL |",
            "1   |\n" +
            "--------\n" +
            "SPLICE |\n" +
            "SPLICE |\n" +
            "SPLICE |",
            "1     |\n" +
            "------------\n" +
            "Space Case |\n" +
            "Space Case |\n" +
            "Space Case |",
            "1   |\n" +
            "-------\n" +
            "-23.5 |\n" +
            "-23.5 |\n" +
            "-23.5 |",
            "1      |\n" +
            "-------------\n" +
            "-hellohello |\n" +
            "     3      |\n" +
            "     5      |",
            "1                                |\n" +
            "-----------------------------------------------------------------\n" +
            "-11                           -11                           -11 |\n" +
            " 3                             3                             3  |\n" +
            " 5                             5                             5  |",
            "1   |\n" +
            "-------\n" +
            "false |\n" +
            "true  |\n" +
            "true  |",
            "1         |\n" +
            "--------------------\n" +
            "1.5707963267948966 |\n" +
            "1.5707963267948966 |\n" +
            "1.5707963267948966 |",
            "1        |\n" +
            "------------------\n" +
            "9.42477796076938 |\n" +
            "9.42477796076938 |",
            "1 |\n" +
            "----\n" +
            " 1 |\n" +
            " 3 |",
            "1  |     2     |\n" +
            "------------------\n" +
            "NULL |2001-01-01 |",
            "1     |\n" +
            "------------\n" +
            "2001-01-08 |\n" +
            "2001-01-08 |\n" +
            "2001-01-08 |",
            "1   |\n" +
            "-------\n" +
            "-25.0 |\n" +
            "-25.0 |\n" +
            "-25.0 |",
            "1     |\n" +
            "------------\n" +
            "2017-09-15 |\n" +
            "2017-09-15 |\n" +
            "2017-09-15 |",
            "1     |\n" +
            "------------\n" +
            "Space Case |",
            "1     |\n" +
            "------------\n" +
            "Space Case |",
            "1     |\n" +
            "-----------\n" +
            "pace Case |",
            "1 |\n" +
            "----\n" +
            "10 |",
            "1     |\n" +
            "------------\n" +
            "Space Case |",
            "1         |\n" +
            "-------------------\n" +
            "1.926342446625655 |",
            "1 |\n" +
            "----\n" +
            " 4 |",
            "1 |\n" +
            "----\n" +
            " 7 |",
            "1         |         2         |\n" +
            "----------------------------------------\n" +
            "4.4355674016019115 |4.4355674016019115 |",
            "1 |\n" +
            "----\n" +
            "10 |",
            "1 |\n" +
            "----\n" +
            "30 |\n" +
            "30 |\n" +
            "30 |",
            "1 |\n" +
            "----\n" +
            "30 |\n" +
            "30 |\n" +
            "30 |",
            "1 |\n" +
            "----\n" +
            " 8 |\n" +
            " 8 |\n" +
            " 8 |",
            "1        | 2 |\n" +
            "---------------------\n" +
            "this is a test. | a |",
            "1        | 2 |\n" +
            "---------------------\n" +
            "this is a test. | a |",
            "1     |\n" +
            "------------\n" +
            "2001-01-31 |",
            "1 | 2 |\n" +
            "--------\n" +
            " 1 | 1 |",
            "1 |\n" +
            "----\n" +
            " 6 |",
            "1        |\n" +
            "-----------------\n" +
            "This Is A Test. |",
            "1       |\n" +
            "----------------\n" +
            "This Is A Test |\n" +
            "This Is A Test |\n" +
            "This Is A Test |",
            "1    |\n" +
            "----------\n" +
            "0.123456 |\n" +
            "0.123456 |\n" +
            "0.123456 |",
            "1  |\n" +
            "-----\n" +
            "2.0 |\n" +
            "2.0 |\n" +
            "2.0 |",
            "1  |\n" +
            "-----\n" +
            "1.0 |\n" +
            "1.0 |\n" +
            "1.0 |",
            "1        |\n" +
            "------------------\n" +
            "3.43494186080076 |\n" +
            "3.43494186080076 |\n" +
            "3.43494186080076 |",
            "1  |\n" +
            "-----\n" +
            "5.0 |\n" +
            "5.0 |\n" +
            "5.0 |",
            "1         |\n" +
            "-------------------\n" +
            "60.00000000000001 |\n" +
            "60.00000000000001 |\n" +
            "60.00000000000001 |",
            "1          |\n" +
            "---------------------\n" +
            "-3.2327281437658275 |\n" +
            "0.6420926159343306  |\n" +
            " 80.99588546088395  |",
            "1  |\n" +
            "------\n" +
            "true |\n" +
            "true |\n" +
            "true |",
            "1          |\n" +
            "---------------------\n" +
            "2001-01-01 12:00:00 |\n" +
            "2001-01-01 12:00:00 |\n" +
            "2001-01-01 12:00:00 |",
            "1           |\n" +
            "-----------------------\n" +
            "2001-01-01 12:00:00.0 |\n" +
            "2001-01-01 12:00:00.0 |\n" +
            "2001-01-01 12:00:00.0 |",
            "1     |\n" +
            "------------\n" +
            "1972-02-28 |\n" +
            "1972-02-28 |\n" +
            "1972-02-28 |",
            "1         |\n" +
            "--------------------\n" +
            "1.0000762088617114 |\n" +
            "1.0453385141288605 |\n" +
            " 1.543080634815244 |",
            "1 |\n" +
            "----\n" +
            " 1 |\n" +
            " 4 |",
            "C          |\n" +
            "---------------------\n" +
            "2001-01-01 12:00:00 |",
            "1  |\n" +
            "------\n" +
            "11.1 |",

            "1           |\n" +
            "-----------------------\n" +
            "2001-01-01 12:00:00.0 |",
            "1          |\n" +
            "----------------------\n" +
            "        -0.3         |\n" +
            "0.012345678901234568 |\n" +
            "         1.0         |",
            "1      |\n" +
            "-------------\n" +
            "   -0.3     |\n" +
            "0.012345679 |\n" +
            "    1.0     |",
            "1   |\n" +
            "-------\n" +
            "-11.0 |\n" +
            " 3.0  |\n" +
            " 5.0  |",
            "1  |\n" +
            "-----\n" +
            "-11 |\n" +
            " 3  |\n" +
            " 5  |",
            "1   |\n" +
            "--------\n" +
            "-11    |\n" +
            "3      |\n" +
            "5      |",
            "1  |\n" +
            "-----\n" +
            "-11 |\n" +
            " 3  |\n" +
            " 5  |",
            "1 |\n" +
            "----\n" +
            " 0 |\n" +
            " 0 |\n" +
            " 1 |",
            "1          |\n" +
            "----------------------\n" +
            " -0.2914567944778671 |\n" +
            "0.012345051733130311 |\n" +
            " 0.7853981633974483  |",
            "1          |\n" +
            "---------------------\n" +
            "-2.0344439357957027 |\n" +
            "1.1071487177940904  |\n" +
            "1.1071487177940904  |",
            "1          |\n" +
            "----------------------\n" +
            " -0.3046926540153975 |\n" +
            "0.012345992535475034 |\n" +
            " 1.5707963267948966  |",
            "1     |\n" +
            "------------\n" +
            "0002-04-25 |\n" +
            "1989-04-25 |\n" +
            "1990-08-25 |",
            "1         |\n" +
            "--------------------\n" +
            "        0.0        |\n" +
            "1.5584503342594216 |\n" +
            "1.8754889808102941 |",
            "1         |\n" +
            "--------------------\n" +
            "0.7306094602878371 |\n" +
            "0.7308781907032909 |",
            "1         |\n" +
            "--------------------\n" +
            "0.7298032243379924 |\n" +
            "0.7298032243379924 |\n" +
            "0.7298032243379924 |",
            "1     |\n" +
            "------------\n" +
            "2017-09-29 |\n" +
            "2017-09-29 |\n" +
            "2017-09-29 |",
            "1                                               |\n" +
            "-----------------------------------------------------------------------------------------------\n" +
            "-11                           -11                           -11                           -11 |\n" +
            " 3                             3                             3                             3  |\n" +
            " 5                             5                             5                             5  |"
        };

        String query[] = {
            "select year(DATE('2001-01-01')) from t1 --splice-properties useSpark=%s\n" +
            "where c is not null and c in ('3','4','5')",
            "select extract(year from {ts'2001-01-01 12:00:00.123456'})  from t1 --splice-properties useSpark=%s\n",
            "select extract(quarter from {ts'2001-01-01 12:00:00.123456'}) from t1 --splice-properties useSpark=%s\n",
            "select extract(month from {ts'2001-01-01 12:00:00.123456'}) from t1 --splice-properties useSpark=%s\n",
            "select extract(monthname from {ts'2001-01-01 12:00:00.123456'}) from t1 --splice-properties useSpark=%s\n",
            "select extract(week from {ts'2001-01-01 12:00:00.123456'}) from t1 --splice-properties useSpark=%s\n",
            "select extract(weekday from {ts'2001-01-01 12:00:00.123456'}) from t1 --splice-properties useSpark=%s\n",
            "select extract(weekdayname from {ts'2001-01-01 12:00:00.123456'}) from t1 --splice-properties useSpark=%s\n",
            "select extract(dayofyear from {ts'2001-01-01 12:00:00.123456'}) from t1 --splice-properties useSpark=%s\n",
            "select extract(day from {ts'2001-01-01 12:00:00.123456'}) from t1 --splice-properties useSpark=%s\n",
            "select extract(hour from {ts'2001-01-01 12:00:00.123456'}) from t1 --splice-properties useSpark=%s\n",
            "select extract(minute from {ts'2001-01-01 12:00:00.123456'}) from t1 --splice-properties useSpark=%s\n",
            "select extract(second from {ts'2001-01-01 12:00:00.123456'}) from t1 --splice-properties useSpark=%s\n",
            "select week(b) from t1 --splice-properties useSpark=%s\n",
            "select varchar(c) from t1 --splice-properties useSpark=%s\n",
            "select user from t1 --splice-properties useSpark=%s\n",
            "select ucase('abc') from t1 --splice-properties useSpark=%s\n",
            "select upper('abc') from t1 --splice-properties useSpark=%s\n",
            "select to_date('2016-12-31', 'yyyy-MM-dd') from t1 --splice-properties useSpark=%s\n",
            "select TO_CHAR(b, 'mm/dd/yyyy') from t1 --splice-properties useSpark=%s\n",
            "select TIMESTAMPDIFF(SQL_TSI_YEAR, Date('11/22/1963'), a) from t7 --splice-properties useSpark=%s\n",
            "select  {ts'2001-01-01 12:00:00.123456'}, TIMESTAMPADD(SQL_TSI_MONTH, 2, {ts'2001-01-01 12:00:00.123456'}) from t1 --splice-properties useSpark=%s\n",
            "select tinyint(1) from t1 --splice-properties useSpark=%s\n",
            "select TIMESTAMP('2015-11-12', '19:02:43') from t1 --splice-properties useSpark=%s\n",
            "select tanh(.089) from t1 --splice-properties useSpark=%s\n",
            "select tan(.089) from t1 --splice-properties useSpark=%s\n",
            "select -sqrt(3421E+09) from t1 --splice-properties useSpark=%s\n",
            "select SUBSTR(a, 1, 4) from t5 --splice-properties useSpark=%s\n",
            "select smallint(132) from t1 --splice-properties useSpark=%s\n",
            "select sinh(a) from t1 --splice-properties useSpark=%s\n",
            "select sin(a) from t1 --splice-properties useSpark=%s\n",
            "select bigint(9000000000)+bigint(9000000000) from t1 --splice-properties useSpark=%s\n",
            "select SIGN(84.4), SIGN(-85.5), SIGN(0), SIGN(NULL) from t1 --splice-properties useSpark=%s\n",
            "select session_user from t1 --splice-properties useSpark=%s\n",
            "select RTRIM('     Space Case      ') from t1 --splice-properties useSpark=%s\n",
            "select ROUND(-23.55, 1) from t1 --splice-properties useSpark=%s\n",
            "select replace(c,'1', 'hello') from t1 --splice-properties useSpark=%s\n",
            "select repeat(c,3) from t1 --splice-properties useSpark=%s\n",
            "select REGEXP_LIKE(c, '[0-9]+[ ]*') from t1 --splice-properties useSpark=%s\n",
            "select RADIANS(90) from t1 --splice-properties useSpark=%s\n",
            "select pi() * 3 from t3 --splice-properties useSpark=%s\n",
            "select nvl(a,b,c,d,e) from t3 --splice-properties useSpark=%s\n",
            "select nullif(a,b), nullif(a,c) from t6 --splice-properties useSpark=%s\n",
            "select (NEXT_DAY(DATE({ts'2001-01-01 12:00:00.123456'}), 'monday')) from t1 --splice-properties useSpark=%s\n",
            "select MONTH_BETWEEN(DATE('2017-9-22'), DATE('2015-8-15')) from t1 --splice-properties useSpark=%s\n",
            "select (DATE('2017-9-22')-7) from t1 --splice-properties useSpark=%s\n",
            "select TRIM(Both '_' from '__Space Case__') from t5 --splice-properties useSpark=%s\n",
            "select TRIM(Trailing '_' from 'Space Case__') from t5 --splice-properties useSpark=%s\n",
            "select TRIM(Leading 'S' from 'Space Case   ') from t5 --splice-properties useSpark=%s\n",
            "select length(TRIM('      Space Case   ')) from t5 --splice-properties useSpark=%s\n",
            "select LTRIM('      Space Case   ') from t5 --splice-properties useSpark=%s\n",
            "select LOG10(84.4) from t5 --splice-properties useSpark=%s\n",
            "select locate('bar', 'foobarbar') from t5 --splice-properties useSpark=%s\n",
            "select locate('bar', 'foobarbar', 5) from t5 --splice-properties useSpark=%s\n",
            "select LOG(84.4), LN(84.4) from t5 --splice-properties useSpark=%s\n",
            "select length(a) from t5 --splice-properties useSpark=%s\n",
            "select length(c) from t1 --splice-properties useSpark=%s\n",
            "select length(c) from t1 --splice-properties useSpark=%s\n",
            "select LENGTH(X'1aFFFFFFFFFFFFFF') from t1 --splice-properties useSpark=%s\n",
            "select lower(a), lower('A') from t4 --splice-properties useSpark=%s\n",
            "select LCASE(a), LCASE('A') from t4 --splice-properties useSpark=%s\n",
            "select (LAST_DAY(DATE({ts'2001-01-01 12:00:00.123456'}))) from t4 --splice-properties useSpark=%s\n",
            "select integer('1'), integer(1) from t4 --splice-properties useSpark=%s\n",
            "select instr('SparkSQL', 'SQL') from t4 --splice-properties useSpark=%s\n",
            "select INITCAP(a) from t4 --splice-properties useSpark=%s\n",
            "select INITCAP('tHIS iS a test') from t1 --splice-properties useSpark=%s\n",
            "select extract (SECOND from {ts'2001-01-01 12:00:00.123456'}) from t1 --splice-properties useSpark=%s\n",
            "select ceil(1.234) from t1 --splice-properties useSpark=%s\n",
            "select floor(1.234) from t1 --splice-properties useSpark=%s\n",
            "select EXP(1.234) from t1 --splice-properties useSpark=%s\n",
            "select DOUBLE(5) from t1 --splice-properties useSpark=%s\n",
            "select DEGREES(ACOS(0.5)) from t1 --splice-properties useSpark=%s\n",
            "select cot(a) from t1 --splice-properties useSpark=%s\n",
            "select cast('TrUe' as boolean) from t1 --splice-properties useSpark=%s\n",
            "select cast('2001-01-01 12:00:00' as long varchar) from t1 --splice-properties useSpark=%s\n",
            "select cast('2001-01-01 12:00:00' as timestamp) from t1 --splice-properties useSpark=%s\n",
            "select date(789) from t1 --splice-properties useSpark=%s\n",
            "select cosh(a) from t1 --splice-properties useSpark=%s\n",
            "select coalesce(a,b,c,e,d) from t3 --splice-properties useSpark=%s\n",
            "select c from t2 --splice-properties useSpark=%s\n" +
            "where a = (select a from t2)",
            "select char(a) from t2 --splice-properties useSpark=%s\n",
            "select cast(c as timestamp) from t2 --splice-properties useSpark=%s\n",
            "select cast(a as float(29)) from t1 --splice-properties useSpark=%s\n",
            "select cast(a as real) from t1 --splice-properties useSpark=%s\n",
            "select cast(c as double) from t1 --splice-properties useSpark=%s\n",
            "select cast(c as bigint) from t1 --splice-properties useSpark=%s\n",
            "select cast(c as text(6)) from t1 --splice-properties useSpark=%s\n",
            "select bigint(c) from t1 --splice-properties useSpark=%s\n",
            "select bigint(a) from t1 --splice-properties useSpark=%s\n",
            "select atan(a) from t1 --splice-properties useSpark=%s\n",
            "select atan2(a,a/2) from t1 --splice-properties useSpark=%s\n",
            "select asin(a) from t1 --splice-properties useSpark=%s\n",
            "select add_months(b, 4) from t1 --splice-properties useSpark=%s\n",
            "select acos(a) from t1 --splice-properties useSpark=%s",
            "select rand(e) from t3 --splice-properties useSpark=%s\n",
            "select rand(13.3) from t1 --splice-properties useSpark=%s\n",
            "select (7+DATE('2017-9-22')) from t1 --splice-properties useSpark=%s\n",
            "select c || c || c || c from t1 --splice-properties useSpark=%s\n",
        };

        for (int i = 0; i < query.length; i++) {
            // Ignore the result for the tanh, sinh, cosh and log10 functions.  There is a slight
            // difference in the result between SparkSQL and splice
            // (maybe due to precision or rounding differences?).
            // The rand function also returns a different starting value,
            // given a fixed input seed.
            if (!useSpark || (i != 24 && i != 29 && i != 51 && i != 77 && i != 95)) {

                // DB-8206: Don't execute test 21 for now.  
                if (i == 21) continue;
                testQuery(format(query[i], useSpark), expected[i], methodWatcher);
            }
        }
    }

    @Test
    public void testExpectedFailures() throws Exception {
        String expected[] = {
            "The '-' operator with a left operand type of 'INTEGER' and a right operand type of 'DATE' is not supported.",
            "DATEs cannot be added. The operation is undefined.",
            "The 'EXTRACT DAY' function is not allowed on the 'DECIMAL' type.",
            "The resulting value is outside the range for the data type DECIMAL/NUMERIC(31,30).",
            "Columns of type 'DATE' cannot hold values of type 'DECIMAL'. ",
            "Columns of type 'DOUBLE' cannot hold values of type 'DATE'. ",
            "The resulting value is outside the range for the data type INTEGER."
        };

        String query[] = {
            "select (7-DATE('2017-9-22')) from t1 --splice-properties useSpark=%s\n",
            "select (DATE('2017-9-22')+DATE('2017-9-22')) from t1 --splice-properties useSpark=%s\n",
            "select day(a) from t1 --splice-properties useSpark=%s\n",
            "select cast(c as decimal(31,30)) from t1 --splice-properties useSpark=%s\n",
            "select add_months(a, 4) from t1 --splice-properties useSpark=%s\n",
            "select acos(b) from t1 --splice-properties useSpark=%s\n",
            "select cast(a as integer) from t8 --splice-properties useSpark=%s\n"
        };

        for (int i = 0; i < query.length; i++)
            testFail(format(query[i], useSpark), Arrays.asList(expected[i]), methodWatcher);
    }

    @Test
    public void testCaseStmt() throws Exception {
        String expected[] = {
            "1  |\n" +
            "------\n" +
            "  1  |\n" +
            "  2  |\n" +
            "NULL |\n" +
            "NULL |",
            "1  |\n" +
            "------\n" +
            "  1  |\n" +
            "  2  |\n" +
            "  3  |\n" +
            "NULL |",
            "1  |     2     |\n" +
            "------------------\n" +
            "NULL |2001-01-01 |"
        };

        String query[] = {
            "select case when a=b then 1 else (case when a=c then 2 else null end) end from t3_case --splice-properties useSpark=%s\n",
            "select case when a=b then 1 when a=c then 2 when a=d then 3 else null end from t3_case --splice-properties useSpark=%s\n",
            "select nullif(a,b), nullif(a,c) from t6 --splice-properties useSpark=%s\n"
        };

        for (int i = 0; i < query.length; i++)
            testQuery(format(query[i], useSpark), expected[i], methodWatcher);
    }

    @Test
    public void testBugs() throws Exception {
        String expected[] = {
            "1          |\n" +
            "----------------------\n" +
            "140733193551867.0000 |\n" +
            "140733193551867.0000 |\n" +
            "140733193551867.0000 |\n" +
            "140733193551867.0000 |\n" +
            "        NULL         |\n" +
            "        NULL         |\n" +
            "        NULL         |",
            "1       |\n" +
            "---------------\n" +
            "10000000000.7 |\n" +
            "10000000000.9 |",
            "The resulting value is outside the range for the data type DECIMAL/NUMERIC(38,28).",
            "The resulting value is outside the range for the data type DOUBLE.",
            "1      |\n" +
            "-------------\n" +
            "1.79769E308 |",
            "The resulting value is outside the range for the data type DOUBLE.",
            "The resulting value is outside the range for the data type DOUBLE.",
            "The resulting value is outside the range for the data type DOUBLE.",
            "The resulting value is outside the range for the data type DOUBLE.",
            "The resulting value is outside the range for the data type DOUBLE.",
            "The 'timestamp' function is not allowed on the 'DOUBLE' type.",
            "The resulting value is outside the range for the data type DOUBLE.",
            "The resulting value is outside the range for the data type DOUBLE.",
            "1 |\n" +
            "----\n" +
            " 0 |",
            "1      |\n" +
            "--------------\n" +
            "-1.79769E308 |",
            "Attempt to divide by zero.",
            "The resulting value is outside the range for the data type DOUBLE."
        };

        boolean Fail[] = {
            false, false, true, true, false, true, true, true, true, true, true, true, true, false, false, true, true
        };

        String query[] = {
            "select s+s+s+s*l/i from ts_int --splice-properties index=null, useSpark=%s\n",
            "select top 2 a+1 from ts_decimal --splice-properties useSpark=%s\n",
            "select a+a+a+a+a+a+a+a+a+a+a+a+a+a+a+a/3 from ts_decimal --splice-properties useSpark=%s\n",
            "select nullif(a+a, b+b) from ts_double --splice-properties useSpark=%s\n",
            "select case when a is not null and a > 1 then a else a+a end from ts_double --splice-properties useSpark=%s\n",
            "select exp(a+a) from ts_double --splice-properties useSpark=%s\n",
            "select nvl(1,a+a) from ts_double --splice-properties useSpark=%s\n",
            "select degrees(a+a) from ts_double --splice-properties useSpark=%s\n",
            "select ln(a+a) from ts_double --splice-properties useSpark=%s\n",
            "select ceil(a+a) from ts_double --splice-properties useSpark=%s\n",
            "select timestamp(a+a) from ts_double --splice-properties useSpark=%s\n",
            "select abs(a+a) from ts_double --splice-properties useSpark=%s\n",
            "select case when a+a is null and a > 1 then 1 else a end from ts_double --splice-properties useSpark=%s\n",
            "select mod(c, 1) from ts_double --splice-properties useSpark=%s\n",
            "select -a - (mod(c, 1)) from ts_double --splice-properties useSpark=%s\n" +
            "order by -a - (mod(c, 1))",
            "select -a-a/0 from ts_double --splice-properties useSpark=%s\n" +
            "order by -a-a/0",
            "select * from ts_double --splice-properties useSpark=%s\n" +
            "where a+a = b+b"
        };

        for (int i = 0; i < query.length; i++) {
            if (Fail[i])
                testFail(format(query[i], useSpark), Arrays.asList(expected[i]), methodWatcher);
            else
                testQuery(format(query[i], useSpark), expected[i], methodWatcher);
        }
    }

    @Test
    public void testTruncate() throws Exception {
        String expected[] = {
            "A           |          2           |\n" +
            "----------------------------------------------\n" +
            "2000-06-07 17:12:30.0 |2000-01-01 00:00:00.0 |",
            "A    |   2    |\n" +
            "------------------\n" +
            "23.1230 |23.1000 |\n" +
            "24.2900 |24.2000 |\n" +
            "24.5500 |24.5000 |\n" +
            "25.5500 |25.5000 |\n" +
            "26.9999 |26.9000 |",
            "A    |   2    |\n" +
            "------------------\n" +
            "23.1230 |23.1200 |\n" +
            "24.2900 |24.2900 |\n" +
            "24.5500 |24.5500 |\n" +
            "25.5500 |25.5500 |\n" +
            "26.9999 |26.9900 |"
        };

        String query[] = {
            "select a, truncate (a,'YEAR') from t1_trunc --splice-properties useSpark=%s\n",
            "select a, truncate (a,1) from t2_trunc --splice-properties useSpark=%s\n",
            "select a, truncate (a,2) from t2_trunc --splice-properties useSpark=%s\n"
        };

        for (int i = 0; i < query.length; i++)
            testQuery(format(query[i], useSpark), expected[i], methodWatcher);
    }

    @Test
    public void testFilter() throws Exception {
        String expected[] = {
            "S   |\n" +
            "-------\n" +
            "32767 |\n" +
            "32767 |\n" +
            "32767 |\n" +
            "32767 |",
            "1       |\n" +
            "---------------\n" +
            "10000000000.7 |\n" +
            "10000000000.9 |",
            "The resulting value is outside the range for the data type DECIMAL/NUMERIC(38,28).",
            "The resulting value is outside the range for the data type DOUBLE.",
            "A      |\n" +
            "-------------\n" +
            "1.79769E308 |",
            "The resulting value is outside the range for the data type DOUBLE.",
            "The resulting value is outside the range for the data type DOUBLE.",
            "The 'timestamp' function is not allowed on the 'DOUBLE' type.",
            "The 'mod' operator with a left operand type of 'DOUBLE' and a right operand type of 'INTEGER' is not supported.",
            "",
            "Attempt to divide by zero.",
            "The resulting value is outside the range for the data type DOUBLE."
        };

        boolean Fail[] = {
        false, false, true, true, false, true, true, true, true, false, true, true, true, false, false, true, true
        };

        String query[] = {
            "select s from ts_int --splice-properties index=null, useSpark=%s\n" +
            "where s+s+s+s*l/i > 0",
            "select top 2 a+1 from ts_decimal --splice-properties useSpark=%s\n" +
            "where a+1 > 0",
            "select a from ts_decimal --splice-properties useSpark=%s\n" +
            "where a+a+a+a+a+a+a+a+a+a+a+a+a+a+a+a/3 is not null",
            "select a from ts_double --splice-properties useSpark=%s\n" +
            "where nullif(a+a, b+b) is not null",
            "select a from ts_double --splice-properties useSpark=%s\n" +
            "where case when a is not null and a > 1 then a else a+a end is not null",
            "select a from ts_double --splice-properties useSpark=%s\n" +
            "where exp(a+a) is null",
            "select a from ts_double --splice-properties useSpark=%s\n" +
            "where nvl(1,a+a) is null",
            "select a from ts_double --splice-properties useSpark=%s\n" +
            "where timestamp(a+a) is null",
            "select mod(c, 1) from ts_double --splice-properties useSpark=%s\n" +
            "where mod(a*a*a/0, 1) is null",
            "select a from ts_double --splice-properties useSpark=%s\n" +
            "where -a - (mod(c, 1)) is null",
            "select a from ts_double --splice-properties useSpark=%s\n" +
            "where -a-a/0 is null",
            "select * from ts_double --splice-properties useSpark=%s\n" +
            "where (a+a = b+b) = true"
        };
        for (int i = 0; i < query.length; i++) {
            if (Fail[i])
                testFail(format(query[i], useSpark), Arrays.asList(expected[i]), methodWatcher);
            else
                testQuery(format(query[i], useSpark), expected[i], methodWatcher);
        }
    }
}
