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

package com.splicemachine.test;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.derby.test.framework.TestConnection;
import com.splicemachine.homeless.TestUtils;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import splice.com.google.common.collect.Lists;

import java.sql.*;
import java.text.Collator;
import java.util.*;

@Category(value = {SerialTest.class})
@RunWith(Parameterized.class)
public class CollationIT {

    private static final String SCHEMA = CollationIT.class.getSimpleName();

    @ClassRule
    public static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(SCHEMA);

    @ClassRule
    public static SpliceWatcher classWatcher = new SpliceWatcher(SCHEMA);

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(SCHEMA);

    final static String noCollationConnection = "jdbc:splice://localhost:1527/splicedb;user=splice;password=admin";
    final static String danskCollationConnection = "jdbc:splice://localhost:1527/splicedb;user=splice;password=admin;create=true;territory=da;collation=TERRITORY_BASED\"";
    final static String andSpark = ";useSpark=true";

    final static String englishSortResult =
            "ID |  NAME    |\n" +
            "---------------\n" +
            " 8 | áccent   |\n" +
            " 7 |  Æble    |\n" +
            " 6 |  Äpfel   |\n" +
            " 0 |  Apple   |\n" +
            " 4 |  apple   |\n" +
            " 1 | Banana   |\n" +
            " 2 |  Grape   |\n" +
            " 5 | orange   |\n" +
            " 3 |Pineapple |";

    final static String danskSortResult =
            "ID |  NAME    |\n" +
            "---------------\n" +
            " 8 | áccent   |\n" +
            " 0 |  Apple   |\n" +
            " 4 |  apple   |\n" +
            " 1 | Banana   |\n" +
            " 2 |  Grape   |\n" +
            " 5 | orange   |\n" +
            " 3 |Pineapple |\n" +
            " 7 |  Æble    |\n" +
            " 6 |  Äpfel   |";

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        Collection<Object[]> params = Lists.newArrayListWithCapacity(4);
        params.add(new Object[]{noCollationConnection, englishSortResult});
        params.add(new Object[]{danskCollationConnection, danskSortResult});
//        params.add(new Object[]{noCollationConnection + andSpark, englishSortResult});
//        params.add(new Object[]{danskCollationConnection + andSpark, danskSortResult});
        return params;
    }

    private String connectionString;
    private String sortResult;

    public CollationIT(String connectionString, String sortResult) {
        this.connectionString = connectionString;
        this.sortResult = sortResult;
    }

    @Before
    public void createTables() throws Exception {
        Connection conn = new TestConnection(DriverManager.getConnection(connectionString, new Properties()));
        conn.setSchema(SCHEMA.toUpperCase());
        methodWatcher.setConnection(conn);
    }


    private static final String[] NAMES =
        {
            "orange",
            "áccent",
            "Æble",
            "Äpfel",
            "Apple",
            "apple",
            "Banana",
            "Pineapple",
            "Grape",
        };


    void sort(ArrayList<String> arr, Locale locale)
    {
        arr.sort(new Comparator<String>() {
            @Override
            public int compare(String arg1, String arg2) {
                Collator usCollator = Collator.getInstance(locale); //Your locale here
                usCollator.setStrength(Collator.PRIMARY);
                return usCollator.compare(arg1, arg2);
            }
        });
        System.out.println( Arrays.toString(new ArrayList[]{arr}) );
    }
    @Test
    public void testJavaCollation() {
        Arrays.sort( NAMES );
        Assert.assertEquals(Arrays.toString(NAMES), "[Apple, Banana, Grape, Pineapple, apple, orange, Äpfel, Æble, áccent]");

        ArrayList<String> arr = new ArrayList<String>(Arrays.asList(NAMES));

        sort(arr, Locale.US);
        Assert.assertEquals(arr.toString(), "[áccent, Æble, Äpfel, Apple, apple, Banana, Grape, orange, Pineapple]");

        sort(arr, new Locale("da", "DK"));
        Assert.assertEquals( arr.toString(), "[áccent, Apple, apple, Banana, Grape, orange, Pineapple, Æble, Äpfel]");
    }


    @Test
    public void testOrderBy() throws Exception {
        methodWatcher.executeUpdate("DROP TABLE IF EXISTS CUSTOMER");
        methodWatcher.executeUpdate("CREATE TABLE CUSTOMER(ID INT, NAME VARCHAR(40))");
        PreparedStatement ps = methodWatcher.prepareStatement("INSERT INTO CUSTOMER VALUES(?,?)");

        Arrays.sort( NAMES );
        for (int i = 0; i < NAMES.length; i++)
        {
            ps.setInt(1, i);
            ps.setString(2, NAMES[i]);
            ps.executeUpdate();
        }
        try(ResultSet rs = methodWatcher.executeQuery("SELECT ID, NAME FROM CUSTOMER ORDER BY NAME, ID")) {
            Assert.assertEquals(sortResult, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        }
    }
}
