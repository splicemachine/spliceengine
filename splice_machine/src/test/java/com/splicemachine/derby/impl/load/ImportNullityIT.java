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

package com.splicemachine.derby.impl.load;

import com.splicemachine.derby.test.framework.*;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.File;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.junit.Assert.assertNotNull;

/**
 * @author Scott Fines
 *         Date: 9/30/16
 */
public class ImportNullityIT{
    public static final String SCHEMA_NAME=ImportNullityIT.class.getSimpleName();
    private final RuledConnection conn = new RuledConnection(SCHEMA_NAME);

    @ClassRule
    public static final SpliceSchemaWatcher schemaWatcher =
            new SpliceSchemaWatcher(SCHEMA_NAME);

    private final TableRule withDefault = new TableRule(conn,"T","(a varchar(10),b varchar(10),c varchar(20) NOT NULL default 'nullDefault')");

    @Rule public final TestRule rules =RuleChain.outerRule(conn)
            .around(withDefault);

    private static File BADDIR;
    @BeforeClass
    public static void beforeClass() throws Exception {
        BADDIR = SpliceUnitTest.createBadLogDirectory(SCHEMA_NAME);
        assertNotNull(BADDIR);
    }

    @Test
    public void importsWithDefaultValue() throws Exception{
        String file =SpliceUnitTest.getResourceDirectory()+"/import/nullity.csv";
        try(Statement s = conn.createStatement()){
            s.execute(String.format("call SYSCS_UTIL.IMPORT_DATA(" +
                            "'%s'," +  // schema name
                            "'%s'," +  // table name
                            "null," +  // insert column list
                            "'%s'," +  // file path
                            "','," +   // column delimiter
                            "null," +  // character delimiter
                            "null," +  // timestamp format
                            "null," +  // date format
                            "null," +  // time format
                            "0," +    // max bad records
                            "'%s'," +  // bad record dir
                            "'false'," +  // has one line records
                            "null)",   // char set
                    SCHEMA_NAME,withDefault, file,
                    BADDIR.getCanonicalPath()));
            int expectedRowCount= 9; //9 total rows
            int expectedNullCount = 2; //a and b column should have 2 null values
            int[] nullCounts=  new int[3];
            try(ResultSet rs = s.executeQuery("select * from "+withDefault)){
                int count = 0;
                while(rs.next()){
                    String aStr = rs.getString(1); //a value
                    if(rs.wasNull()){
                        Assert.assertNull("Did not return a null value!",aStr);
                        nullCounts[0]++;
                    }else Assert.assertNotNull("Unexpected null",aStr);

                    String bStr = rs.getString(2); //b value
                    if(rs.wasNull()){
                        Assert.assertNull("Did not return a null value!",bStr);
                        nullCounts[1]++;
                    }else Assert.assertNotNull("Unexpected null",bStr);

                    String cStr = rs.getString(3); //c value
                    //c value should never be null
                    Assert.assertFalse("C should never have a null value!",rs.wasNull());
                    Assert.assertNotNull("C should never have a null value!",cStr);

                    count++;
                }
                Assert.assertEquals("Incorrect number of rows imported!",expectedRowCount,count);
                Assert.assertEquals("Incorrect null count for column a!",expectedNullCount,nullCounts[0]);
                Assert.assertEquals("Incorrect null count for column b!",expectedNullCount,nullCounts[1]);
            }
        }
    }
}
