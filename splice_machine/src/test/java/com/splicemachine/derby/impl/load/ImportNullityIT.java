/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 */

package com.splicemachine.derby.impl.load;

import com.splicemachine.derby.test.framework.RuledConnection;
import com.splicemachine.derby.test.framework.SchemaRule;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.TableRule;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
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
    private final RuledConnection conn = new RuledConnection(null);

    private final TableRule withDefault = new TableRule(conn,"T","(a varchar(10),b varchar(10),c varchar(20) NOT NULL default 'nullDefault')");

    @Rule public final TestRule rules =RuleChain.outerRule(conn)
            .around(new SchemaRule(conn,SCHEMA_NAME))
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
