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
 */

package com.splicemachine.derby.impl.sql.execute.actions;

import com.splicemachine.access.HBaseConfigurationSource;
import com.splicemachine.access.HConfiguration;
import com.splicemachine.derby.test.framework.*;
import com.splicemachine.homeless.TestUtils;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import java.sql.Connection;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

/**
 * @author Scott Fines
 *         Date: 1/19/16
 */
public class TempTableHBaseIT{

    public static final String CLASS_NAME = TempTableHBaseIT.class.getSimpleName().toUpperCase();
    private static SpliceSchemaWatcher tableSchema = new SpliceSchemaWatcher(CLASS_NAME);

    private static final List<String> empNameVals = Arrays.asList(
            "(001,'Jeff','Cunningham')",
            "(002,'Bill','Gates')",
            "(003,'John','Jones')",
            "(004,'Warren','Buffet')",
            "(005,'Tom','Jones')");

    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher();
    private static final String SIMPLE_TEMP_TABLE = "SIMPLE_TEMP_TABLE";
    private static String simpleDef = "(id int, fname varchar(8), lname varchar(10))";
    private static String constraintTableDef = "(id int not null primary key, fname varchar(8) not null, lname varchar(10) not null)";
    private static final String EMP_PRIV_TABLE = "EMP_PRIV";
    private static String ePrivDef = "(id int not null primary key, dob varchar(10) not null, ssn varchar(12) not null)";
    private static SpliceTableWatcher empPrivTable = new SpliceTableWatcher(EMP_PRIV_TABLE,CLASS_NAME, ePrivDef);

    private static final String CONSTRAINT_TEMP_TABLE1 = "CONSTRAINT_TEMP_TABLE1";
    private static SpliceTableWatcher constraintTable1 = new SpliceTableWatcher(CONSTRAINT_TEMP_TABLE1,CLASS_NAME, constraintTableDef);
    private static String viewFormat = "(id, lname, fname, dob, ssn) as select n.id, n.lname, n.fname, p.dob, p.ssn from %s n, %s p where n.id = p.id";

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(tableSchema)
            .around(empPrivTable);
    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher();
    /**
     * Make sure the HBase table that backs a Splice temp table gets cleaned up at the end of the user session.
     * @throws Exception
     */
    @Test
    public void testTempHBaseTableGetsDropped() throws Exception {
        long start = System.currentTimeMillis();
        HBaseAdmin hBaseAdmin = new HBaseAdmin(HConfiguration.unwrapDelegate());
        String tempConglomID;
        boolean hbaseTempExists;
        final String tmpCreate = "DECLARE GLOBAL TEMPORARY TABLE %s.%s %s not logged on commit preserve rows";
        try (Connection connection = methodWatcher.createConnection()) {
            SQLClosures.execute(connection,new SQLClosures.SQLAction<Statement>(){
                @Override
                public void execute(Statement statement) throws Exception{
                    statement.execute(String.format(tmpCreate,tableSchema.schemaName,SIMPLE_TEMP_TABLE,simpleDef));
                    SpliceUnitTest.loadTable(statement,tableSchema.schemaName+"."+SIMPLE_TEMP_TABLE,empNameVals);
                }
            });
            tempConglomID = TestUtils.lookupConglomerateNumber(tableSchema.schemaName,SIMPLE_TEMP_TABLE,methodWatcher);
            connection.commit();

        }  finally {
            methodWatcher.closeAll();
        }
        hbaseTempExists = hBaseAdmin.tableExists(tempConglomID);
        if (hbaseTempExists) {
            // HACK: wait a sec, try again.  It's going away, just takes some time.
            Thread.sleep(1000);
            hbaseTempExists = hBaseAdmin.tableExists(tempConglomID);
        }
        hBaseAdmin.close();
        Assert.assertFalse("HBase temp table [" + tempConglomID + "] still exists.", hbaseTempExists);
    }
}
