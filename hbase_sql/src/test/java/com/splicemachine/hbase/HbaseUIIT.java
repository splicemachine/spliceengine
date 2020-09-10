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
 *
 */

package com.splicemachine.hbase;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.derby.utils.SpliceAdmin;
import com.splicemachine.si.constants.SIConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.IOException;
import java.sql.Connection;
import java.sql.Statement;
import java.util.Arrays;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class HbaseUIIT extends SpliceUnitTest{

    public static final String CLASS_NAME = HbaseUIIT.class.getSimpleName().toUpperCase();
    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher(CLASS_NAME);
    protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(spliceSchemaWatcher);



    @ClassRule
    public static SpliceWatcher methodWatcher = new SpliceWatcher(CLASS_NAME);


    @Test
    public void testTableAndIndexHaveNames() throws Exception {
        try (Connection c = methodWatcher.getOrCreateConnection()) {
            try (Statement s = c.createStatement()) {
                s.execute("create table a (i int, j int)");
                s.execute("create index ai on a (i)");

                long[] conglomerates = SpliceAdmin.getConglomNumbers(c, CLASS_NAME, "A");

                assertEquals(2, conglomerates.length);

                Arrays.sort(conglomerates);

                // Make sure display names are set for each conglomerate
                checkDisplayNames(conglomerates, "A", "AI");

                s.execute("truncate table a");

                long[] newConglomerates = SpliceAdmin.getConglomNumbers(c, CLASS_NAME, "A");

                Arrays.sort(newConglomerates);

                assertFalse(Arrays.equals(conglomerates, newConglomerates));

                // Make sure display names are set for the new conglomerates too
                checkDisplayNames(conglomerates, "A", "AI");
            }
        }
    }


    @Test
    public void testTruncateForeighnKey() throws Exception {
        try (Connection c = methodWatcher.getOrCreateConnection()) {
            try (Statement s = c.createStatement()) {
                s.execute("create table b (i int primary key, j int)");
                s.execute("create table c (i int primary key, j int references b(i))");

                long[] conglomerates = SpliceAdmin.getConglomNumbers(c, CLASS_NAME, "C");

                assertEquals(2, conglomerates.length);

                Arrays.sort(conglomerates);

                // Make sure display names are set for each conglomerate
                checkDisplayNames(conglomerates, "C", "SQL");

                s.execute("truncate table c");

                long[] newConglomerates = SpliceAdmin.getConglomNumbers(c, CLASS_NAME, "C");

                Arrays.sort(newConglomerates);

                assertFalse(Arrays.equals(conglomerates, newConglomerates));

                // Make sure display names are set for the new conglomerates too
                checkDisplayNames(conglomerates, "C", "SQL");
            }
        }
    }


    @Test
    public void testTruncateUniqueConstraint() throws Exception {
        try (Connection c = methodWatcher.getOrCreateConnection()) {
            try (Statement s = c.createStatement()) {
                s.execute("create table d (i int unique)");

                long[] conglomerates = SpliceAdmin.getConglomNumbers(c, CLASS_NAME, "D");

                assertEquals(2, conglomerates.length);

                Arrays.sort(conglomerates);

                // Make sure display names are set for each conglomerate
                checkDisplayNames(conglomerates, "D", "SQL");

                s.execute("truncate table d");

                long[] newConglomerates = SpliceAdmin.getConglomNumbers(c, CLASS_NAME, "D");

                Arrays.sort(newConglomerates);

                assertFalse(Arrays.equals(conglomerates, newConglomerates));

                // Make sure display names are set for the new conglomerates too
                checkDisplayNames(conglomerates, "D", "SQL");
            }
        }
    }

    private void checkDisplayNames(long[] conglomerates, String tableName, String indexName) throws IOException {
        try (Admin admin = ConnectionFactory.createConnection(new Configuration()).getAdmin()) {

            TableName tn = TableName.valueOf("splice:" + conglomerates[0]);
            TableDescriptor baseTable = admin.getDescriptor(tn);

            assertEquals(CLASS_NAME, baseTable.getValue(SIConstants.SCHEMA_DISPLAY_NAME_ATTR));
            assertEquals(tableName, baseTable.getValue(SIConstants.TABLE_DISPLAY_NAME_ATTR));


            tn = TableName.valueOf("splice:" + conglomerates[1]);
            TableDescriptor indexTable = admin.getDescriptor(tn);

            assertEquals(CLASS_NAME, indexTable.getValue(SIConstants.SCHEMA_DISPLAY_NAME_ATTR));
            assertEquals(tableName, indexTable.getValue(SIConstants.TABLE_DISPLAY_NAME_ATTR));
            if (indexName.equals("SQL")) {
                assertTrue(indexTable.getValue(SIConstants.INDEX_DISPLAY_NAME_ATTR).startsWith("SQL"));
            } else {
                assertEquals(indexName, indexTable.getValue(SIConstants.INDEX_DISPLAY_NAME_ATTR));
            }

        }
    }
}
