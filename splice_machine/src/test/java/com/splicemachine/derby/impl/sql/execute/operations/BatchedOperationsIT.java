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

package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.db.shared.common.reference.SQLState;
import com.splicemachine.derby.test.framework.SpliceDataWatcher;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceTableWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.derby.test.framework.TestConnection;
import com.splicemachine.homeless.TestUtils;
import com.splicemachine.test.SerialTest;
import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import splice.com.google.common.collect.Lists;
import splice.com.google.common.collect.Maps;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.math.BigDecimal;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLDataException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static com.splicemachine.derby.test.framework.SpliceUnitTest.getBaseDirectory;
import static com.splicemachine.derby.test.framework.SpliceUnitTest.getResourceDirectory;
import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


@RunWith(Parameterized.class)
@Category(SerialTest.class)
public class BatchedOperationsIT {

    private static final String SCHEMA = BatchedOperationsIT.class.getSimpleName();

    public static SpliceWatcher classWatcher = new SpliceWatcher(SCHEMA);

    @ClassRule
    public static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(SCHEMA);

    protected static SpliceTableWatcher spliceTableSource = new SpliceTableWatcher("source", SCHEMA, "(i int)");
    protected static SpliceTableWatcher spliceTablePK = new SpliceTableWatcher("tpk", SCHEMA, "(col1 int primary key, col2 int)");
    protected static SpliceTableWatcher spliceTableNPK = new SpliceTableWatcher("tnpk", SCHEMA, "(col1 int, col2 int)");

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(classWatcher)
            .around(spliceSchemaWatcher)
            .around(spliceTableSource)
            .around(spliceTablePK)
            .around(spliceTableNPK)
            .around(new SpliceDataWatcher() {
                @Override
                protected void starting(Description description) {
                    try {
                        PreparedStatement ps = classWatcher.prepareStatement("insert into source (i) values (?)");
                        for (int i = 0; i < 10; i++) {
                            ps.setInt(1, i);
                            ps.executeUpdate();
                        }
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    } finally {
                        classWatcher.closeAll();
                    }
                }

            });

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        Collection<Object[]> params = Lists.newArrayListWithCapacity(2);
        params.add(new Object[]{true});
        params.add(new Object[]{false});
        return params;
    }

    private Boolean useSpark;

    public BatchedOperationsIT(Boolean useSpark) {
        this.useSpark = useSpark;
    }

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(SCHEMA);

    @Test
    public void testBatchedInserts() throws Exception {
        try (TestConnection conn = methodWatcher.connectionBuilder().useOLAP(useSpark).build()) {
            conn.setAutoCommit(false);

            // Insert 10 rows into a PK table in a single batch
            try (PreparedStatement ps = conn.prepareStatement("insert into tpk values (?, ?)")) {
                for (int i = 0; i < 10; ++i) {
                    ps.setInt(1, i);
                    ps.setInt(2, i);
                    ps.addBatch();
                }
                int[] counts = ps.executeBatch();
                assertEquals(10, counts.length);
                for (int c : counts) {
                    assertEquals(1, c);
                }
            }

            try (PreparedStatement ps = conn.prepareStatement("select * from tpk where col1 = ?")) {
                for (int i = 0; i < 10; ++i) {
                    ps.setInt(1, i);
                    try (ResultSet rs = ps.executeQuery()) {
                        assertTrue(rs.next());
                        assertEquals(i, rs.getInt(1));
                    }
                }
            }

            // Insert variable amounts of rows per statement into a table in a single batch
            try (PreparedStatement ps = conn.prepareStatement("insert into tnpk select col1, col2 from tpk where col1 <= ?")) {
                for (int i = 0; i < 10; ++i) {
                    ps.setInt(1, i);
                    ps.addBatch();
                }
                int[] counts = ps.executeBatch();
                assertEquals(10, counts.length);
                for (int i = 0; i < 10; ++i) {
                    assertEquals(i + 1, counts[i]);
                }
            }

            try (PreparedStatement ps = conn.prepareStatement("select count(*) from tnpk where col1 = ?")) {
                for (int i = 0; i < 10; ++i) {
                    ps.setInt(1, i);
                    try (ResultSet rs = ps.executeQuery()) {
                        assertTrue(rs.next());
                        assertEquals(10 - i, rs.getInt(1));
                    }
                }
            }
        }

    }


    @Test
    public void testMultiBatchedInserts() throws Exception {
        if (useSpark) return; // execution time is too long

        try (TestConnection conn = methodWatcher.connectionBuilder().useOLAP(useSpark).build()) {
            conn.setAutoCommit(false);

            // Insert 100 rows into a PK table in 10 batches

            try (PreparedStatement ps = conn.prepareStatement("insert into tpk values (?, ?)")) {
                for (int j = 0; j < 10; ++j) {
                    for (int i = 0; i < 10; ++i) {
                        ps.setInt(1, i + j * 10);
                        ps.setInt(2, i + j * 10);
                        ps.addBatch();
                    }
                    int[] counts = ps.executeBatch();
                    assertEquals(10, counts.length);
                    for (int c : counts) {
                        assertEquals(1, c);
                    }
                }
            }

            try (PreparedStatement ps = conn.prepareStatement("select * from tpk where col1 = ?")) {
                for (int i = 0; i < 100; ++i) {
                    ps.setInt(1, i);
                    try (ResultSet rs = ps.executeQuery()) {
                        assertTrue(rs.next());
                        assertEquals(i, rs.getInt(1));
                    }
                }
            }

            // Insert variable amounts of rows per statement into a table in a multiple batches
            try (PreparedStatement ps = conn.prepareStatement("insert into tnpk select col1, col2 from tpk where col1 <= ?")) {
                for (int j = 0; j < 10; ++j) {
                    for (int i = 0; i < 10; ++i) {
                        ps.setInt(1, i + j * 10);
                        ps.addBatch();
                    }
                    int[] counts = ps.executeBatch();
                    assertEquals(10, counts.length);
                    for (int i = 0; i < 10; ++i) {
                        int c = i + 1 + j * 10;
                        assertEquals(c, counts[i]);
                    }
                }
            }

            try (PreparedStatement ps = conn.prepareStatement("select count(*) from tnpk where col1 = ?")) {
                for (int i = 0; i < 100; ++i) {
                    ps.setInt(1, i);
                    try (ResultSet rs = ps.executeQuery()) {
                        assertTrue(rs.next());
                        assertEquals(100 - i, rs.getInt(1));
                    }
                }
            }
        }

    }


    @Test
    @Ignore("DB-7961")
    public void testFailuresInBatchedInserts() throws Exception {

        try (TestConnection conn = methodWatcher.connectionBuilder().useOLAP(useSpark).build();
             Statement statement = conn.createStatement()) {
            conn.setAutoCommit(false);

            statement.executeUpdate("drop table tpk if exists");
            statement.executeUpdate("create table tpk (col1 int primary key)");

            // Insert 10 rows into a PK table in a single batch

            try (PreparedStatement ps = conn.prepareStatement("insert into tpk values ?")) {
                ps.setInt(1, 9);
                ps.execute();

                for (int i = 0; i < 10; ++i) {
                    ps.setInt(1, i);
                    ps.addBatch();
                }
                try {
                    int[] counts = ps.executeBatch();
                    assertEquals(10, counts.length);
                    for (int c : counts) {
                        assertEquals(1, c);
                    }
                } catch (Exception e) {
                    // TODO make sure the BatchUpdateException reports the right counts and exceptions
                }
            }

            try (PreparedStatement ps = conn.prepareStatement("select * from tpk where col1 = ?")) {
                for (int i = 0; i < 10; ++i) {
                    ps.setInt(1, i);
                    try (ResultSet rs = ps.executeQuery()) {
                        assertTrue(rs.next());
                        assertEquals(i, rs.getInt(1));
                    }
                }
            }
        }

    }


    @Test
    public void testBatchedUpdates() throws Exception {
        try (TestConnection conn = methodWatcher.connectionBuilder().useOLAP(true).build();
             Statement statement = conn.createStatement()) {
            conn.setAutoCommit(false);

            statement.executeUpdate("insert into tpk select i, i from source");

            // Update 10 rows on a PK table in a single batch

            try (PreparedStatement ps = conn.prepareStatement("update tpk set col2 = col2 + 1 where col1 = ?")) {
                for (int i = 0; i < 10; ++i) {
                    ps.setInt(1, i);
                    ps.addBatch();
                }
                int[] counts = ps.executeBatch();
                assertEquals(10, counts.length);
                for (int c : counts) {
                    assertEquals(1, c);
                }
            }

            try (PreparedStatement ps = conn.prepareStatement("select * from tpk where col1 = ?")) {
                for (int i = 0; i < 10; ++i) {
                    ps.setInt(1, i);
                    try (ResultSet rs = ps.executeQuery()) {
                        assertTrue(rs.next());
                        assertEquals(i, rs.getInt(1));
                        assertEquals(i + 1, rs.getInt(2));
                    }
                }
            }

            try (PreparedStatement ps = conn.prepareStatement("insert into tnpk select ?, i from source where i <= ?")) {
                for (int i = 0; i < 10; ++i) {
                    ps.setInt(1, i);
                    ps.setInt(2, i);
                    ps.execute();
                }
            }

            // Update variable amounts of rows per statement into a table in a single batch

            try (PreparedStatement ps = conn.prepareStatement("update tnpk set col2 = col2 + 1 where col1 = ?")) {
                for (int i = 0; i < 10; ++i) {
                    ps.setInt(1, i);
                    ps.addBatch();
                }
                int[] counts = ps.executeBatch();
                assertEquals(10, counts.length);
                for (int i = 0; i < 10; ++i) {
                    assertEquals(i + 1, counts[i]);
                }
            }

            try (PreparedStatement ps = conn.prepareStatement("select sum(col2) from tnpk where col1 = ?")) {
                for (int i = 0; i < 10; ++i) {
                    ps.setInt(1, i);
                    try (ResultSet rs = ps.executeQuery()) {
                        assertTrue(rs.next());
                        assertEquals((i + 1) * (i + 2) / 2, rs.getInt(1));
                    }
                }
            }
        }

    }

    @Test
    public void testBatchedDeletes() throws Exception {
        try (TestConnection conn = methodWatcher.connectionBuilder().useOLAP(useSpark).build();
             Statement statement = conn.createStatement()) {
            conn.setAutoCommit(false);

            statement.executeUpdate("insert into tpk select i, i from source");

            // Delete 9 rows on a PK table in a single batch

            try (PreparedStatement ps = conn.prepareStatement("delete from tpk where col1 = ?")) {
                for (int i = 0; i < 9; ++i) {
                    ps.setInt(1, i);
                    ps.addBatch();
                }
                int[] counts = ps.executeBatch();
                assertEquals(9, counts.length);
                for (int c : counts) {
                    assertEquals(1, c);
                }
            }

            try (PreparedStatement ps = conn.prepareStatement("select * from tpk where col1 = ?")) {
                for (int i = 0; i < 9; ++i) {
                    ps.setInt(1, i);
                    try (ResultSet rs = ps.executeQuery()) {
                        assertFalse(rs.next());
                    }
                }
                ps.setInt(1, 9);
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next());
                }
            }

            try (PreparedStatement ps = conn.prepareStatement("insert into tnpk select ?, i from source where i <= ?")) {
                for (int i = 0; i < 10; ++i) {
                    ps.setInt(1, i);
                    ps.setInt(2, i);
                    ps.execute();
                }
            }

            // Delete variable amounts of rows per statement into a table in a single batch

            try (PreparedStatement ps = conn.prepareStatement("delete from tnpk where col1 = ?")) {
                for (int i = 0; i < 9; ++i) {
                    ps.setInt(1, i);
                    ps.addBatch();
                }
                int[] counts = ps.executeBatch();
                assertEquals(9, counts.length);
                for (int i = 0; i < 9; ++i) {
                    assertEquals(i + 1, counts[i]);
                }
            }

            try (PreparedStatement ps = conn.prepareStatement("select sum(col2) from tnpk where col1 = ?")) {
                for (int i = 0; i < 9; ++i) {
                    ps.setInt(1, i);
                    try (ResultSet rs = ps.executeQuery()) {
                        assertTrue(rs.next());
                        assertEquals(0, rs.getInt(1));
                    }
                }

                ps.setInt(1, 9);
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals(45, rs.getInt(1));
                }
            }
        }
    }
}
