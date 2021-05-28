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

package com.splicemachine.derby.impl.sql.execute.operations.joins;

import com.splicemachine.derby.test.framework.*;
import com.splicemachine.homeless.TestUtils;
import com.splicemachine.test_tools.TableCreator;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.Arrays;
import java.util.Collection;

import static com.splicemachine.test_tools.Rows.row;
import static com.splicemachine.test_tools.Rows.rows;
import static org.junit.Assert.*;

@RunWith(Parameterized.class)
public class HashableJoinIT extends SpliceUnitTest {
    private final Boolean useSpark;

    @Parameterized.Parameters
    public static Collection testParams() {
        return Arrays.asList(new Object[][] {
                { true },
                { false },
        });
    }

    public HashableJoinIT(Boolean useSpark) {
        this.useSpark = useSpark;
    }

    public static final String SCHEMA_NAME = HashableJoinIT.class.getSimpleName().toUpperCase();
    protected static SpliceWatcher classWatcher = new SpliceWatcher(SCHEMA_NAME);
    protected static SpliceSchemaWatcher schemaWatcher = new SpliceSchemaWatcher(SCHEMA_NAME);

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(classWatcher)
            .around(schemaWatcher);
    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(SCHEMA_NAME);

    public static void createData(Connection conn, String schemaName) throws Exception {

        new TableCreator(conn)
                .withCreate("create table D (c1 int primary key, c2 int not null, ts timestamp not null)")
                .create();

        new TableCreator(conn)
                .withCreate("create table D_LZ (c1 int not null, lf int not null, primary key(c1, lf))")
                .create();

        new TableCreator(conn)
                .withCreate("create table G (c2 int not null primary key)")
                .create();

        new TableCreator(conn)
                .withCreate("create table L (c2 int not null, e char(1) not null, foreign key (c2) references G(c2))")
                .create();

        new TableCreator(conn)
                .withCreate("create table TA (c1 char(36))")
                .create();

        new TableCreator(conn)
                .withCreate("create table TB (c2 char(36), s char(1), z char(1), hsp char(2), f char(1), pfnr char(10))")
                .withIndex("create index XPFHSPF on TB (hsp, f, s, z, c2)")
                .create();

        conn.commit();
    }

    @BeforeClass
    public static void createDataSet() throws Exception {
        createData(classWatcher.getOrCreateConnection(), schemaWatcher.toString());
    }

    @Test
    public void testInfeasibleHashableJoin() throws Exception {
        String sqlText = String.format("select D.c1, D.c2, G.c2 from D, G --splice-properties useSpark=%b\n " +
                "where G.c2 = D.c2 and D.ts <= '2000-01-01' " +
                "  and not (exists(select * from D_LZ, L " +
                "                  where D_LZ.c1 = D.c1" +
                "                    and L.c2 = D.c2" +
                "                    and L.e = 'X'))", useSpark);

        rowContainsQuery(new int[]{4, 6}, "explain " + sqlText, methodWatcher, "Subquery", useSpark ? "BroadcastJoin" : "NestedLoopJoin");

        try (ResultSet rs = methodWatcher.executeQuery(sqlText)) {
            assertFalse(rs.next());
        }
    }

    // DB-12085
    @Test
    public void testInvalidOrListQualifierForHashableJoin() throws Exception {
        String sqlText = String.format("select * from --splice-properties joinOrder=fixed\n" +
                                               " TA, TB --splice-properties index=XPFHSPF, joinStrategy=sortmerge\n" +
                                               " where TA.c1 = TB.c2" +
                                               "   AND TB.s = '3' AND TB.z <> 'S' AND TB.hsp = 'LE'" +
                                               "   AND NOT (TB.f = 'L' AND TB.pfnr = '12345678  ')");

        // NOT (TB.f = 'L' AND TB.pfnr = '12345678  ') is transformed to
        // (TB.f <> 'L' OR TB.pfnr <> '12345678  ')
        // Before DB-12085, isQualifier() returns true for TB.pfnr <> '12345678  '. This is wrong because index
        // columns of XPFHSPF do not contain pfnr.

        rowContainsQuery(new int[]{4, 6}, "explain " + sqlText, methodWatcher,
                         new String[]{"ProjectRestrict", "preds=[((TB.F[3:5] <> L) or ((TB.PFNR[3:6] <> 12345678  ) or false))]"},
                         new String[]{"IndexScan[XPFHSPF", "keys=[(TB.HSP[3:4] = LE)],preds=[(TA.C1[5:1] = TB.C2[5:2]),(TB.S[3:2] = 3),(TB.Z[3:3] <> S)]"});

        try (ResultSet rs = methodWatcher.executeQuery(sqlText)) {
            assertFalse(rs.next());
        }
    }
}
