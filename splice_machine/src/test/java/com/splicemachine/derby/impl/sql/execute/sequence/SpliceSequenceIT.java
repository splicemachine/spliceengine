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

package com.splicemachine.derby.impl.sql.execute.sequence;

import com.splicemachine.derby.test.framework.*;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;

import java.sql.Connection;
import java.sql.ResultSet;

import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SpliceSequenceIT {

    private static final String SCHEMA = SpliceSequenceIT.class.getSimpleName().toUpperCase();
    private static final String USER = SCHEMA+"_USER";
    private static final String PASSWORD = "password";
    private static final String SEQUENCE = SCHEMA+"."+"testseq";



    private static SpliceWatcher spliceClassWatcher=new SpliceWatcher();

    @ClassRule
    public static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(SCHEMA);

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(SCHEMA);

    private static SpliceTableWatcher spliceTableWatcher = new SpliceTableWatcher("FOO",SCHEMA,"(col1 bigint)");
    private static SpliceUserWatcher spliceUserWatcher = new SpliceUserWatcher(USER,PASSWORD);

    @ClassRule
    public static TestRule chain= RuleChain.outerRule(spliceClassWatcher)
            .around(spliceSchemaWatcher)
            .around(spliceTableWatcher)
            .around(spliceUserWatcher)
            .around(new SpliceDataWatcher() {
                @Override
                protected void starting(Description description) {
                    try {
                        spliceClassWatcher.executeUpdate(format("insert into %s.%s values (1)", SCHEMA, "FOO"));
                    } catch (Exception e) {
                        throw new RuntimeException("Failure to insert");
                    }
                }
            });

    @Test
    public void testShortSequence() throws Exception {
        methodWatcher.executeUpdate("create sequence SMALLSEQ AS smallint");

        Integer first = methodWatcher.query("values (next value for SMALLSEQ)");
        assertEquals(new Long(first + 1), methodWatcher.query(
                String.format("VALUES SYSCS_UTIL.SYSCS_PEEK_AT_SEQUENCE('%s', 'SMALLSEQ')",SCHEMA)));
        assertEquals((first + 1), (int)methodWatcher.query("values (next value for SMALLSEQ)"));
        assertEquals((first + 2), (int)methodWatcher.query("values (nextval for SMALLSEQ)"));
        assertEquals((first + 3), (int)methodWatcher.query("values (next value for SMALLSEQ)"));
        assertEquals((first + 4), (int)methodWatcher.query("values (nextval for SMALLSEQ)"));
        assertEquals((first + 5), (int)methodWatcher.query("values (next value for SMALLSEQ)"));
        assertEquals((first+ 6), (int)methodWatcher.query("values (nextval for SMALLSEQ)"));

        assertTrue(first >= Short.MIN_VALUE && first <= Short.MAX_VALUE);

        methodWatcher.executeUpdate("drop sequence SMALLSEQ restrict");
    }

    @Test
    @Ignore
    public void testIdentityValLocal() throws Exception{
        methodWatcher.executeUpdate(String.format("create table t1(c1 int generated always as identity, c2 int)"));
        methodWatcher.executeUpdate(String.format("insert into t1(c2) values (8)"));
        methodWatcher.executeUpdate(String.format("insert into t1(c2) values (IDENTITY_VAL_LOCAL())"));


    }

    @Test
    public void testGrantPrivileges() throws Exception {
        try {methodWatcher.executeUpdate("drop sequence "+SEQUENCE + " restrict");} catch (Exception e) {}
        methodWatcher.executeUpdate("create sequence "+SEQUENCE);
        methodWatcher.executeUpdate(String.format("grant usage on SEQUENCE %s to %s",SEQUENCE,USER));
        Connection connection = methodWatcher.connectionBuilder().user(USER).password(PASSWORD).build();
        ResultSet rs = connection.prepareStatement("values (next value for "+SEQUENCE+")").executeQuery();
        assertTrue(rs.next());
        assertEquals(-2147483648,rs.getInt(1));
        rs.close();
        connection.close();
    }

}
