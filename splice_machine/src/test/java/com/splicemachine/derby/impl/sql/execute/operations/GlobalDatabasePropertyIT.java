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
package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.derby.test.framework.TestConnection;
import com.splicemachine.test.SerialTest;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.sql.ResultSet;
import java.sql.Statement;

/**
 *
 */
@Category(SerialTest.class) // maybe not run in parallel since it is changing global DB configs
public class GlobalDatabasePropertyIT extends SpliceUnitTest {
    private static final String SCHEMA = GlobalDatabasePropertyIT.class.getSimpleName().toUpperCase();
    protected static final SpliceWatcher spliceClassWatcher = new SpliceWatcher(SCHEMA);
    protected static final SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(SCHEMA);
    protected static final SpliceWatcher methodWatcher = new SpliceWatcher(SCHEMA);

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
                                            .around(spliceSchemaWatcher)
                                            .around(methodWatcher);


    public void setProperty(String property, String value) throws Exception {
        methodWatcher.execute(format("call SYSCS_UTIL.SYSCS_SET_GLOBAL_DATABASE_PROPERTY( '%s', '%s')", property, value));
    }

    @Test
    public void testSelectivityEstimationIncludingSkewed() throws Exception {
        String sql = "call syscs_util.syscs_get_global_database_property('derby.database.selectivityEstimationIncludingSkewedDefault')";
        try (TestConnection conn = methodWatcher.getOrCreateConnection()) {
            setProperty("derby.database.selectivityEstimationIncludingSkewedDefault", "true");
            try(Statement s = conn.createStatement();
                ResultSet rs = s.executeQuery(sql)) {
                while(rs.next()) {
                    String result = rs.getString("PROPERTY_VALUE");
                    Assert.assertTrue(result.compareToIgnoreCase("true") == 0);
                }
            }
        } finally {
            setProperty("derby.database.selectivityEstimationIncludingSkewedDefault", "NULL");
        }
    }
}
