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
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;

import java.sql.CallableStatement;
import java.sql.ResultSet;
import java.util.Arrays;
import java.util.List;

public class NaturalJoinIT extends SpliceUnitTest {
    public static final String CLASS_NAME = NaturalJoinIT.class.getSimpleName();

    protected static final SpliceWatcher spliceClassWatcher = new SpliceWatcher(CLASS_NAME);

    protected static final SpliceSchemaWatcher schemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);

    protected static final SpliceTableWatcher A_TABLE = new SpliceTableWatcher("A",schemaWatcher.schemaName,
            "(k varchar(3) not null, a1 decimal(4))");
    protected static final SpliceTableWatcher B_TABLE = new SpliceTableWatcher("B",schemaWatcher.schemaName,
            "(k varchar(3) not null, b1 decimal(14, 0))");

    private static final String A_VALS =
            "INSERT INTO A VALUES ('E2', 10)";

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(schemaWatcher)
            .around(A_TABLE)
            .around(B_TABLE)
            .around(new SpliceDataWatcher() {
                @Override
                protected void starting(Description description) {
                    try {
                        spliceClassWatcher.getStatement().executeUpdate(A_VALS);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    } finally {
                        spliceClassWatcher.closeAll();
                    }
                }
            }).around(new SpliceDataWatcher(){
                @Override
                protected void starting(Description description){
                   try(CallableStatement cs = spliceClassWatcher.prepareCall("call SYSCS_UTIL.COLLECT_SCHEMA_STATISTICS(?,false)")){
                       cs.setString(1,schemaWatcher.schemaName);
                       cs.execute();
                   }catch(Exception e){
                       throw new RuntimeException(e);
                   }
                }
            });

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(CLASS_NAME);

    @Test
    public void testBugDB9348() throws Exception {
        String query = format("select k, b1 from B natural right outer join A");
        String expected = "K | B1  |\n" +
                "----------\n" +
                "E2 |NULL |";
        testQuery(query, expected, methodWatcher);
    }
}
