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

package com.splicemachine.triggers;

import com.splicemachine.db.iapi.reference.MessageId;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.impl.sql.compile.DeleteNode;
import com.splicemachine.db.impl.sql.compile.InsertNode;
import com.splicemachine.derby.test.framework.*;
import com.splicemachine.test.SerialTest;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import splice.com.google.common.collect.Lists;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Collection;
import java.util.Properties;

/**
 * Tests creating/defining triggers.
 */
@Category(value = {SerialTest.class})
@RunWith(Parameterized.class)
public class LoadReplaceModeIT {

    private static final String SCHEMA = LoadReplaceModeIT.class.getSimpleName().toUpperCase();

    @ClassRule
    public static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(SCHEMA);

    @ClassRule
    public static SpliceWatcher classWatcher = new SpliceWatcher(SCHEMA);

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(SCHEMA);

    @Parameterized.Parameters( name = "{index}.useOLAP={0}" )
    public static Collection<Object[]> data() {
        Collection<Object[]> params = Lists.newArrayListWithCapacity(2);
        params.add(new Object[]{Boolean.FALSE});
        params.add(new Object[]{Boolean.TRUE});
        return params;
    }

    private Boolean useOLAP;

    public LoadReplaceModeIT(Boolean useOLAP) {
        this.useOLAP = useOLAP;
    }

    // 2.8 doesn't have LOAD_REPLACE_MODE but we need to test the flush problem

    // see DB-10690
    @Test
    public void testFlushProblem() throws Exception {
        methodWatcher.executeUpdate("CREATE TABLE riA (c1 INTEGER PRIMARY KEY)");
        methodWatcher.executeUpdate("CREATE TABLE riB (\n" +
                "   c1 INTEGER PRIMARY KEY,\n" +
                "   c2 INTEGER REFERENCES riA(c1)"
                + ")"
        );
        methodWatcher.executeUpdate("INSERT INTO riA VALUES 11, 12, 13");
        methodWatcher.executeUpdate("INSERT INTO riB VALUES (100,11), (200, 12), (300, 13)");
        methodWatcher.executeUpdate("DELETE FROM riB");
        methodWatcher.executeUpdate("DELETE FROM riA");

        methodWatcher.executeUpdate("DELETE FROM riB");
        methodWatcher.executeUpdate("INSERT INTO riA VALUES 11, 12, 13");
        methodWatcher.executeUpdate("INSERT INTO riB VALUES (999,11)");
        methodWatcher.executeUpdate("INSERT INTO riB VALUES (100,11), (200, 12), (300, 13) ");
        methodWatcher.executeUpdate("call SYSCS_UTIL.SYSCS_FLUSH_TABLE('" + SCHEMA + "', 'RIB')");
        methodWatcher.execute("DROP TABLE riB");
        methodWatcher.execute("DROP TABLE riA");
    }
}
