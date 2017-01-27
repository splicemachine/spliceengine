/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
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
import com.splicemachine.derby.test.framework.SpliceWatcher;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * Tests around creating schemas
 */
public class CreateSchemaIT {
    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher();
    private static final Logger LOG = Logger.getLogger(CreateSchemaIT.class);

    protected static SpliceSchemaWatcher sullivan1SchemaWatcher = new SpliceSchemaWatcher("SULLIVAN1");
    protected static SpliceSchemaWatcher sullivanSchemaWatcher = new SpliceSchemaWatcher("SULLIVAN");

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(sullivan1SchemaWatcher)
            .around(sullivanSchemaWatcher);

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher();

    @Test
    public void testCreateSchemasWitSimilarName() throws Exception {
        PreparedStatement ps = methodWatcher.prepareStatement("select * from sys.sysschemas where schemaName like 'SULLIVAN%'");
        ResultSet rs = ps.executeQuery();
        int count = 0;
        while (rs.next()) {
            count++;
        }
        Assert.assertEquals("Incorrect row count", 2, count);
    }

}
