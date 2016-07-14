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
