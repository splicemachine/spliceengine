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

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;

import org.apache.commons.dbutils.DbUtils;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;

/**
 * @author Scott Fines
 *         Created on: 3/8/13
 */
public class EmbeddedCallStatementOperationIT extends SpliceUnitTest {
	protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher();
    private static final Logger LOG = Logger.getLogger(EmbeddedCallStatementOperationIT.class);
	@ClassRule public static TestRule chain = RuleChain.outerRule(spliceClassWatcher);
	@Rule public SpliceWatcher methodWatcher = new SpliceWatcher();

    @Test
    public void testCallGetIndexInfo() throws Exception{
        DatabaseMetaData dmd = methodWatcher.getOrCreateConnection().getMetaData();
        ResultSet rs = dmd.getIndexInfo(null, "SYS", "SYSSCHEMAS", false, true);
        int count = 0;
        while(rs.next()){
        	count++;
            LOG.trace(rs.getString(1));
        }
        Assert.assertTrue(count > 0);
        DbUtils.closeQuietly(rs);
    }

}
