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

package com.splicemachine.derby.impl.sql.execute.actions;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceTableWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.sql.SQLException;

public class LockTableConstantOperationIT extends SpliceUnitTest { 
	protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher();
	protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(LockTableConstantOperationIT.class.getSimpleName());	
	protected static SpliceTableWatcher spliceTableWatcher = new SpliceTableWatcher("A",LockTableConstantOperationIT.class.getSimpleName(),"(name varchar(40) NOT NULL, val int)");
	
	@ClassRule 
	public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
		.around(spliceSchemaWatcher)
		.around(spliceTableWatcher);
	
	@Rule public SpliceWatcher methodWatcher = new SpliceWatcher();

    @Test (expected=SQLException.class)
    public void aquireLockThrowsException() throws Exception{
    	try {
    		methodWatcher.getStatement().execute(String.format("LOCK TABLE %s IN SHARE MODE",getPaddedTableReference("A")));
    	} catch (SQLException e) {
    		if (!e.getMessage().contains("cannot be locked")) {
    			Assert.assertTrue("Incorrect Message Thrown for Locking",false);
    		}
    		throw e;
    	}
    }
	
}
