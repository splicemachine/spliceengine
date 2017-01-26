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
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @author Scott Fines
 *         Created on: 3/8/13
 */
public class NonNullConstraintIT extends SpliceUnitTest { 
	protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher();
	protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(NonNullConstraintIT.class.getSimpleName());	
	protected static SpliceTableWatcher spliceTableWatcher = new SpliceTableWatcher("A",NonNullConstraintIT.class.getSimpleName(),"(name varchar(40) NOT NULL, val int)");
	
	@ClassRule 
	public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
		.around(spliceSchemaWatcher)
		.around(spliceTableWatcher);
	
	@Rule public SpliceWatcher methodWatcher = new SpliceWatcher();

    @Test(expected = SQLException.class)
    public void testCannotAddNullEntryToNonNullTable() throws Exception{
        methodWatcher.getStatement().execute("insert into"+this.getPaddedTableReference("A")+"(name, val) values (null,27)");
    }

    @Test
    public void testCanStillAddEntryToNonNullTable() throws Exception{
    	methodWatcher.getStatement().execute("insert into" +this.getPaddedTableReference("A")+"(name, val) values ('sfines',27)");
        ResultSet rs = methodWatcher.executeQuery("select * from"+this.getPaddedTableReference("A"));
        Assert.assertTrue("No Columns returned!",rs.next());
        String name = rs.getString(1);
        int val = rs.getInt(2);
        Assert.assertEquals("Incorrect name returned","sfines",name);
        Assert.assertEquals("Incorrect value returned",27,val);
    }
}
