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
