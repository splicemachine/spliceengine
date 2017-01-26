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

package com.splicemachine.derby.impl.sql.execute.operations.microstrategy;

import org.spark_project.guava.collect.Lists;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.derby.test.framework.tables.SpliceItemTable;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;

/**
 * @author Scott Fines
 *         Created on: 2/24/13
 */
public class MicrostrategiesItemIT extends SpliceUnitTest { 
    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher();
	public static final String CLASS_NAME = MicrostrategiesItemIT.class.getSimpleName().toUpperCase();
	public static final String TABLE_NAME = "A";
	protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);	
	protected static SpliceItemTable spliceTableWatcher = new SpliceItemTable(TABLE_NAME,CLASS_NAME); 	
	@ClassRule 
	public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
		.around(spliceSchemaWatcher)
		.around(spliceTableWatcher);
	
	@Rule public SpliceWatcher methodWatcher = new SpliceWatcher();

    /**
     * Regression test for Bug #241. Confirms that ORDER BY does not throw an exception and
     * correctly sorts data
     */
    @Test
    public void testOrderBySorts() throws Exception{
        PreparedStatement ps = methodWatcher.prepareStatement("call SYSCS_UTIL.IMPORT_DATA (?, ?, null,?,',',null,null,null,null,1,null,true,null)");
        ps.setString(1,CLASS_NAME);
        ps.setString(2,TABLE_NAME);
        ps.setString(3,getResourceDirectory()+"item.csv");
        ps.executeQuery();
        ResultSet rs = methodWatcher.executeQuery(format("select itm_subcat_id from %s order by itm_subcat_id",this.getTableReference(TABLE_NAME)));
        List<Integer> results = Lists.newArrayList();
        int count=0;
        while(rs.next()){
            if(rs.getObject(1)==null){
                Assert.assertTrue("Sort incorrect! Null entries are not in the front of the list",count==0);
            }
            results.add(rs.getInt(1));
            count++;
        }

        //check that sort order is maintained
        for(int i=0;i<results.size()-1;i++){
            Integer first = results.get(i);
            Integer second = results.get(i+1);
            Assert.assertTrue("Sort order incorrect!",first.compareTo(second)<=0);
        }
    }
}
