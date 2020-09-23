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

import com.splicemachine.derby.test.framework.*;
import com.splicemachine.test.Transactions;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;

import java.sql.Connection;
import java.sql.PreparedStatement;

/**
 * Index tests. Using more manual SQL, rather than SpliceIndexWatcher.
 */
@Category({Transactions.class})
public class WriteWriteRollbackIT extends SpliceUnitTest { 
    public static final String CLASS_NAME = WriteWriteRollbackIT.class.getSimpleName().toUpperCase();

    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher();
    public static final String TABLE_NAME_1 = "A";
    protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);

    private static String tableDef = "(col1 int, col2 int)";
    protected static SpliceTableWatcher spliceTableWatcher1 = new SpliceTableWatcher(TABLE_NAME_1,CLASS_NAME, tableDef);

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(spliceSchemaWatcher)
            .around(spliceTableWatcher1)
            .around(new SpliceDataWatcher(){
			@Override
			protected void starting(Description description) {
				try {
					PreparedStatement s = spliceClassWatcher.prepareStatement(String.format("insert into %s.%s values (?, ?)", CLASS_NAME, TABLE_NAME_1));
					s.setInt(1, 1);
					s.setInt(2, 10);
					s.execute();
					
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
				finally {
					spliceClassWatcher.closeAll();
				}
			}

		});

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher();



    @Test
    public void testUpdateWriteWriteRollbackConcurrent() throws Exception {

    	Connection c1 = methodWatcher.createConnection();
    	c1.setAutoCommit(false);
    	PreparedStatement ps1 = c1.prepareStatement(String.format("update %s.%s set col2 = ? where col1 = ?", CLASS_NAME, TABLE_NAME_1));
    	ps1.setInt(1, 100);
    	ps1.setInt(2, 1);
    	ps1.execute();


    	Connection c2 = methodWatcher.createConnection();
    	c2.setAutoCommit(false);
    	try { // catch problem with rollback
    		try { // catch write-write conflict
    			PreparedStatement ps2 = c2.prepareStatement(String.format("update %s.%s set col2 = ? where col1 = ?", CLASS_NAME, TABLE_NAME_1));
    			ps2.setInt(1, 1000);
    			ps2.setInt(2, 1);
    			ps2.execute();   				
    			Assert.fail("Didn't raise write-conflict exception");
    		} catch (Exception e) {
    			c2.rollback();
    		}
    	} catch (Exception e) {
    		Assert.fail("Unexpected exception " + e);
    	}
    }
    

}
