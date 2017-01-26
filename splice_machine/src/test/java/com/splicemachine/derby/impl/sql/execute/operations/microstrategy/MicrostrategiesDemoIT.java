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

import java.sql.ResultSet;
import java.sql.Statement;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.derby.test.framework.tables.SpliceCustomerTable;
import com.splicemachine.derby.test.framework.tables.SpliceOrderLineTable;

/**
 * 
 * @author jessiezhang
 * 
 * Since the demo machine has the current code, we need to ensure demo queries continuously running fine.
 */

public class MicrostrategiesDemoIT extends SpliceUnitTest { 
    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher();
	public static final String CLASS_NAME = MicrostrategiesDemoIT.class.getSimpleName().toUpperCase();
	public static final String TABLE_NAME_1 = "CUSTOMER";
	public static final String TABLE_NAME_2 = "ORDER_LINE";	
	protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);	
	protected static SpliceCustomerTable spliceTableWatcher1 = new SpliceCustomerTable(TABLE_NAME_1,CLASS_NAME) {
		@Override
		protected void starting(Description description) {
			super.starting(description);
			importData(getResourceDirectory()+"customer_iso.csv");
		}
	}; 	
	protected static SpliceOrderLineTable spliceTableWatcher2 = new SpliceOrderLineTable(TABLE_NAME_2,CLASS_NAME) {
		@Override
		protected void starting(Description description) {
			super.starting(description);
			importData(getResourceDirectory()+"order_line_small.csv");
		}		
	}; 	

	@ClassRule 
	public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
		.around(spliceSchemaWatcher)
		.around(spliceTableWatcher1)
		.around(spliceTableWatcher2);
	
	@Rule public SpliceWatcher methodWatcher = new SpliceWatcher();
	
	
	@Test
	public void testCustomerOrderJoin() throws Exception {
		int count = 0;
		ResultSet rs = methodWatcher.executeQuery(format(
            "select cst_zipcode, sum(orl_qty_sold*orl_unit_price) " +
                    "from %s \n" +
				"left outer join %s " +
                    "on orl_customer_id=cst_id group by cst_zipcode",spliceTableWatcher1,spliceTableWatcher2));
		while (rs.next()) {
			count++;
		}	
		Assert.assertEquals(217, count);
	}
	
	@Test
	public void testTransactionDemoQueries() throws Exception {
		int count = 0;
		methodWatcher.setAutoCommit(false);
        try {
            ResultSet rs = methodWatcher.executeQuery(format("select orl_order_id as ORDER_ID, orl_qty_sold as QTY, orl_unit_price as UNIT_PRICE, " +
                    "orl_customer_id as CUST_ID from %s where orl_customer_id=5520",this.getTableReference(TABLE_NAME_2)));
            while (rs.next()) {
                count++;
            }
            Assert.assertEquals(10, count);
            Statement s = methodWatcher.getStatement();
            s.execute(format("insert into %s (ORL_ORDER_ID,ORL_ITEM_ID,ORL_QTY_SOLD,ORL_UNIT_PRICE,ORL_CUSTOMER_ID) values "+
                "('99999_999_1',310,1,100,5520), ('99999_999_1',311,1,100,5520), ('99999_999_1',312,1,100,5520), ('99999_999_1',313,1,100,5520), ('99999_999_1',314,1,100,5520), ('99999_999_1',315,1,100,5520), ('99999_999_1',316,1,100,5520), ('99999_999_1',317,1,100,5520), ('99999_999_1',318,1,100,5520), ('99999_999_1',319,1,100,5520),"+
                "('99999_999_1',320,1,100,5520), ('99999_999_1',321,1,100,5520), ('99999_999_1',322,1,100,5520), ('99999_999_1',323,1,100,5520), ('99999_999_1',324,1,100,5520), ('99999_999_1',325,1,100,5520), ('99999_999_1',326,1,100,5520), ('99999_999_1',327,1,100,5520), ('99999_999_1',328,1,100,5520), ('99999_999_1',329,1,100,5520),"+
                "('99999_999_1',330,1,100,5520), ('99999_999_1',331,1,100,5520), ('99999_999_1',332,1,100,5520), ('99999_999_1',333,1,100,5520), ('99999_999_1',334,1,100,5520), ('99999_999_1',335,1,100,5520), ('99999_999_1',336,1,100,5520), ('99999_999_1',337,1,100,5520), ('99999_999_1',338,1,100,5520), ('99999_999_1',339,1,100,5520), "+
                "('99999_999_1',340,1,100,5520), ('99999_999_1',341,1,100,5520), ('99999_999_1',342,1,100,5520), ('99999_999_1',343,1,100,5520),('99999_999_1',344,1,100,5520),('99999_999_1',345,1,100,5520)",this.getTableReference(TABLE_NAME_2)));
            count = 0;
            rs = methodWatcher.executeQuery(format("select orl_order_id as ORDER_ID, orl_qty_sold as QTY, orl_unit_price as UNIT_PRICE, " +
                    "orl_customer_id as CUST_ID from %s where orl_customer_id=5520",this.getTableReference(TABLE_NAME_2)));
            while (rs.next()) {
                count++;
            }
            Assert.assertEquals(46, count);
            methodWatcher.rollback();
            count = 0;
            rs = methodWatcher.executeQuery(format("select orl_order_id as ORDER_ID, orl_qty_sold as QTY, orl_unit_price as UNIT_PRICE, " +
                    "orl_customer_id as CUST_ID from %s where orl_customer_id=5520",this.getTableReference(TABLE_NAME_2)));
            while (rs.next()) {
                count++;
            }
            Assert.assertEquals(10, count);
        } finally {
            methodWatcher.commit();
        }
    }
	
}
