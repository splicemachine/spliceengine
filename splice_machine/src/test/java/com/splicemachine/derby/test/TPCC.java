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

package com.splicemachine.derby.test;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import com.splicemachine.derby.test.framework.SpliceDataWatcher;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceTableWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
public class TPCC extends SpliceUnitTest {
	protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher();
	public static final String CLASS_NAME = TPCC.class.getSimpleName().toUpperCase();
	protected static final String CUSTOMER = "CUSTOMER";
	protected static final String DISTRICT = "DISTRICT";	
	protected static final String HISTORY = "HISTORY";	
	protected static final String ITEM = "ITEM";
	protected static final String NEW_ORDER = "NEW_ORDER";
	protected static final String ORDER = "OORDER";
	protected static final String ORDER_LINE = "ORDER_LINE";
	protected static final String STOCK = "STOCK";
	protected static final String WAREHOUSE = "WAREHOUSE";
	
	protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);	
	protected static SpliceTableWatcher customerTable = new SpliceTableWatcher(CUSTOMER,CLASS_NAME,
			"(c_w_id int NOT NULL, c_d_id int NOT NULL, c_id int NOT NULL, c_discount decimal(4,4) NOT NULL,"+
		    "c_credit char(2) NOT NULL,c_last varchar(16) NOT NULL,c_first varchar(16) NOT NULL,c_credit_lim decimal(12,2) NOT NULL,"+
		    "c_balance decimal(12,2) NOT NULL,c_ytd_payment float NOT NULL,c_payment_cnt int NOT NULL,c_delivery_cnt int NOT NULL," +
		    "c_street_1 varchar(20) NOT NULL,c_street_2 varchar(20) NOT NULL,c_city varchar(20) NOT NULL,c_state char(2) NOT NULL," +
			"c_zip char(9) NOT NULL, c_phone char(16) NOT NULL,c_since timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,"+
			"c_middle char(2) NOT NULL, c_data varchar(500) NOT NULL,PRIMARY KEY (c_w_id,c_d_id,c_id))");
	protected static SpliceTableWatcher districtTable = new SpliceTableWatcher(DISTRICT,CLASS_NAME,	
			"(d_w_id int NOT NULL,d_id int NOT NULL,d_ytd decimal(12,2) NOT NULL,d_tax decimal(4,4) NOT NULL,"+
			"d_next_o_id int NOT NULL,d_name varchar(10) NOT NULL,d_street_1 varchar(20) NOT NULL,d_street_2 varchar(20) NOT NULL,"+
			"d_city varchar(20) NOT NULL,d_state char(2) NOT NULL,d_zip char(9) NOT NULL,PRIMARY KEY (d_w_id,d_id))");
	protected static SpliceTableWatcher historyTable = new SpliceTableWatcher(HISTORY,CLASS_NAME,	
			"(h_c_id int NOT NULL,h_c_d_id int NOT NULL,h_c_w_id int NOT NULL,h_d_id int NOT NULL,h_w_id int NOT NULL,"+
			"h_date timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,h_amount decimal(6,2) NOT NULL,h_data varchar(24) NOT NULL)");
	protected static SpliceTableWatcher itemTable = new SpliceTableWatcher(ITEM,CLASS_NAME,				
			"(i_id int NOT NULL,i_name varchar(24) NOT NULL,i_price decimal(5,2) NOT NULL,"+
			"i_data varchar(50) NOT NULL,i_im_id int NOT NULL,PRIMARY KEY (i_id))");
	protected static SpliceTableWatcher newOrderTable = new SpliceTableWatcher(NEW_ORDER,CLASS_NAME,				
			"(no_w_id int NOT NULL,no_d_id int NOT NULL,no_o_id int NOT NULL,PRIMARY KEY (no_w_id,no_d_id,no_o_id))");
	protected static SpliceTableWatcher orderTable = new SpliceTableWatcher(ORDER,CLASS_NAME,		
			"(o_w_id int NOT NULL,o_d_id int NOT NULL,o_id int NOT NULL,o_c_id int NOT NULL,o_carrier_id int,"+
			"o_ol_cnt decimal(2,0) NOT NULL,o_all_local decimal(1,0) NOT NULL,o_entry_d timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,"+
			"PRIMARY KEY (o_w_id,o_d_id,o_id))");
	protected static SpliceTableWatcher orderLineTable = new SpliceTableWatcher(ORDER_LINE,CLASS_NAME,			
			"(ol_w_id int NOT NULL,ol_d_id int NOT NULL,ol_o_id int NOT NULL,ol_number int NOT NULL,"+
			"ol_i_id int NOT NULL,ol_delivery_d timestamp,ol_amount decimal(6,2) NOT NULL,ol_supply_w_id int NOT NULL,"+
			"ol_quantity decimal(2,0) NOT NULL,ol_dist_info char(24) NOT NULL,PRIMARY KEY (ol_w_id,ol_d_id,ol_o_id,ol_number))");			
	protected static SpliceTableWatcher stockTable = new SpliceTableWatcher(STOCK,CLASS_NAME,	
			"(s_w_id int NOT NULL,s_i_id int NOT NULL,s_quantity decimal(4,0) NOT NULL,s_ytd decimal(8,2) NOT NULL,s_order_cnt int NOT NULL,"+
			"s_remote_cnt int NOT NULL,s_data varchar(50) NOT NULL,s_dist_01 char(24) NOT NULL,s_dist_02 char(24) NOT NULL,s_dist_03 char(24) NOT NULL,"+
			"s_dist_04 char(24) NOT NULL,s_dist_05 char(24) NOT NULL,s_dist_06 char(24) NOT NULL,s_dist_07 char(24) NOT NULL,"+
			"s_dist_08 char(24) NOT NULL,s_dist_09 char(24) NOT NULL,s_dist_10 char(24) NOT NULL,PRIMARY KEY (s_w_id,s_i_id))");
	protected static SpliceTableWatcher warehouseTable = new SpliceTableWatcher(WAREHOUSE,CLASS_NAME,				
			"(w_id int NOT NULL,w_ytd decimal(12,2) NOT NULL,w_tax decimal(4,4) NOT NULL,w_name varchar(10) NOT NULL,w_street_1 varchar(20) NOT NULL,"+
			"w_street_2 varchar(20) NOT NULL,w_city varchar(20) NOT NULL,w_state char(2) NOT NULL,w_zip char(9) NOT NULL,PRIMARY KEY (w_id))");

	
	@ClassRule 
	public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
		.around(spliceSchemaWatcher)
		.around(customerTable)
		.around(districtTable)
		.around(historyTable)
		.around(itemTable)
		.around(newOrderTable)
		.around(orderTable)
		.around(orderLineTable)
		.around(stockTable)
		.around(warehouseTable)
		.around(new SpliceDataWatcher(){
		@Override
		protected void starting(Description description) {
			try {
				PreparedStatement ps = spliceClassWatcher.prepareStatement(format("call SYSCS_UTIL.IMPORT_DATA('%s','%s',null,'%s',',',null,null,null,null,0,null,true,null)",CLASS_NAME,CUSTOMER,getResource("customer.csv.gz")));
				ps.execute();
				ps = spliceClassWatcher.prepareStatement(format("call SYSCS_UTIL.IMPORT_DATA('%s','%s',null,'%s',',',null,null,null,null,0,null,true,null)",CLASS_NAME,WAREHOUSE,getResource("warehouse.csv")));
				ps.execute();
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
			finally {
				spliceClassWatcher.closeAll();
			}
		}

	});

	@Rule public SpliceWatcher methodWatcher = new SpliceWatcher();

	@Test
	public void testCountOfCustomers() throws Exception {
			PreparedStatement s = methodWatcher.prepareStatement(String.format("SELECT count(*) FROM %s.%s",CLASS_NAME,CUSTOMER));
			ResultSet rs = s.executeQuery();
			while (rs.next()) {
				Assert.assertEquals(300000, rs.getInt(1));
			}
	}	
	
	@Test
	public void testJoinWithPrimaryKeySpecificity() throws Exception {
			PreparedStatement s = methodWatcher.prepareStatement(String.format("SELECT c_discount, c_last, c_credit, w_tax FROM %s.%s, %s.%s WHERE w_id = ? AND c_w_id = ? AND c_d_id = ? AND c_id = ?",CLASS_NAME,CUSTOMER,CLASS_NAME,WAREHOUSE));
			s.setInt(1, 1);
			s.setInt(2, 1);
			s.setInt(3, 1);
			s.setInt(4, 1);
			ResultSet rs = s.executeQuery();
			while (rs.next()) {
				
			}
	}	

	protected static String getResource(String name) {
		return getResourceDirectory()+"tpcc/data/"+name;
	}
}
