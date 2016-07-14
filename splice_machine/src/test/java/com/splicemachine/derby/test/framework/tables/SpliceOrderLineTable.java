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

package com.splicemachine.derby.test.framework.tables;

import com.splicemachine.derby.test.framework.SpliceTableWatcher;

public class SpliceOrderLineTable extends SpliceTableWatcher {
	public boolean loadScratchData = false;
	public static final String TABLE_NAME = "ORDER_DETAIL";
	public static final String CREATE_STRING = "(orl_order_id VARCHAR(50), orl_amt INT, orl_item_id INT, orl_date TIMESTAMP, orl_emp_id INT, orl_promotion_id INT, orl_qty_sold INT, " + 
				"orl_unit_price FLOAT, orl_unit_cost FLOAT, orl_discount FLOAT, orl_customer_id INT)";
	public SpliceOrderLineTable(String schemaName) {
		this(TABLE_NAME,schemaName);
	}

	public SpliceOrderLineTable(String orderDetailName, String schemaName) {
		super(orderDetailName,schemaName,CREATE_STRING);
	}
	public SpliceOrderLineTable(String schemaName, boolean loadScratchData) {
		super(TABLE_NAME,schemaName,CREATE_STRING);
		this.loadScratchData = loadScratchData;
	}

}
