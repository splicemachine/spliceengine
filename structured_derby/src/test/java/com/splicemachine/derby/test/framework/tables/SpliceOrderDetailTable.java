package com.splicemachine.derby.test.framework.tables;

import com.splicemachine.derby.test.framework.SpliceTableWatcher;

public class SpliceOrderDetailTable extends SpliceTableWatcher {
	public boolean loadScratchData = false;
	public static final String TABLE_NAME = "ORDER_DETAIL";
	public static final String CREATE_STRING = "(order_id varchar(50), item_id INT, order_amt INT," +
            "order_date TIMESTAMP, emp_id INT, promotion_id INT, qty_sold INT," +
            "unit_price FLOAT, unit_cost FLOAT, discount FLOAT, customer_id INT)";
	public SpliceOrderDetailTable(String schemaName) {
		this(TABLE_NAME,schemaName);
	}

	public SpliceOrderDetailTable(String orderDetailName, String schemaName) {
		super(orderDetailName,schemaName,CREATE_STRING);
	}
	public SpliceOrderDetailTable(String schemaName, boolean loadScratchData) {
		super(TABLE_NAME,schemaName,CREATE_STRING);
		this.loadScratchData = loadScratchData;
	}

}
